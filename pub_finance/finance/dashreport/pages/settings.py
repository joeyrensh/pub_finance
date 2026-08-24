from datetime import datetime
import json
import os
import numpy as np
import pandas as pd
from dash import Dash, Input, Output, State, ctx, dcc, html
import dash_bootstrap_components as dbc
from flask import session

from finance import FINANCE_ROOT
from finance.dashreport.data_loader import ReportDataLoader
from finance.dashreport.utils import Header
from finance.utility.toolkit import ToolKit

# -------------------------- 路径与默认配置 --------------------------
JSON_FILE_PATH = os.path.join(FINANCE_ROOT, "utility", "scoring_weights.json")

# A股 (CN) 默认分组配置
DEFAULT_GROUPING_CN = {
    "grouping_mode": "manual",
    "bins": "5e9, 1e10, 5e10, 1e11, 2e11, inf",
    "n_groups": 5,
    "top_n_per_group": 100,
}

# 美股 (US) 默认分组配置
DEFAULT_GROUPING_US = {
    "grouping_mode": "manual",
    "bins": "2e9, 1e10, 5e10, 1e11, 2e11, inf",
    "n_groups": 5,
    "top_n_per_group": 100,
}

DEFAULT_CONFIG = {
    "weights": {
        "industry": 0.2,
        "pnl": 0.25,
        "stability": 0.2,
        "erp": 0.1,
        "strategy": 0.25,
    },
    "sub_weights": {
        "industry": {"arrow": 0.5, "bracket": 0.5},
        "pnl": {"daily": 0.4, "weighted_return": 0.6},
        "stability": {
            "win_rate": 0.2,
            "avg_trans": 0.2,
            "sortino": 0.2,
            "maxdd": 0.4,
        },
        "strategy": {"cnt": 0.6, "signal": 0.4},
    },
    "stock_filter": {
        "collection_days": 10,
        "capital_flow": 0.6,
        "up_days": 0.4,
        "quantile": 0.95,
    },
    "chart_display": {
        "chart_time_range": 120,
        "minichart_time_range": 60,
        "kline_limit": 10,
    },
    "grouping_settings_cn": DEFAULT_GROUPING_CN,
    "grouping_settings_us": DEFAULT_GROUPING_US,
    "grouping_settings_etf": {
        "min_threshold": 5e9,
        "n_groups": 1,
        "top_n_per_group": 50,
    },
}

# 【界面名称映射】
LABEL_MAPPING = {
    # 全局权重
    "industry": "Industry Factor",
    "pnl": "PnL Factor",
    "stability": "Stability Factor",
    "erp": "Equity Risk Premium Factor",
    "strategy": "Strategy Factor",
    # industry子项
    "arrow": "Uptrend",
    "bracket": "Industry Ranking",
    # pnl子项
    "daily": "Daily PnL",
    "weighted_return": "Weighted PnL",
    # stability子项
    "win_rate": "Win Rate",
    "avg_trans": "Trade Frequency",
    "sortino": "Sortino Ratio",
    "maxdd": "Max Drawdown",
    # strategy子项
    "cnt": "Strategy Intensity",
    "signal": "Strategy Tier",
    # 股票过滤器
    "stock_filter": "Stock Filter",
    "collection_days": "Collection Days",
    "capital_flow": "Capital Flow",
    "up_days": "Up Days",
    "quantile": "Quantile",
    # 图表显示范围
    "chart_display": "Chart Display",
    "chart_time_range": "Chart Time Range",
    "minichart_time_range": "Mini Chart Time Range",
    "kline_limit": "Max Klines",
    # 股票分组设置
    "grouping_settings": "Market & Grouping Settings",
    "market": "Target Market Config",
    "grouping_mode": "Grouping Mode",
    "bins": "Manual Bins",
    "n_groups": "Auto Groups",
    "top_n_per_group": "Top N per Group",
}


# -------------------------- 工具函数 --------------------------
def parse_bins_str(bins_str: str) -> list:
    """将 UI 传入的字符串（如 "2e9, 1e10, 5e10, 1e11, 2e11, inf"）解析为数值与 np.inf 列表"""
    if not bins_str or not isinstance(bins_str, str):
        return [2e9, 1e10, 5e10, 1e11, 2e11, np.inf]

    clean_items = [item.strip().lower() for item in bins_str.split(",") if item.strip()]
    parsed_bins = []

    for item in clean_items:
        if item in ("inf", "+inf", "np.inf"):
            parsed_bins.append(np.inf)
        else:
            try:
                parsed_bins.append(float(item))
            except ValueError:
                pass

    if not parsed_bins:
        return [2e9, 1e10, 5e10, 1e11, 2e11, np.inf]

    parsed_bins = sorted(list(set(parsed_bins)))
    if parsed_bins[0] > 0:
        parsed_bins.insert(0, 0.0)

    return parsed_bins


def init_json_file():
    utility_full_path = os.path.join(FINANCE_ROOT, "utility")
    os.makedirs(utility_full_path, exist_ok=True)
    if not os.path.exists(JSON_FILE_PATH):
        with open(JSON_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=4)


def load_config() -> dict:
    init_json_file()
    with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
        cfg = json.load(f)
        # 兼容补全 US/CN 独立分组键
        if "grouping_settings_cn" not in cfg:
            cfg["grouping_settings_cn"] = cfg.get(
                "grouping_settings", DEFAULT_GROUPING_CN
            )
        if "grouping_settings_us" not in cfg:
            cfg["grouping_settings_us"] = DEFAULT_GROUPING_US
        return cfg


def save_config(config: dict):
    with open(JSON_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def get_default_trade_date(market: str = "cn") -> str:
    """获取指定市场的默认交易日期（含兜底逻辑）"""
    try:
        data = ReportDataLoader.load(prefix=market, datasets=("overall",))
        df_overall = data.get("overall")
        if df_overall is not None and not df_overall.empty:
            end_date = df_overall.at[0, "end_date"]
            if isinstance(end_date, pd.Timestamp):
                return end_date.strftime("%Y%m%d")
            return str(end_date).replace("-", "")
    except Exception:
        pass

    try:
        if market == "us":
            return ToolKit.get_us_latest_trade_date(0)
        return ToolKit.get_cn_latest_trade_date(0)
    except Exception:
        return datetime.now().strftime("%Y%m%d")


def export_dynamic_list(market: str = None, trade_date: str = None) -> None:
    """读取指定市场 (或默认 cn, us) 的 stockdetail.csv 文件并执行股票排名导出"""
    column_map_default = {
        "symbol": "SYMBOL",
        "name": "NAME",
        "industry": "IND",
        "erp": "ERP",
        "open_date": "OPEN DATE",
        "daily_return_array": "DAILY RETURN",
        "pnl_ratio": "PNL RATIO",
        "win_rate": "WIN RATE",
        "avg_trans": "AVG TRANS",
        "sortino": "SORTINO RATIO",
        "max_dd": "MAX DD",
        "strategy_cnt": "STRATEGY CNT",
        "strategy": "STRATEGY",
    }

    markets = [market] if market else ["cn", "us"]

    for mkt in markets:
        csv_path = FINANCE_ROOT / f"data/{mkt}_stockdetail.csv"
        if not csv_path.exists():
            print(f"[{mkt.upper()}] 错误: 文件不存在 -> {csv_path}")
            continue

        pd_position_history = pd.read_csv(csv_path)
        if pd_position_history.empty:
            print(f"[{mkt.upper()}] 警告: 数据为空，跳过导出")
            continue

        mkt_date = trade_date or get_default_trade_date(mkt)

        toolkit = ToolKit("股票排名导出")
        selected_symbols, _ = toolkit.score_and_select_symbols(
            pd_position_history, column_map_default, mkt, mkt_date
        )
        toolkit.export_if_changed(selected_symbols, mkt)


# -------------------------- 静态键名定义 --------------------------
ROOT_KEYS = list(DEFAULT_CONFIG["weights"].keys())
IND_KEYS = list(DEFAULT_CONFIG["sub_weights"]["industry"].keys())
PNL_KEYS = list(DEFAULT_CONFIG["sub_weights"]["pnl"].keys())
STA_KEYS = list(DEFAULT_CONFIG["sub_weights"]["stability"].keys())
STR_KEYS = list(DEFAULT_CONFIG["sub_weights"]["strategy"].keys())
STK_KEYS = list(DEFAULT_CONFIG["stock_filter"].keys())
CHT_KEYS = list(DEFAULT_CONFIG["chart_display"].keys())


# -------------------------- 卡片构建函数 --------------------------
def build_root_card(cfg):
    row_list = []
    for k in ROOT_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text, width=2, className="mb-0 fw-normal l2_label"
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_root_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=cfg["weights"][k],
                            marks={0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"},
                            tooltip={"placement": "bottom", "always_visible": True},
                            className="mb-0 weight-slider-primary",
                            drag_value=0,
                            disabled=False,
                        ),
                        width=8,
                    ),
                ],
                className="mb-3 align-items-center",
            )
        )
    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    "Global Weights", className="fs-5 fw-bold text-dark l1_label"
                ),
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_industry_card(cfg):
    row_list = []
    for k in IND_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text, width=2, className="mb-0 fw-normal l2_label"
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_ind_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=cfg["sub_weights"]["industry"][k],
                            marks={0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"},
                            tooltip={"placement": "bottom", "always_visible": True},
                            className="mb-0 weight-slider-primary",
                            drag_value=0,
                            disabled=False,
                        ),
                        width=8,
                    ),
                ],
                className="mb-3 align-items-center",
            )
        )
    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    "Industry Sub-weights", className="fs-5 fw-bold text-dark l1_label"
                )
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_pnl_card(cfg):
    row_list = []
    for k in PNL_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text, width=2, className="mb-0 fw-normal l2_label"
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_pnl_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=cfg["sub_weights"]["pnl"][k],
                            marks={0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"},
                            tooltip={"placement": "bottom", "always_visible": True},
                            className="mb-0 weight-slider-primary",
                            drag_value=0,
                            disabled=False,
                        ),
                        width=8,
                    ),
                ],
                className="mb-3 align-items-center",
            )
        )
    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    "PnL Sub-weights", className="fs-5 fw-bold text-dark l1_label"
                )
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_stability_card(cfg):
    row_list = []
    for k in STA_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text, width=2, className="mb-0 fw-normal l2_label"
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_sta_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=cfg["sub_weights"]["stability"][k],
                            marks={0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"},
                            tooltip={"placement": "bottom", "always_visible": True},
                            className="mb-0 weight-slider-primary",
                            drag_value=0,
                            disabled=False,
                        ),
                        width=8,
                    ),
                ],
                className="mb-3 align-items-center",
            )
        )
    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    "Stability Sub-weights", className="fs-5 fw-bold text-dark l1_label"
                )
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_strategy_card(cfg):
    row_list = []
    for k in STR_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text, width=2, className="mb-0 fw-normal l2_label"
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_str_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=cfg["sub_weights"]["strategy"][k],
                            marks={0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"},
                            tooltip={"placement": "bottom", "always_visible": True},
                            className="mb-0 weight-slider-primary",
                            drag_value=0,
                            disabled=False,
                        ),
                        width=8,
                    ),
                ],
                className="mb-3 align-items-center",
            )
        )
    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    "Strategy Sub-weights", className="fs-5 fw-bold text-dark l1_label"
                )
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_stock_filter_card(cfg):
    row_list = []
    for k in STK_KEYS:
        display_text = LABEL_MAPPING.get(k, k)
        is_days = k == "collection_days"
        is_quantile = k == "quantile"

        # 动态设置上下限与步长
        min_v = 1 if is_days else (0.75 if is_quantile else 0)
        max_v = 30 if is_days else (0.99 if is_quantile else 1)
        step_v = 1 if is_days else 0.01

        # 动态设置刻度标记
        if is_days:
            marks_v = {10: "10", 20: "20", 30: "30"}
        elif is_quantile:
            marks_v = {0.75: "0.75", 0.85: "0.85", 0.95: "0.95", 0.99: "0.99"}
        else:
            marks_v = {0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"}

        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text, width=2, className="mb-0 fw-normal l2_label"
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_stk_{k}",
                            min=min_v,
                            max=max_v,
                            step=step_v,
                            value=cfg["stock_filter"].get(
                                k, 0.95 if is_quantile else 0
                            ),
                            marks=marks_v,
                            tooltip={"placement": "bottom", "always_visible": True},
                            className="mb-0 weight-slider-primary",
                            drag_value=0,
                            disabled=False,
                        ),
                        width=8,
                    ),
                ],
                className="mb-3 align-items-center",
            )
        )
    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    LABEL_MAPPING["stock_filter"],
                    className="fs-5 fw-bold text-dark l1_label",
                )
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_chart_display_card(cfg):
    row_list = []
    for k in CHT_KEYS:
        display_text = LABEL_MAPPING.get(k, k)
        if k == "kline_limit":
            min_v, max_v, step_v = 0, 20, 1
            marks_v = {10: "10", 20: "20"}
        elif k in ("chart_time_range", "minichart_time_range"):
            min_v = 10
            max_v = 120 if k == "minichart_time_range" else 200
            step_v = 5
            marks_v = (
                {60: "60", 120: "120", 200: "200"}
                if k == "chart_time_range"
                else {30: "30", 60: "60", 90: "90", 120: "120"}
            )
        else:
            min_v, max_v, step_v = 0, 100, 1
            marks_v = {}

        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text, width=2, className="mb-0 fw-normal l2_label"
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_cht_{k}",
                            min=min_v,
                            max=max_v,
                            step=step_v,
                            value=cfg.get("chart_display", {}).get(k, min_v),
                            marks=marks_v,
                            tooltip={"placement": "bottom", "always_visible": True},
                            className="mb-0 weight-slider-primary",
                            drag_value=0,
                            disabled=False,
                        ),
                        width=8,
                    ),
                ],
                className="mb-3 align-items-center",
            )
        )

    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    LABEL_MAPPING.get("chart_display", "Chart Display"),
                    className="fs-5 fw-bold text-dark l1_label",
                )
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def format_to_sci(val) -> str:
    """将数字或字符串格式化为简洁科学计数法 (如 5000000000.0 -> '5e9')"""
    if val is None or val == "":
        return ""
    try:
        f_val = float(val)
        if f_val == float("inf"):
            return "inf"
        # 格式化为科学计数法并去除多余字符
        s = f"{f_val:.2e}"  # -> '5.00e+09'
        num, exp = s.split("e")
        num_str = f"{float(num):g}"  # 去掉末尾的 .00 -> '5'
        exp_int = int(exp)  # '+09' -> 9
        return f"{num_str}e{exp_int}" if exp_int != 0 else num_str
    except (ValueError, TypeError):
        return str(val)


def build_grouping_settings_card(cfg: dict) -> dbc.Card:
    grp_cfg = cfg.get("grouping_settings_cn", DEFAULT_GROUPING_CN)
    etf_cfg = cfg.get("grouping_settings_etf", {"n_groups": 1, "top_n_per_group": 50})
    mode = grp_cfg.get("grouping_mode", "manual")

    card_body = [
        # 1. Target Market Config 行
        html.Div(
            [
                html.Span(f"{LABEL_MAPPING['market']}：", className="grp-label-fixed"),
                dbc.RadioItems(
                    id="grp_market",
                    options=[
                        {"label": "A-Share", "value": "cn"},
                        {"label": "US Market", "value": "us"},
                    ],
                    value="cn",
                    className="custom-grp-radio",
                ),
            ],
            className="grp-flex-row",
        ),
        # 2. Grouping Mode 行
        html.Div(
            [
                html.Span(
                    f"{LABEL_MAPPING['grouping_mode']}：", className="grp-label-fixed"
                ),
                dbc.RadioItems(
                    id="grp_mode",
                    options=[
                        {"label": "Custom Bins", "value": "manual"},
                        {"label": "Auto Groups", "value": "auto"},
                    ],
                    value=mode,
                    className="custom-grp-radio",
                ),
            ],
            className="grp-flex-row",
        ),
        # 3. 3个 Input 强制一行并排容器
        html.Div(
            [
                # Manual Bins
                html.Div(
                    [
                        html.Label(LABEL_MAPPING["bins"], className="mb-1 l2_label"),
                        dbc.Input(
                            id="grp_bins",
                            type="text",
                            value=str(grp_cfg.get("bins", "")),
                            placeholder="e.g. 2e9, 1e10, 5e10, inf",
                            disabled=(mode != "manual"),
                            className="custom-grp-input",
                        ),
                    ],
                    style={"flex": "2"},  # 占 50% 宽度
                ),
                # Auto Group Count
                html.Div(
                    [
                        html.Label(
                            LABEL_MAPPING["n_groups"],
                            className="mb-1 l2_label text-truncate",
                        ),
                        dbc.Input(
                            id="grp_n_groups",
                            type="number",
                            min=1,
                            max=50,
                            step=1,
                            value=grp_cfg.get("n_groups", 5),
                            disabled=(mode == "manual"),
                            className="custom-grp-input",
                        ),
                    ],
                    style={"flex": "1"},  # 占 25% 宽度
                ),
                # Top N
                html.Div(
                    [
                        html.Label(
                            LABEL_MAPPING["top_n_per_group"],
                            className="mb-1 l2_label text-truncate",
                        ),
                        dbc.Input(
                            id="grp_top_n",
                            type="number",
                            min=1,
                            max=100,
                            step=1,
                            value=grp_cfg.get("top_n_per_group", 10),
                            className="custom-grp-input",
                        ),
                    ],
                    style={"flex": "1"},  # 占 25% 宽度
                ),
            ],
            className="grp-inputs-row",
        ),
        # ETF Settings 专属配置项 (包含最小阈值与 Auto 模式参数)
        html.Div(
            [
                html.Div(
                    "ETF Grouping Config (Auto Mode Only)",
                    className="fw-bold mb-2 text-secondary fs-6",
                ),
                html.Div(
                    [
                        # Min Threshold
                        html.Div(
                            [
                                html.Label(
                                    LABEL_MAPPING.get("min_threshold", "Min Threshold"),
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="etf_min_threshold",
                                    type="text",
                                    value=format_to_sci(
                                        etf_cfg.get("min_threshold", "5e9")
                                    ),  # 强制格式化为 '5e9'
                                    placeholder="e.g. 5e9",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                        # Auto Groups
                        html.Div(
                            [
                                html.Label(
                                    LABEL_MAPPING.get("n_groups", "Auto Groups"),
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="etf_n_groups",
                                    type="number",
                                    min=1,
                                    max=50,
                                    step=1,
                                    value=etf_cfg.get("n_groups", 1),
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                        # Top N
                        html.Div(
                            [
                                html.Label(
                                    LABEL_MAPPING.get("top_n_per_group", "Top N"),
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="etf_top_n",
                                    type="number",
                                    min=1,
                                    max=200,
                                    step=1,
                                    value=etf_cfg.get("top_n_per_group", 50),
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                    ],
                    className="grp-inputs-row",
                ),
            ],
            id="etf_settings_container",
            className="border-top pt-3 mt-3",
        ),
    ]

    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    LABEL_MAPPING.get(
                        "grouping_settings", "Market & Grouping Settings"
                    ),
                    className="fs-5 fw-bold text-dark l1_label",
                )
            ),
            dbc.CardBody(card_body, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


# -------------------------- 页面布局 --------------------------
def create_layout(app: Dash):
    init_json_file()
    init_cfg = load_config()

    title_section = html.Div(
        [
            html.H6("Weight Configuration", className="subtitle padded"),
            html.P(
                "Drag and drop to adjust weights freely. Save profile with one click.",
                className="text-secondary text-center mb-5 small page-sub-desc",
            ),
        ]
    )

    card_row_1 = dbc.Row(
        [
            dbc.Col(build_root_card(init_cfg), lg=6, md=12),
            dbc.Col(build_industry_card(init_cfg), lg=6, md=12),
        ]
    )
    card_row_2 = dbc.Row(
        [
            dbc.Col(build_pnl_card(init_cfg), lg=6, md=12),
            dbc.Col(build_stability_card(init_cfg), lg=6, md=12),
        ]
    )
    card_row_3 = dbc.Row(
        [
            dbc.Col(build_strategy_card(init_cfg), lg=6, md=12),
            dbc.Col(build_stock_filter_card(init_cfg), lg=6, md=12),
        ]
    )
    card_row_4 = dbc.Row(
        [
            dbc.Col(build_chart_display_card(init_cfg), lg=6, md=12),
        ]
    )
    card_row_5 = dbc.Row(
        [
            dbc.Col(build_grouping_settings_card(init_cfg), lg=6, md=12),
        ]
    )

    button_section = html.Div(
        [
            dbc.Button(
                "Reset to Defaults",
                id="btn-reset",
                color="secondary",
                outline=True,
                className="py-2 rounded-2 weight-btn-reset flex-btn-item flex-grow-1",
            ),
            dbc.Button(
                "Save to JSON",
                id="btn-save",
                color="primary",
                className="py-2 rounded-2 weight-btn-save flex-btn-item flex-grow-1",
            ),
        ],
        className="d-flex gap-4 mt-5 mb-4 flex-row",
    )

    msg_section = html.Div(
        id="save-msg", className="text-center mb-4 fw-medium save-message-tip"
    )

    preview_section = html.Div(
        [
            html.H6("Real-time Configuration Preview", className="subtitle padded"),
            dcc.Textarea(
                id="json-preview", className="json-preview-box", readOnly=True
            ),
        ]
    )

    sub_page_content = [
        title_section,
        card_row_1,
        card_row_2,
        card_row_3,
        card_row_4,
        card_row_5,
        button_section,
        msg_section,
        preview_section,
    ]

    return html.Div(
        [
            html.Div(sub_page_content, className="sub_page"),
        ],
        className="page",
    )


# -------------------------- 回调注册 --------------------------
def register_callbacks(app: Dash):
    # 1. 扩充所有输入的组件列表 (增加了 etf 的 3 个 Input)
    all_slider_input_list = (
        [Input(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Input(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Input(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Input(f"slider_sta_{k}", "value") for k in STA_KEYS]
        + [Input(f"slider_str_{k}", "value") for k in STR_KEYS]
        + [
            Input(f"slider_stk_{k}", "value") for k in STK_KEYS
        ]  # 已包含 STK_KEYS 中的 quantile
        + [Input(f"slider_cht_{k}", "value") for k in CHT_KEYS]
        + [
            Input("grp_market", "value"),
            Input("grp_mode", "value"),
            Input("grp_bins", "value"),
            Input("grp_n_groups", "value"),
            Input("grp_top_n", "value"),
            # 【新增】ETF 实时监听输入
            Input("etf_min_threshold", "value"),
            Input("etf_n_groups", "value"),
            Input("etf_top_n", "value"),
        ]
    )

    # 2. 扩充重置按钮的输出列表 (增加了 etf 的 3 个 Output)
    reset_outputs = (
        [Output(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Output(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Output(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Output(f"slider_sta_{k}", "value") for k in STA_KEYS]
        + [Output(f"slider_str_{k}", "value") for k in STR_KEYS]
        + [Output(f"slider_stk_{k}", "value") for k in STK_KEYS]
        + [Output(f"slider_cht_{k}", "value") for k in CHT_KEYS]
        + [
            Output("grp_market", "value"),
            Output("grp_mode", "value"),
            Output("grp_bins", "value"),
            Output("grp_n_groups", "value"),
            Output("grp_top_n", "value"),
            # 【新增】ETF 重置输出
            Output("etf_min_threshold", "value"),
            Output("etf_n_groups", "value"),
            Output("etf_top_n", "value"),
        ]
    )

    # 手动/自动模式切换禁用逻辑
    @app.callback(
        [Output("grp_bins", "disabled"), Output("grp_n_groups", "disabled")],
        Input("grp_mode", "value"),
    )
    def toggle_grouping_mode(mode):
        if mode == "manual":
            return False, True
        return True, False

    # 切换市场单选框时，自动填充该市场的专属参数
    @app.callback(
        [
            Output("grp_mode", "value", allow_duplicate=True),
            Output("grp_bins", "value", allow_duplicate=True),
            Output("grp_n_groups", "value", allow_duplicate=True),
            Output("grp_top_n", "value", allow_duplicate=True),
        ],
        Input("grp_market", "value"),
        prevent_initial_call=True,
    )
    def switch_market_settings(market):
        cfg = load_config()
        grp_key = f"grouping_settings_{market}"
        def_val = DEFAULT_GROUPING_CN if market == "cn" else DEFAULT_GROUPING_US
        grp_cfg = cfg.get(grp_key, def_val)

        return (
            grp_cfg.get("grouping_mode", "manual"),
            grp_cfg.get("bins", ""),
            grp_cfg.get("n_groups", 5),
            grp_cfg.get("top_n_per_group", 10),
        )

    # 控制 ETF 区域在 CN 时显示，US 时隐藏
    @app.callback(
        Output("etf_settings_container", "style"), Input("grp_market", "value")
    )
    def toggle_etf_settings(market):
        if market == "cn":
            return {"display": "block"}
        return {"display": "none"}

    # 重置按钮逻辑
    @app.callback(
        reset_outputs,
        inputs=[Input("btn-reset", "n_clicks")],
        prevent_initial_call=True,
    )
    def reset_btn(n):
        out = []
        for v in DEFAULT_CONFIG["weights"].values():
            out.append(v)
        for v in DEFAULT_CONFIG["sub_weights"]["industry"].values():
            out.append(v)
        for v in DEFAULT_CONFIG["sub_weights"]["pnl"].values():
            out.append(v)
        for v in DEFAULT_CONFIG["sub_weights"]["stability"].values():
            out.append(v)
        for v in DEFAULT_CONFIG["sub_weights"]["strategy"].values():
            out.append(v)
        for v in DEFAULT_CONFIG["stock_filter"].values():
            out.append(v)
        for v in DEFAULT_CONFIG["chart_display"].values():
            out.append(v)

        # 还原当前选中的 Market 及该 Market 的默认分组配置
        out.append("cn")
        out.append(DEFAULT_GROUPING_CN["grouping_mode"])
        out.append(DEFAULT_GROUPING_CN["bins"])
        out.append(DEFAULT_GROUPING_CN["n_groups"])
        out.append(DEFAULT_GROUPING_CN["top_n_per_group"])

        # 【新增】还原 ETF 默认配置
        def_etf = DEFAULT_CONFIG.get(
            "grouping_settings_etf",
            {"min_threshold": "5e9", "n_groups": 1, "top_n_per_group": 50},
        )
        out.append(def_etf.get("min_threshold", "5e9"))
        out.append(def_etf.get("n_groups", 1))
        out.append(def_etf.get("top_n_per_group", 50))

        return out

    # JSON 实时预览刷新逻辑
    @app.callback(Output("json-preview", "value"), all_slider_input_list)
    def refresh_json(*all_values):
        ptr = 0
        cfg = load_config()

        for k in ROOT_KEYS:
            cfg["weights"][k] = all_values[ptr]
            ptr += 1
        for k in IND_KEYS:
            cfg["sub_weights"]["industry"][k] = all_values[ptr]
            ptr += 1
        for k in PNL_KEYS:
            cfg["sub_weights"]["pnl"][k] = all_values[ptr]
            ptr += 1
        for k in STA_KEYS:
            cfg["sub_weights"]["stability"][k] = all_values[ptr]
            ptr += 1
        for k in STR_KEYS:
            cfg["sub_weights"]["strategy"][k] = all_values[ptr]
            ptr += 1
        for k in STK_KEYS:
            cfg["stock_filter"][k] = all_values[ptr]
            ptr += 1
        for k in CHT_KEYS:
            cfg["chart_display"][k] = all_values[ptr]
            ptr += 1

        selected_market = all_values[ptr]
        ptr += 1
        mode_val = all_values[ptr]
        ptr += 1
        bins_val = all_values[ptr]
        ptr += 1
        n_groups_val = all_values[ptr]
        ptr += 1
        top_n_val = all_values[ptr]
        ptr += 1

        # 1. 更新对应选定市场的独立 grouping_settings
        grp_key = f"grouping_settings_{selected_market}"
        cfg[grp_key] = {
            "grouping_mode": mode_val,
            "bins": bins_val,
            "n_groups": n_groups_val,
            "top_n_per_group": top_n_val,
        }

        # 2. 【新增】提取并更新 ETF grouping_settings
        etf_min_thresh = all_values[ptr]
        ptr += 1
        etf_n_groups = all_values[ptr]
        ptr += 1
        etf_top_n = all_values[ptr]
        ptr += 1

        cfg["grouping_settings_etf"] = {
            "min_threshold": str(etf_min_thresh),
            "n_groups": etf_n_groups,
            "top_n_per_group": etf_top_n,
        }

        return json.dumps(cfg, indent=4, ensure_ascii=False)

    # 保存配置写入文件 (保存函数不变)
    @app.callback(
        Output("save-msg", "children"),
        Input("btn-save", "n_clicks"),
        State("json-preview", "value"),
        prevent_initial_call=True,
    )
    def save_btn(n, json_str):
        role = session.get("role")
        if role != "admin":
            return html.Span(
                "⛔ Only super administrators can modify the configuration.",
                style={"color": "#dc3545"},
            )
        try:
            d = json.loads(json_str)
            save_config(d)
            export_dynamic_list()
            return html.Span(
                "✅ Configuration file saved successfully.", style={"color": "#198754"}
            )
        except Exception as e:
            return html.Span(f"❌ Save failed: {str(e)}", style={"color": "#dc3545"})
