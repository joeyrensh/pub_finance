from datetime import datetime
import json
import os
import shutil
import numpy as np
import pandas as pd
from dash import Dash, Input, Output, State, ctx, dcc, html, no_update
import dash_bootstrap_components as dbc
from flask import session

from finance import FINANCE_ROOT
from finance.dashreport.data_loader import ReportDataLoader
from finance.dashreport.utils import Header
from finance.utility.toolkit import ToolKit
from finance.dashreport.pages.cron_manager import CronManager

# -------------------------- 路径定义与工具函数 --------------------------
JSON_FILE_PATH = os.path.join(FINANCE_ROOT, "utility", "scoring_weights.json")
DEFAULT_JSON_FILE_PATH = os.path.join(
    FINANCE_ROOT, "utility", "default_scoring_weights.json"
)


def init_config_files():
    """初始化检查：确保默认配置文件与当前配置文件均存在"""
    utility_full_path = os.path.join(FINANCE_ROOT, "utility")
    os.makedirs(utility_full_path, exist_ok=True)

    # 若 scoring_weights.json 不存在，首次启动时直接拷贝 default_scoring_weights.json
    if not os.path.exists(JSON_FILE_PATH):
        if os.path.exists(DEFAULT_JSON_FILE_PATH):
            shutil.copy(DEFAULT_JSON_FILE_PATH, JSON_FILE_PATH)
        else:
            raise FileNotFoundError(f"未找到默认配置文件: {DEFAULT_JSON_FILE_PATH}")


def load_default_config() -> dict:
    """从 default_scoring_weights.json 读取默认模版配置"""
    init_config_files()
    with open(DEFAULT_JSON_FILE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def load_config() -> dict:
    """服务启动/页面加载时读取最新的 scoring_weights.json"""
    init_config_files()
    with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


def save_config(config: dict):
    """保存当前配置到 scoring_weights.json"""
    with open(JSON_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)


def save_as_default_config(config: dict):
    """保存最新配置到 default_scoring_weights.json 和 scoring_weights.json"""
    with open(DEFAULT_JSON_FILE_PATH, "w", encoding="utf-8") as f:
        json.dump(config, f, ensure_ascii=False, indent=4)
    save_config(config)


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
    "min_kline_time_range": "Min Kline Time Range",
    # 股票分组设置
    "grouping_settings": "Market & Grouping Settings",
    "market": "Target Market Config",
    "grouping_mode": "Grouping Mode",
    "bins": "Manual Bins",
    "n_groups": "Auto Groups",
    "top_n_per_group": "Top N per Group",
    # 策略与回测设置映射
    "backtest_settings": "Strategy Configuration",
    "macd": "MACD Indicators",
    "moving_average": "Price Moving Averages",
    "volume_ma": "Volume Moving Averages",
    "window_metrics": "Lookback & Performance Windows",
    "capital_flow": "Fund Net Inflow Windows",
    "ma_convergence": "MA Convergence",
    "exit_rules": "Stop Loss & Trailing Take-Profit",
    # 策略的具体细节
    "macd_fast_period": "MACD Fast Period",
    "macd_slow_period": "MACD Slow Period",
    "macd_signal_period": "MACD Signal Period",
    "ma_short_period": "MA Short Period",
    "ma_mid_period": "MA Mid Period",
    "ma_long_period": "MA Long Period",
    "annual_ma_period": "Annual MA Period",
    "vol_short_period": "Vol MA Short Period",
    "vol_mid_period": "Vol MA Mid Period",
    "vol_long_period": "Vol MA Long Period",
    "short_term_window": "Short-term Window",
    "risk_window": "Risk Window",
    "net_inflow_short_period": "Inflow Short Period",
    "net_inflow_mid_period": "Inflow Mid Period",
    "ma_convergence_window": "MA Convergence Window",
    "ma_convergence_threshold_pct": "Convergence Dist Thresh (%)",
    "ma_convergence_min_days": "Convergence Min Days",
    "min_holding_days": "Min Holding Days",
    "hard_stop_loss_pct": "Hard Stop Loss (%)",
    "trailing_tp_tier_1_threshold": "Tier 1 TP Profit (%)",
    "trailing_tp_tier_1_drawback": "Tier 1 TP Drawback (%)",
    "trailing_tp_tier_2_threshold": "Tier 2 TP Profit (%)",
    "trailing_tp_tier_2_drawback": "Tier 2 TP Drawback (%)",
    "trailing_tp_tier_3_threshold": "Tier 3 TP Profit (%)",
    "trailing_tp_tier_3_drawback": "Tier 3 TP Drawback (%)",
    # 调度配置
    "schedule_settings": "Schedule Configuration",
    "strategy_schedule": "Strategy Schedule",
    "proxy_schedule": "Proxy Schedule",
    "cn_stock_cron": "A-Share Cron",
    "us_stock_cron": "US Market Cron",
    "cn_proxy_cron": "CN Proxy Cron",
    "oversea_proxy_cron": "Oversea Proxy Cron",
}


def parse_bins_str(bins_str: str) -> list:
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


def get_default_trade_date(market: str = "cn") -> str:
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
_init_cfg_for_keys = load_config()

ROOT_KEYS = list(_init_cfg_for_keys.get("weights", {}).keys())
IND_KEYS = list(_init_cfg_for_keys.get("sub_weights", {}).get("industry", {}).keys())
PNL_KEYS = list(_init_cfg_for_keys.get("sub_weights", {}).get("pnl", {}).keys())
STA_KEYS = list(_init_cfg_for_keys.get("sub_weights", {}).get("stability", {}).keys())
STR_KEYS = list(_init_cfg_for_keys.get("sub_weights", {}).get("strategy", {}).keys())
STK_KEYS = list(_init_cfg_for_keys.get("stock_filter", {}).keys())
CHT_KEYS = list(_init_cfg_for_keys.get("chart_display", {}).keys())

_bt_cfg_for_keys = _init_cfg_for_keys.get("backtest_settings", {})
BT_MACD_KEYS = list(_bt_cfg_for_keys.get("macd", {}).keys())
BT_MA_KEYS = list(_bt_cfg_for_keys.get("moving_average", {}).keys())
BT_VOL_KEYS = list(_bt_cfg_for_keys.get("volume_ma", {}).keys())
BT_WIN_KEYS = list(_bt_cfg_for_keys.get("window_metrics", {}).keys())
BT_FLOW_KEYS = list(_bt_cfg_for_keys.get("capital_flow", {}).keys())
BT_CONVERGENCE_KEYS = list(_bt_cfg_for_keys.get("ma_convergence", {}).keys())
BT_EXIT_KEYS = list(_bt_cfg_for_keys.get("exit_rules", {}).keys())

SCHEDULE_KEYS = list(_init_cfg_for_keys.get("schedule_settings", {}).keys())
if not SCHEDULE_KEYS:
    SCHEDULE_KEYS = [
        "cn_stock_cron",
        "us_stock_cron",
        "cn_proxy_cron",
        "oversea_proxy_cron",
    ]


# -------------------------- 卡片构建函数 --------------------------
def build_root_card(cfg):
    row_list = []
    for k in ROOT_KEYS:
        display_text = LABEL_MAPPING.get(k, k)
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
                            value=cfg.get("weights", {}).get(k, 0),
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
        display_text = LABEL_MAPPING.get(k, k)
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
                            value=cfg.get("sub_weights", {})
                            .get("industry", {})
                            .get(k, 0),
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
        display_text = LABEL_MAPPING.get(k, k)
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
                            value=cfg.get("sub_weights", {}).get("pnl", {}).get(k, 0),
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
        display_text = LABEL_MAPPING.get(k, k)
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
                            value=cfg.get("sub_weights", {})
                            .get("stability", {})
                            .get(k, 0),
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
        display_text = LABEL_MAPPING.get(k, k)
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
                            value=cfg.get("sub_weights", {})
                            .get("strategy", {})
                            .get(k, 0),
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

        min_v = 1 if is_days else (0.8 if is_quantile else 0)
        max_v = 30 if is_days else (0.99 if is_quantile else 1)
        step_v = 1 if is_days else 0.01

        if is_days:
            marks_v = {10: "10", 20: "20", 30: "30"}
        elif is_quantile:
            marks_v = {0.85: "0.85", 0.9: "0.9", 0.95: "0.95", 0.99: "0.99"}
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
                            value=cfg.get("stock_filter", {}).get(
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
                    LABEL_MAPPING.get("stock_filter", "Stock Filter"),
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
            max_v = 200 if k == "chart_time_range" else 120
            step_v = 5
            marks_v = (
                {50: "50", 100: "100", 150: "150", 200: "200"}
                if k == "chart_time_range"
                else {30: "30", 60: "60", 90: "90", 120: "120"}
            )
        elif k == "min_kline_time_range":
            min_v, max_v, step_v = 0, 400, 5
            marks_v = {100: "100", 200: "200", 300: "300", 400: "400"}
        else:
            min_v, max_v, step_v = 0, 100, 1
            marks_v = {100: "100"}

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
    if val is None or val == "":
        return ""
    try:
        f_val = float(val)
        if f_val == float("inf"):
            return "inf"
        s = f"{f_val:.2e}"
        num, exp = s.split("e")
        num_str = f"{float(num):g}"
        exp_int = int(exp)
        return f"{num_str}e{exp_int}" if exp_int != 0 else num_str
    except (ValueError, TypeError):
        return str(val)


def build_grouping_settings_card(cfg: dict) -> dbc.Card:
    grp_cfg = cfg.get("grouping_settings_cn", {})
    etf_cfg = cfg.get("grouping_settings_etf", {})
    mode = grp_cfg.get("grouping_mode", "manual")

    card_body = [
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
        html.Div(
            [
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
                    style={"flex": "2"},
                ),
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
                    style={"flex": "1"},
                ),
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
                            max=500,
                            step=1,
                            value=grp_cfg.get("top_n_per_group", 10),
                            className="custom-grp-input",
                        ),
                    ],
                    style={"flex": "1"},
                ),
            ],
            className="grp-inputs-row",
        ),
        html.Div(
            [
                html.Div(
                    "ETF Grouping Config (Auto Mode Only)",
                    className="fw-bold mb-2 text-secondary fs-6",
                ),
                html.Div(
                    [
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
                                    ),
                                    placeholder="e.g. 5e9",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
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
                                    max=500,
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


def get_slider_bounds_and_marks(k: str):
    if k in ("macd_fast_period", "macd_slow_period", "macd_signal_period"):
        return 0, 30, 1, {10: "10", 20: "20", 30: "30"}
    elif k == "ma_short_period":
        return 0, 30, 1, {10: "10", 20: "20", 30: "30"}
    elif k == "ma_mid_period":
        return 0, 90, 1, {30: "30", 60: "60", 90: "90"}
    elif k == "ma_long_period":
        return 0, 150, 1, {50: "50", 100: "100", 150: "150"}
    elif k == "annual_ma_period":
        return 200, 260, 1, {220: "220", 240: "240", 260: "260"}
    elif k == "vol_short_period":
        return 0, 20, 1, {10: "10", 20: "20"}
    elif k == "vol_mid_period":
        return 0, 40, 1, {10: "10", 20: "20", 30: "30", 40: "40"}
    elif k == "vol_long_period":
        return 0, 60, 1, {20: "20", 40: "40", 60: "60"}
    elif k == "short_term_window":
        return 0, 20, 1, {10: "10", 20: "20"}
    elif k == "risk_window":
        return 0, 60, 1, {20: "20", 40: "40", 60: "60"}
    elif k == "net_inflow_short_period":
        return 5, 20, 1, {10: "10", 15: "15", 20: "20"}
    elif k == "net_inflow_mid_period":
        return 0, 60, 1, {20: "20", 40: "40", 60: "60"}

    return 0, 100, 1, {100: "100"}


def build_strategy_sub_card(category_key: str, key_list: list, cfg: dict) -> dbc.Card:
    bt_cfg = cfg.get("backtest_settings", {}).get(category_key, {})
    card_title = LABEL_MAPPING.get(category_key, category_key)

    row_list = []
    for k in key_list:
        display_text = LABEL_MAPPING.get(k, k)
        val = bt_cfg.get(k, 0)
        min_v, max_v, step_v, marks_v = get_slider_bounds_and_marks(k)

        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text,
                        width=2,
                        className="mb-0 fw-normal l2_label align-self-center form-label text-truncate",
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_bt_{k}",
                            min=min_v,
                            max=max_v,
                            step=step_v,
                            value=val,
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
                html.Label(card_title, className="fs-5 fw-bold text-dark l1_label")
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_ma_convergence_card(cfg: dict) -> dbc.Card:
    sqz_cfg = cfg.get("backtest_settings", {}).get("ma_convergence", {})

    card_body = html.Div(
        [
            html.Div(
                [
                    html.Label(
                        LABEL_MAPPING.get(
                            "ma_convergence_window", "ma_convergence_window"
                        ),
                        className="mb-1 l2_label text-truncate",
                    ),
                    dbc.Input(
                        id="input_bt_ma_convergence_window",
                        type="number",
                        min=5,
                        max=120,
                        step=1,
                        value=sqz_cfg.get("ma_convergence_window", 20),
                        className="custom-grp-input",
                    ),
                ],
                style={"flex": "1"},
            ),
            html.Div(
                [
                    html.Label(
                        LABEL_MAPPING.get(
                            "ma_convergence_threshold_pct",
                            "ma_convergence_threshold_pct",
                        ),
                        className="mb-1 l2_label text-truncate",
                    ),
                    dbc.Input(
                        id="input_bt_ma_convergence_threshold_pct",
                        type="number",
                        min=0.0,
                        max=0.5,
                        step=0.005,
                        value=sqz_cfg.get("ma_convergence_threshold_pct", 0.02),
                        className="custom-grp-input",
                    ),
                ],
                style={"flex": "1"},
            ),
            html.Div(
                [
                    html.Label(
                        LABEL_MAPPING.get(
                            "ma_convergence_min_days", "ma_convergence_min_days"
                        ),
                        className="mb-1 l2_label text-truncate",
                    ),
                    dbc.Input(
                        id="input_bt_ma_convergence_min_days",
                        type="number",
                        min=0,
                        max=30,
                        step=1,
                        value=sqz_cfg.get("ma_convergence_min_days", 5),
                        className="custom-grp-input",
                    ),
                ],
                style={"flex": "1"},
            ),
        ],
        className="grp-inputs-row",
    )

    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    LABEL_MAPPING.get("ma_convergence", "MA Convergence"),
                    className="fs-5 fw-bold text-dark l1_label",
                )
            ),
            dbc.CardBody(card_body, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_exit_rules_card(cfg: dict) -> dbc.Card:
    exit_cfg = cfg.get("backtest_settings", {}).get("exit_rules", {})

    card_body = [
        html.Div(
            [
                html.Div(
                    [
                        html.Label(
                            LABEL_MAPPING.get("min_holding_days", "min_holding_days"),
                            className="mb-1 l2_label text-truncate",
                        ),
                        dbc.Input(
                            id="input_bt_min_holding_days",
                            type="number",
                            min=1,
                            max=120,
                            step=1,
                            value=exit_cfg.get("min_holding_days", 20),
                            className="custom-grp-input",
                        ),
                    ],
                    style={"flex": "1"},
                ),
                html.Div(
                    [
                        html.Label(
                            LABEL_MAPPING.get(
                                "hard_stop_loss_pct", "hard_stop_loss_pct"
                            ),
                            className="mb-1 l2_label text-truncate",
                        ),
                        dbc.Input(
                            id="input_bt_hard_stop_loss_pct",
                            type="number",
                            min=0.0,
                            max=0.5,
                            step=0.01,
                            value=exit_cfg.get("hard_stop_loss_pct", 0.10),
                            className="custom-grp-input",
                        ),
                    ],
                    style={"flex": "1"},
                ),
            ],
            className="grp-inputs-row mb-3",
        ),
        html.Div(
            [
                html.Div(
                    "Trailing Take-Profit Tiers (Profit Threshold / Drawback)",
                    className="fw-bold mb-2 text-secondary fs-6",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "Tier 1 Profit",
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="input_bt_trailing_tp_tier_1_threshold",
                                    type="number",
                                    value=exit_cfg.get(
                                        "trailing_tp_tier_1_threshold", 1.0
                                    ),
                                    min=0,
                                    max=5,
                                    step=0.05,
                                    placeholder="Profit",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                        html.Div(
                            [
                                html.Label(
                                    "Tier 1 Drawback",
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="input_bt_trailing_tp_tier_1_drawback",
                                    type="number",
                                    value=exit_cfg.get(
                                        "trailing_tp_tier_1_drawback", 0.2
                                    ),
                                    min=0,
                                    max=1,
                                    step=0.05,
                                    placeholder="Drawback",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                    ],
                    className="grp-inputs-row mb-3",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "Tier 2 Profit",
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="input_bt_trailing_tp_tier_2_threshold",
                                    type="number",
                                    value=exit_cfg.get(
                                        "trailing_tp_tier_2_threshold", 0.5
                                    ),
                                    min=0,
                                    max=5,
                                    step=0.05,
                                    placeholder="Profit",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                        html.Div(
                            [
                                html.Label(
                                    "Tier 2 Drawback",
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="input_bt_trailing_tp_tier_2_drawback",
                                    type="number",
                                    value=exit_cfg.get(
                                        "trailing_tp_tier_2_drawback", 0.3
                                    ),
                                    min=0,
                                    max=1,
                                    step=0.05,
                                    placeholder="Drawback",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                    ],
                    className="grp-inputs-row mb-3",
                ),
                html.Div(
                    [
                        html.Div(
                            [
                                html.Label(
                                    "Tier 3 Profit",
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="input_bt_trailing_tp_tier_3_threshold",
                                    type="number",
                                    value=exit_cfg.get(
                                        "trailing_tp_tier_3_threshold", 0.2
                                    ),
                                    min=0,
                                    max=5,
                                    step=0.05,
                                    placeholder="Profit",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                        html.Div(
                            [
                                html.Label(
                                    "Tier 3 Drawback",
                                    className="mb-1 l2_label text-truncate",
                                ),
                                dbc.Input(
                                    id="input_bt_trailing_tp_tier_3_drawback",
                                    type="number",
                                    value=exit_cfg.get(
                                        "trailing_tp_tier_3_drawback", 0.5
                                    ),
                                    min=0,
                                    max=1,
                                    step=0.05,
                                    placeholder="Drawback",
                                    className="custom-grp-input",
                                ),
                            ],
                            style={"flex": "1"},
                        ),
                    ],
                    className="grp-inputs-row mb-2",
                ),
            ]
        ),
    ]

    return dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    LABEL_MAPPING.get("exit_rules", "Stop Loss & Trailing Take-Profit"),
                    className="fs-5 fw-bold text-dark l1_label",
                )
            ),
            dbc.CardBody(card_body, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def create_schedule_card(config: dict) -> html.Div:
    sch_cfg = config.get("schedule_settings", {})

    strategy_schedules = [
        ("cn_stock_cron", sch_cfg.get("cn_stock_cron", "30 15 * * *")),
        ("us_stock_cron", sch_cfg.get("us_stock_cron", "00 07 * * *")),
    ]

    proxy_schedules = [
        ("cn_proxy_cron", sch_cfg.get("cn_proxy_cron", "30 07 * * *")),
        ("oversea_proxy_cron", sch_cfg.get("oversea_proxy_cron", "30 08 * * *")),
    ]

    def render_cron_input(key_name: str, val: str):
        return html.Div(
            [
                html.Label(
                    LABEL_MAPPING.get(key_name, key_name),
                    className="mb-1 l2_label text-truncate",
                ),
                dbc.Input(
                    id=f"input_cron_{key_name}",
                    type="text",
                    value=val,
                    placeholder="e.g. 30 15 * * *",
                    className="custom-grp-input",
                ),
            ],
            style={"flex": "1"},
        )

    strategy_card = dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    LABEL_MAPPING.get(
                        "strategy_schedule", "Strategy Execution Schedule"
                    ),
                    className="fs-5 fw-bold text-dark l1_label mb-0",
                )
            ),
            dbc.CardBody(
                html.Div(
                    [render_cron_input(k, v) for k, v in strategy_schedules],
                    className="grp-inputs-row",
                ),
                className="py-3 card-body",
            ),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )

    proxy_card = dbc.Card(
        [
            dbc.CardHeader(
                html.Label(
                    LABEL_MAPPING.get("proxy_schedule", "Proxy Execution Schedule"),
                    className="fs-5 fw-bold text-dark l1_label mb-0",
                )
            ),
            dbc.CardBody(
                html.Div(
                    [render_cron_input(k, v) for k, v in proxy_schedules],
                    className="grp-inputs-row",
                ),
                className="py-3 card-body",
            ),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )

    return html.Div([strategy_card, proxy_card])


# -------------------------- 页面布局 --------------------------
def create_layout(app: Dash):
    init_config_files()
    init_cfg = (
        load_config()
    )  # 逻辑1：服务重新启动/加载页面时，加载 scoring_weights.json

    weight_title_section = html.Div(
        [
            html.H6("WEIGHT CONFIGURATION", className="subtitle padded"),
            html.P(
                "Drag and drop to adjust scoring weights freely. Click [Save to JSON] or [Save as Default].",
                className="text-secondary text-center mb-4 small page-sub-desc",
            ),
        ]
    )

    weight_rows = html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(build_root_card(init_cfg), lg=6, md=12),
                    dbc.Col(build_industry_card(init_cfg), lg=6, md=12),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(build_pnl_card(init_cfg), lg=6, md=12),
                    dbc.Col(build_stability_card(init_cfg), lg=6, md=12),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(build_strategy_card(init_cfg), lg=6, md=12),
                    dbc.Col(build_chart_display_card(init_cfg), lg=6, md=12),
                ]
            ),
        ]
    )

    strategy_title_section = html.Div(
        [
            html.H6("STRATEGY CONFIGURATION", className="subtitle padded mt-5"),
            html.P(
                "Fine-tune strategy parameters, indicators, market grouping, and filtering criteria.",
                className="text-secondary text-center mb-4 small page-sub-desc",
            ),
        ]
    )

    strategy_rows = html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(build_stock_filter_card(init_cfg), lg=6, md=12),
                    dbc.Col(build_grouping_settings_card(init_cfg), lg=6, md=12),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        build_strategy_sub_card("macd", BT_MACD_KEYS, init_cfg),
                        lg=6,
                        md=12,
                    ),
                    dbc.Col(
                        build_strategy_sub_card("moving_average", BT_MA_KEYS, init_cfg),
                        lg=6,
                        md=12,
                    ),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        build_strategy_sub_card("volume_ma", BT_VOL_KEYS, init_cfg),
                        lg=6,
                        md=12,
                    ),
                    dbc.Col(
                        build_strategy_sub_card(
                            "window_metrics", BT_WIN_KEYS, init_cfg
                        ),
                        lg=6,
                        md=12,
                    ),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        build_strategy_sub_card("capital_flow", BT_FLOW_KEYS, init_cfg),
                        lg=6,
                        md=12,
                    ),
                    dbc.Col(
                        build_ma_convergence_card(init_cfg),
                        lg=6,
                        md=12,
                    ),
                ]
            ),
            dbc.Row(
                [
                    dbc.Col(
                        build_exit_rules_card(init_cfg),
                        lg=12,
                        md=12,
                    ),
                ]
            ),
        ]
    )

    schedule_title_section = html.Div(
        [
            html.H6("SCHEDULE CONFIGURATION", className="subtitle padded mt-5"),
            html.P(
                "Set crontab timings for strategy execution and proxy data collection.",
                className="text-secondary text-center mb-4 small page-sub-desc",
            ),
        ]
    )

    schedule_rows = html.Div(
        [
            dbc.Row(
                [
                    dbc.Col(
                        create_schedule_card(init_cfg),
                        lg=12,
                        md=12,
                    ),
                ]
            ),
        ]
    )

    # 4. 底部操作按钮区域（包含 Save as Default 按钮）
    button_section = html.Div(
        [
            dbc.Button(
                "Reset to Default",
                id="btn-reset",
                color="secondary",
                outline=True,
                className="py-2 rounded-2 weight-btn-reset",
                style={"flex": "1 1 0"},
            ),
            dbc.Button(
                "Save as Default",
                id="btn-save-default",
                color="warning",
                className="py-2 rounded-2 weight-btn-save-default",
                style={"flex": "1 1 0"},
            ),
            dbc.Button(
                "Save to JSON",
                id="btn-save",
                color="primary",
                className="py-2 rounded-2 weight-btn-save",
                style={"flex": "1 1 0"},
            ),
        ],
        className="d-flex gap-4 mt-5 mb-4 flex-row grp-inputs-row",
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
        weight_title_section,
        weight_rows,
        strategy_title_section,
        strategy_rows,
        schedule_title_section,
        schedule_rows,
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


# -------------------------- 回调注册主函数 --------------------------
def register_callbacks(app: Dash):
    bt_inputs = (
        [Input(f"slider_bt_{k}", "value") for k in BT_MACD_KEYS]
        + [Input(f"slider_bt_{k}", "value") for k in BT_MA_KEYS]
        + [Input(f"slider_bt_{k}", "value") for k in BT_VOL_KEYS]
        + [Input(f"slider_bt_{k}", "value") for k in BT_WIN_KEYS]
        + [Input(f"slider_bt_{k}", "value") for k in BT_FLOW_KEYS]
        + [Input(f"input_bt_{k}", "value") for k in BT_CONVERGENCE_KEYS]
        + [Input(f"input_bt_{k}", "value") for k in BT_EXIT_KEYS]
    )

    bt_outputs = (
        [Output(f"slider_bt_{k}", "value") for k in BT_MACD_KEYS]
        + [Output(f"slider_bt_{k}", "value") for k in BT_MA_KEYS]
        + [Output(f"slider_bt_{k}", "value") for k in BT_VOL_KEYS]
        + [Output(f"slider_bt_{k}", "value") for k in BT_WIN_KEYS]
        + [Output(f"slider_bt_{k}", "value") for k in BT_FLOW_KEYS]
        + [Output(f"input_bt_{k}", "value") for k in BT_CONVERGENCE_KEYS]
        + [Output(f"input_bt_{k}", "value") for k in BT_EXIT_KEYS]
    )

    schedule_inputs = [Input(f"input_cron_{k}", "value") for k in SCHEDULE_KEYS]
    schedule_outputs = [Output(f"input_cron_{k}", "value") for k in SCHEDULE_KEYS]

    all_slider_input_list = (
        [Input(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Input(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Input(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Input(f"slider_sta_{k}", "value") for k in STA_KEYS]
        + [Input(f"slider_str_{k}", "value") for k in STR_KEYS]
        + [Input(f"slider_stk_{k}", "value") for k in STK_KEYS]
        + [Input(f"slider_cht_{k}", "value") for k in CHT_KEYS]
        + [
            Input("grp_market", "value"),
            Input("grp_mode", "value"),
            Input("grp_bins", "value"),
            Input("grp_n_groups", "value"),
            Input("grp_top_n", "value"),
            Input("etf_min_threshold", "value"),
            Input("etf_n_groups", "value"),
            Input("etf_top_n", "value"),
        ]
        + bt_inputs
        + schedule_inputs
    )

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
            Output("etf_min_threshold", "value"),
            Output("etf_n_groups", "value"),
            Output("etf_top_n", "value"),
        ]
        + bt_outputs
        + schedule_outputs
    )

    @app.callback(
        [Output("grp_bins", "disabled"), Output("grp_n_groups", "disabled")],
        Input("grp_mode", "value"),
    )
    def toggle_grouping_mode(mode):
        if mode == "manual":
            return False, True
        return True, False

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
        def_cfg = load_default_config()
        def_grp = def_cfg.get(grp_key, {})
        grp_cfg = cfg.get(grp_key, def_grp)

        return (
            grp_cfg.get("grouping_mode", "manual"),
            grp_cfg.get("bins", ""),
            grp_cfg.get("n_groups", 5),
            grp_cfg.get("top_n_per_group", 100),
        )

    @app.callback(
        Output("etf_settings_container", "style"), Input("grp_market", "value")
    )
    def toggle_etf_settings(market):
        if market == "cn":
            return {"display": "block"}
        return {"display": "none"}

    # -------------------------- 逻辑3：Reset 恢复默认 --------------------------
    @app.callback(
        reset_outputs,
        Input("btn-reset", "n_clicks"),
        prevent_initial_call=True,
    )
    def reset_to_default(n_clicks):
        if not n_clicks:
            return [no_update] * len(reset_outputs)

        def_cfg = load_default_config()

        root_vals = [def_cfg.get("weights", {}).get(k) for k in ROOT_KEYS]
        ind_vals = [
            def_cfg.get("sub_weights", {}).get("industry", {}).get(k) for k in IND_KEYS
        ]
        pnl_vals = [
            def_cfg.get("sub_weights", {}).get("pnl", {}).get(k) for k in PNL_KEYS
        ]
        sta_vals = [
            def_cfg.get("sub_weights", {}).get("stability", {}).get(k) for k in STA_KEYS
        ]
        str_vals = [
            def_cfg.get("sub_weights", {}).get("strategy", {}).get(k) for k in STR_KEYS
        ]
        stk_vals = [def_cfg.get("stock_filter", {}).get(k) for k in STK_KEYS]
        cht_vals = [def_cfg.get("chart_display", {}).get(k) for k in CHT_KEYS]

        grp_cn = def_cfg.get("grouping_settings_cn", {})
        etf_cfg = def_cfg.get("grouping_settings_etf", {})
        grp_vals = [
            "cn",
            grp_cn.get("grouping_mode", "manual"),
            grp_cn.get("bins", ""),
            grp_cn.get("n_groups", 5),
            grp_cn.get("top_n_per_group", 100),
            format_to_sci(etf_cfg.get("min_threshold", "5e9")),
            etf_cfg.get("n_groups", 1),
            etf_cfg.get("top_n_per_group", 50),
        ]

        bt_cfg = def_cfg.get("backtest_settings", {})
        bt_macd = [bt_cfg.get("macd", {}).get(k) for k in BT_MACD_KEYS]
        bt_ma = [bt_cfg.get("moving_average", {}).get(k) for k in BT_MA_KEYS]
        bt_vol = [bt_cfg.get("volume_ma", {}).get(k) for k in BT_VOL_KEYS]
        bt_win = [bt_cfg.get("window_metrics", {}).get(k) for k in BT_WIN_KEYS]
        bt_flow = [bt_cfg.get("capital_flow", {}).get(k) for k in BT_FLOW_KEYS]
        bt_sqz = [bt_cfg.get("ma_convergence", {}).get(k) for k in BT_CONVERGENCE_KEYS]
        bt_exit = [bt_cfg.get("exit_rules", {}).get(k) for k in BT_EXIT_KEYS]

        sch_cfg = def_cfg.get("schedule_settings", {})
        cron_vals = [sch_cfg.get(k) for k in SCHEDULE_KEYS]

        return (
            root_vals
            + ind_vals
            + pnl_vals
            + sta_vals
            + str_vals
            + stk_vals
            + cht_vals
            + grp_vals
            + bt_macd
            + bt_ma
            + bt_vol
            + bt_win
            + bt_flow
            + bt_sqz
            + bt_exit
            + cron_vals
        )

    # -------------------------- 逻辑2：Save 与 Save as Default --------------------------
    @app.callback(
        [Output("json-preview", "value"), Output("save-msg", "children")],
        [Input("btn-save", "n_clicks"), Input("btn-save-default", "n_clicks")]
        + all_slider_input_list,
    )
    def update_and_save(btn_save_clicks, btn_save_default_clicks, *args):
        idx = 0
        root_vals = args[idx : idx + len(ROOT_KEYS)]
        idx += len(ROOT_KEYS)

        ind_vals = args[idx : idx + len(IND_KEYS)]
        idx += len(IND_KEYS)

        pnl_vals = args[idx : idx + len(PNL_KEYS)]
        idx += len(PNL_KEYS)

        sta_vals = args[idx : idx + len(STA_KEYS)]
        idx += len(STA_KEYS)

        str_vals = args[idx : idx + len(STR_KEYS)]
        idx += len(STR_KEYS)

        stk_vals = args[idx : idx + len(STK_KEYS)]
        idx += len(STK_KEYS)

        cht_vals = args[idx : idx + len(CHT_KEYS)]
        idx += len(CHT_KEYS)

        (
            market,
            mode,
            bins,
            n_groups,
            top_n,
            etf_min,
            etf_n,
            etf_top_n,
        ) = args[idx : idx + 8]
        idx += 8

        bt_macd_vals = args[idx : idx + len(BT_MACD_KEYS)]
        idx += len(BT_MACD_KEYS)

        bt_ma_vals = args[idx : idx + len(BT_MA_KEYS)]
        idx += len(BT_MA_KEYS)

        bt_vol_vals = args[idx : idx + len(BT_VOL_KEYS)]
        idx += len(BT_VOL_KEYS)

        bt_win_vals = args[idx : idx + len(BT_WIN_KEYS)]
        idx += len(BT_WIN_KEYS)

        bt_flow_vals = args[idx : idx + len(BT_FLOW_KEYS)]
        idx += len(BT_FLOW_KEYS)

        bt_sqz_vals = args[idx : idx + len(BT_CONVERGENCE_KEYS)]
        idx += len(BT_CONVERGENCE_KEYS)

        bt_exit_vals = args[idx : idx + len(BT_EXIT_KEYS)]
        idx += len(BT_EXIT_KEYS)

        schedule_vals = args[idx : idx + len(SCHEDULE_KEYS)]

        current_cfg = load_config()

        current_cfg["weights"] = dict(zip(ROOT_KEYS, root_vals))
        current_cfg["sub_weights"]["industry"] = dict(zip(IND_KEYS, ind_vals))
        current_cfg["sub_weights"]["pnl"] = dict(zip(PNL_KEYS, pnl_vals))
        current_cfg["sub_weights"]["stability"] = dict(zip(STA_KEYS, sta_vals))
        current_cfg["sub_weights"]["strategy"] = dict(zip(STR_KEYS, str_vals))
        current_cfg["stock_filter"] = dict(zip(STK_KEYS, stk_vals))
        current_cfg["chart_display"] = dict(zip(CHT_KEYS, cht_vals))

        target_grp_key = f"grouping_settings_{market}"
        current_cfg[target_grp_key] = {
            "grouping_mode": mode,
            "bins": bins,
            "n_groups": n_groups,
            "top_n_per_group": top_n,
        }

        current_cfg["grouping_settings_etf"] = {
            "min_threshold": etf_min,
            "n_groups": etf_n,
            "top_n_per_group": etf_top_n,
        }

        current_cfg["backtest_settings"] = {
            "macd": dict(zip(BT_MACD_KEYS, bt_macd_vals)),
            "moving_average": dict(zip(BT_MA_KEYS, bt_ma_vals)),
            "volume_ma": dict(zip(BT_VOL_KEYS, bt_vol_vals)),
            "window_metrics": dict(zip(BT_WIN_KEYS, bt_win_vals)),
            "capital_flow": dict(zip(BT_FLOW_KEYS, bt_flow_vals)),
            "ma_convergence": dict(zip(BT_CONVERGENCE_KEYS, bt_sqz_vals)),
            "exit_rules": dict(zip(BT_EXIT_KEYS, bt_exit_vals)),
        }

        schedule_dict = dict(zip(SCHEDULE_KEYS, schedule_vals))
        current_cfg["schedule_settings"] = schedule_dict

        preview_json = json.dumps(current_cfg, indent=4, ensure_ascii=False)
        triggered_id = ctx.triggered_id
        msg = ""

        if triggered_id in ("btn-save", "btn-save-default"):
            role = session.get("role")
            if role != "admin":
                msg = html.Span(
                    "⛔ Only super administrators can modify the configuration.",
                    style={"color": "#dc3545"},
                )
            else:
                try:
                    if triggered_id == "btn-save-default":
                        save_as_default_config(current_cfg)
                        msg_text = "✅ Saved to both default & active configurations successfully."
                    else:
                        save_config(current_cfg)
                        msg_text = "✅ Active configuration saved successfully."

                    export_dynamic_list()

                    cron_updated = CronManager.update_system_cron(schedule_dict)

                    if not cron_updated:
                        msg_text += " (Crontab update failed)"

                    msg = html.Span(msg_text, style={"color": "#198754"})
                except Exception as e:
                    msg = html.Span(
                        f"❌ Save failed: {str(e)}", style={"color": "#dc3545"}
                    )

        return preview_json, msg
