from datetime import datetime
import json
import os
from dash import Dash, Input, Output, State, ctx, dcc, html
import dash_bootstrap_components as dbc
from finance import FINANCE_ROOT
from finance.dashreport.data_loader import ReportDataLoader
from finance.dashreport.utils import Header
from finance.utility.toolkit import ToolKit
from flask import session
import pandas as pd

# -------------------------- 路径与默认配置 --------------------------
JSON_FILE_PATH = os.path.join(FINANCE_ROOT, "utility", "scoring_weights.json")

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
    },
    "chart_display": {
        "chart_time_range": 120,
        "minichart_time_range": 60,
        "kline_limit": 10,
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
    # 图表显示范围
    "chart_display": "Chart Display",
    "chart_time_range": "Chart Time Range",
    "minichart_time_range": "Mini Chart Time Range",
    "kline_limit": "Max Klines",
}


# 文件读写工具
def init_json_file():
    utility_full_path = os.path.join(FINANCE_ROOT, "utility")
    os.makedirs(utility_full_path, exist_ok=True)
    if not os.path.exists(JSON_FILE_PATH):
        with open(JSON_FILE_PATH, "w", encoding="utf-8") as f:
            json.dump(DEFAULT_CONFIG, f, ensure_ascii=False, indent=4)


def load_config() -> dict:
    with open(JSON_FILE_PATH, "r", encoding="utf-8") as f:
        return json.load(f)


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
# 【新增：静态键名定义】
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
                            marks={
                                0.25: "0.25",
                                0.5: "0.5",
                                0.75: "0.75",
                                1: "1",
                            },
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
                            marks={
                                0.25: "0.25",
                                0.5: "0.5",
                                0.75: "0.75",
                                1: "1",
                            },
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
                            marks={
                                0.25: "0.25",
                                0.5: "0.5",
                                0.75: "0.75",
                                1: "1",
                            },
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
                            marks={
                                0.25: "0.25",
                                0.5: "0.5",
                                0.75: "0.75",
                                1: "1",
                            },
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
                            marks={
                                0.25: "0.25",
                                0.5: "0.5",
                                0.75: "0.75",
                                1: "1",
                            },
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


# 【新增：Stock Filter 卡片构建】
def build_stock_filter_card(cfg):
    row_list = []
    for k in STK_KEYS:
        display_text = LABEL_MAPPING.get(k, k)
        # 根据是否是天数/整数控制 Slider 的步长与区间
        is_days = k == "collection_days"
        min_v = 1 if is_days else 0
        max_v = 30 if is_days else 1
        step_v = 1 if is_days else 0.01
        marks_v = (
            {10: "10", 20: "20", 30: "30"}
            if is_days
            else {0.25: "0.25", 0.5: "0.5", 0.75: "0.75", 1: "1"}
        )

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
                            value=cfg["stock_filter"][k],
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


# 【新增：Chart Display 卡片构建】
def build_chart_display_card(cfg):
    row_list = []
    for k in CHT_KEYS:
        display_text = LABEL_MAPPING.get(k, k)

        # ---------------- 1. 内部判断并设置 slider 参数 ----------------
        if k == "kline_limit":
            min_v = 0
            max_v = 20
            step_v = 1  # 若只希望固定切到 0, 10, 20 档位，可改为 10
            marks_v = {0: "0", 10: "10", 20: "20"}
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
            # 兜底默认值
            min_v, max_v, step_v = 0, 100, 1
            marks_v = {}

        # ---------------- 2. 构建组件 Row ----------------
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(
                        display_text,
                        width=2,
                        className="mb-0 fw-normal l2_label",
                    ),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_cht_{k}",
                            min=min_v,
                            max=max_v,
                            step=step_v,
                            value=cfg.get("chart_display", {}).get(k, min_v),
                            marks=marks_v,
                            tooltip={
                                "placement": "bottom",
                                "always_visible": True,
                            },
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


# -------------------------- 页面结构拆分 --------------------------
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
            # 【新增：拼接 Stock Filter 卡片于第3行右侧】
            dbc.Col(build_stock_filter_card(init_cfg), lg=6, md=12),
        ]
    )
    # 【新增：第4行拼接 Chart Display 卡片】
    card_row_4 = dbc.Row(
        [
            dbc.Col(build_chart_display_card(init_cfg), lg=6, md=12),
        ]
    )

    button_section = html.Div(
        [
            dbc.Button(
                "Reset to Defaults",
                id="btn-reset",
                color="secondary",
                outline=True,
                className="py-2 rounded-2 weight-btn-reset flex-btn-item",
            ),
            dbc.Button(
                "Save to JSON",
                id="btn-save",
                color="primary",
                className="py-2 rounded-2 weight-btn-save flex-btn-item",
            ),
        ],
        className="d-flex gap-4 mt-5 mb-4 flex-row flex-wrap",
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
        card_row_4,  # 【新增】
        button_section,
        msg_section,
        preview_section,
    ]

    layout = html.Div(
        [
            Header(app),
            html.Div(
                sub_page_content,
                className="sub_page",
            ),
        ],
        className="page",
    )
    return layout


# -------------------------- 回调注册 --------------------------
def register_callbacks(app: Dash):
    # 扩展全量控制项列表
    all_slider_input_list = (
        [Input(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Input(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Input(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Input(f"slider_sta_{k}", "value") for k in STA_KEYS]
        + [Input(f"slider_str_{k}", "value") for k in STR_KEYS]
        + [Input(f"slider_stk_{k}", "value") for k in STK_KEYS]  # 【新增】
        + [Input(f"slider_cht_{k}", "value") for k in CHT_KEYS]  # 【新增】
    )

    reset_outputs = (
        [Output(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Output(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Output(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Output(f"slider_sta_{k}", "value") for k in STA_KEYS]
        + [Output(f"slider_str_{k}", "value") for k in STR_KEYS]
        + [Output(f"slider_stk_{k}", "value") for k in STK_KEYS]  # 【新增】
        + [Output(f"slider_cht_{k}", "value") for k in CHT_KEYS]  # 【新增】
    )

    # 重置按钮回调
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
        # 【新增：组装新增两组配置的重置默认值】
        for v in DEFAULT_CONFIG["stock_filter"].values():
            out.append(v)
        for v in DEFAULT_CONFIG["chart_display"].values():
            out.append(v)
        return out

    # JSON预览实时刷新
    @app.callback(
        Output("json-preview", "value"), all_slider_input_list, allow_duplicate=True
    )
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
        # 【新增：写入 stock_filter】
        for k in STK_KEYS:
            cfg["stock_filter"][k] = all_values[ptr]
            ptr += 1
        # 【新增：写入 chart_display】
        for k in CHT_KEYS:
            cfg["chart_display"][k] = all_values[ptr]
            ptr += 1

        return json.dumps(cfg, indent=4, ensure_ascii=False)

    # 保存配置写入文件
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
