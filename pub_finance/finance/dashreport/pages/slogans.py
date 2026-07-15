from finance import FINANCE_ROOT
import json
import os
from dash import html, dcc, Input, Output, State, ctx, Dash
import dash_bootstrap_components as dbc
from finance.dashreport.utils import Header
from flask import session

# -------------------------- 路径与默认配置 --------------------------
JSON_FILE_PATH = os.path.join(FINANCE_ROOT, "utility", "scoring_weights.json")

DEFAULT_CONFIG = {
    "weights": {
        "industry": 0.2,
        "pnl": 0.3,
        "stability": 0.1,
        "erp": 0.2,
        "strategy": 0.2,
    },
    "sub_weights": {
        "industry": {"arrow": 0.6, "bracket": 0.4},
        "pnl": {"daily": 0.4, "weighted_return": 0.6},
        "stability": {"win_rate": 0.2, "avg_trans": 0.2, "sortino": 0.2, "maxdd": 0.4},
        "strategy": {"cnt": 0.6, "signal": 0.4},
    },
}

# 【新增：界面名称映射，只改展示，不改动底层key】
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


# -------------------------- 静态键名定义（修复：从DEFAULT_CONFIG读取，不依赖init_cfg） --------------------------
ROOT_KEYS = list(DEFAULT_CONFIG["weights"].keys())
IND_KEYS = list(DEFAULT_CONFIG["sub_weights"]["industry"].keys())
PNL_KEYS = list(DEFAULT_CONFIG["sub_weights"]["pnl"].keys())
STA_KEYS = list(DEFAULT_CONFIG["sub_weights"]["stability"].keys())
STR_KEYS = list(DEFAULT_CONFIG["sub_weights"]["strategy"].keys())


# -------------------------- 卡片构建函数（替换Label展示名称） --------------------------
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


# 【新增：策略因子子权重卡片】
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


# -------------------------- 页面区块拆分（对齐项目示例结构） --------------------------
def create_layout(app: Dash):
    # 每次访问页面，实时校验文件、读取最新磁盘配置
    init_json_file()
    init_cfg = load_config()
    # 标题区块
    title_section = html.Div(
        [
            html.H6("Weight Configuration", className="subtitle padded"),
            html.P(
                "Drag and drop to adjust weights freely.Save profile with one click.",
                className="text-secondary text-center mb-5 small page-sub-desc",
            ),
        ]
    )

    # 双列卡片行
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
    # 新增第三行：策略子权重卡片
    card_row_3 = dbc.Row(
        [
            dbc.Col(build_strategy_card(init_cfg), lg=6, md=12),
        ]
    )

    # 按钮操作区块
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

    # 提示信息
    msg_section = html.Div(
        id="save-msg", className="text-center mb-4 fw-medium save-message-tip"
    )

    # JSON预览区块
    preview_section = html.Div(
        [
            html.H6("Real-time Configuration Preview", className="subtitle padded"),
            dcc.Textarea(
                id="json-preview", className="json-preview-box", readOnly=True
            ),
        ]
    )

    # 拼接sub_page内部所有内容（列表形式，和项目范例完全一致）
    sub_page_content = [
        title_section,
        card_row_1,
        card_row_2,
        card_row_3,
        button_section,
        msg_section,
        preview_section,
    ]

    # 严格匹配你提供的外层layout骨架
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


# -------------------------- 回调注册（无自动分摊拖拽逻辑） --------------------------
def register_callbacks(app: Dash):
    # 数值实时展示
    @app.callback(
        [Output(f"val_root_{k}", "children") for k in ROOT_KEYS],
        [Input(f"slider_root_{k}", "value") for k in ROOT_KEYS],
        allow_duplicate=True,
    )
    def show_root_vals(*vals):
        return [f"{v:.2f}" for v in vals]

    @app.callback(
        [Output(f"val_ind_{k}", "children") for k in IND_KEYS],
        [Input(f"slider_ind_{k}", "value") for k in IND_KEYS],
        allow_duplicate=True,
    )
    def show_ind_vals(*vals):
        return [f"{v:.2f}" for v in vals]

    @app.callback(
        [Output(f"val_pnl_{k}", "children") for k in PNL_KEYS],
        [Input(f"slider_pnl_{k}", "value") for k in PNL_KEYS],
        allow_duplicate=True,
    )
    def show_pnl_vals(*vals):
        return [f"{v:.2f}" for v in vals]

    @app.callback(
        [Output(f"val_sta_{k}", "children") for k in STA_KEYS],
        [Input(f"slider_sta_{k}", "value") for k in STA_KEYS],
        allow_duplicate=True,
    )
    def show_sta_vals(*vals):
        return [f"{v:.2f}" for v in vals]

    # 新增策略子项数值显示回调
    @app.callback(
        [Output(f"val_str_{k}", "children") for k in STR_KEYS],
        [Input(f"slider_str_{k}", "value") for k in STR_KEYS],
        allow_duplicate=True,
    )
    def show_str_vals(*vals):
        return [f"{v:.2f}" for v in vals]

    # 重置按钮回调
    reset_outputs = (
        [Output(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Output(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Output(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Output(f"slider_sta_{k}", "value") for k in STA_KEYS]
        + [Output(f"slider_str_{k}", "value") for k in STR_KEYS]
    )
    all_slider_input_list = (
        [Input(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Input(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Input(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Input(f"slider_sta_{k}", "value") for k in STA_KEYS]
        + [Input(f"slider_str_{k}", "value") for k in STR_KEYS]
    )

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
        return json.dumps(cfg, indent=4, ensure_ascii=False)

    # 保存配置写入文件
    @app.callback(
        Output("save-msg", "children"),
        Input("btn-save", "n_clicks"),
        State("json-preview", "value"),
        prevent_initial_call=True,
    )
    def save_btn(n, json_str):
        # ----- 权限检查 -----
        role = session.get("role")
        if role != "admin":
            return html.Span(
                "⛔ Only super administrators can modify the configuration.",
                style={"color": "#dc3545"},
            )
        try:
            d = json.loads(json_str)
            save_config(d)
            return html.Span(
                "✅ Configuration file saved successfully.", style={"color": "#198754"}
            )
        except Exception as e:
            return html.Span(f"❌ Save failed: {str(e)}", style={"color": "#dc3545"})
