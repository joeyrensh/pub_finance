from finance import FINANCE_ROOT
import json
import os
from dash import html, dcc, Input, Output, State, ctx, Dash
import dash_bootstrap_components as dbc
from finance.dashreport.utils import Header

# -------------------------- 路径与默认配置 --------------------------
JSON_FILE_PATH = os.path.join(FINANCE_ROOT, "utility", "scoring_weights.json")

DEFAULT_CONFIG = {
    "weights": {"industry": 0.25, "pnl": 0.3, "stability": 0.25, "erp": 0.2},
    "sub_weights": {
        "industry": {"arrow": 0.3, "bracket": 0.7},
        "pnl": {"daily": 0.3, "weighted_return": 0.7},
        "stability": {
            "win_rate": 0.2,
            "avg_trans": 0.1,
            "sortino": 0.35,
            "maxdd": 0.35,
        },
    },
}

# 【新增：界面名称映射，只改展示，不改动底层key】
LABEL_MAPPING = {
    # 全局权重
    "industry": "行业因子",
    "pnl": "损益因子",
    "stability": "稳定性因子",
    "erp": "股权溢价因子",
    # industry子项
    "arrow": "上升趋势",
    "bracket": "行业排名",
    # pnl子项
    "daily": "日度损益",
    "weighted_return": "加权损益",
    # stability子项
    "win_rate": "交易胜率",
    "avg_trans": "交易频次",
    "sortino": "索提诺比率",
    "maxdd": "最大回撤",
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


init_json_file()
init_cfg = load_config()

# -------------------------- 静态键名定义 --------------------------
ROOT_KEYS = list(init_cfg["weights"].keys())
IND_KEYS = list(init_cfg["sub_weights"]["industry"].keys())
PNL_KEYS = list(init_cfg["sub_weights"]["pnl"].keys())
STA_KEYS = list(init_cfg["sub_weights"]["stability"].keys())


# -------------------------- 卡片构建函数（替换Label展示名称） --------------------------
def build_root_card():
    row_list = []
    for k in ROOT_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(display_text, width=2, className="mb-0 fw-normal"),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_root_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=init_cfg["weights"][k],
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
                html.Label("全局权重", className="fs-5 fw-bold text-dark"),
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_industry_card():
    row_list = []
    for k in IND_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(display_text, width=2, className="mb-0 fw-normal"),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_ind_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=init_cfg["sub_weights"]["industry"][k],
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
                html.Label("行业子权重", className="fs-5 fw-bold text-dark")
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_pnl_card():
    row_list = []
    for k in PNL_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(display_text, width=2, className="mb-0 fw-normal"),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_pnl_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=init_cfg["sub_weights"]["pnl"][k],
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
                html.Label("损益子权重", className="fs-5 fw-bold text-dark")
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


def build_stability_card():
    row_list = []
    for k in STA_KEYS:
        display_text = LABEL_MAPPING[k]
        row_list.append(
            dbc.Row(
                [
                    dbc.Label(display_text, width=2, className="mb-0 fw-normal"),
                    dbc.Col(
                        dcc.Slider(
                            id=f"slider_sta_{k}",
                            min=0,
                            max=1,
                            step=0.01,
                            value=init_cfg["sub_weights"]["stability"][k],
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
                html.Label("稳定性子权重", className="fs-5 fw-bold text-dark")
            ),
            dbc.CardBody(row_list, className="py-3 card-body"),
        ],
        className="h-100 shadow-sm border-0 rounded-3 mb-4 weight-config-card",
    )


# -------------------------- 页面区块拆分（对齐项目示例结构） --------------------------
def create_layout(app: Dash):
    # 标题区块
    title_section = html.Div(
        [
            html.H6("权重配置", className="subtitle padded"),
            html.P(
                "自由拖拽调整权重数值，无强制归一约束，一键保存配置文件",
                className="text-secondary text-center mb-5 small page-sub-desc",
            ),
        ]
    )

    # 双列卡片行
    card_row_1 = dbc.Row(
        [
            dbc.Col(build_root_card(), lg=6, md=12),
            dbc.Col(build_industry_card(), lg=6, md=12),
        ]
    )
    card_row_2 = dbc.Row(
        [
            dbc.Col(build_pnl_card(), lg=6, md=12),
            dbc.Col(build_stability_card(), lg=6, md=12),
        ]
    )

    # 按钮操作区块
    button_section = html.Div(
        [
            dbc.Button(
                "重置默认参数",
                id="btn-reset",
                color="secondary",
                outline=True,
                className="py-2 rounded-2 weight-btn-reset flex-btn-item",
            ),
            dbc.Button(
                "保存配置至JSON",
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
            html.H6("实时配置预览", className="subtitle padded"),
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

    # 重置按钮回调
    reset_outputs = (
        [Output(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Output(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Output(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Output(f"slider_sta_{k}", "value") for k in STA_KEYS]
    )
    all_slider_input_list = (
        [Input(f"slider_root_{k}", "value") for k in ROOT_KEYS]
        + [Input(f"slider_ind_{k}", "value") for k in IND_KEYS]
        + [Input(f"slider_pnl_{k}", "value") for k in PNL_KEYS]
        + [Input(f"slider_sta_{k}", "value") for k in STA_KEYS]
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
        return json.dumps(cfg, indent=4, ensure_ascii=False)

    # 保存配置写入文件
    @app.callback(
        Output("save-msg", "children"),
        Input("btn-save", "n_clicks"),
        State("json-preview", "value"),
        prevent_initial_call=True,
    )
    def save_btn(n, json_str):
        try:
            d = json.loads(json_str)
            save_config(d)
            return html.Span("✅ 配置文件保存成功", style={"color": "#198754"})
        except Exception as e:
            return html.Span(f"❌ 保存失败：{str(e)}", style={"color": "#dc3545"})
