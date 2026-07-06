# finance/dashreport/table_callback.py

from dash import callback, Output, Input, MATCH, html, State, no_update, ctx, ALL
import dash
import time
from finance.dashreport.data_loader import ReportDataLoader
from finance.dashreport.utils import Header, make_dash_format_table
import threading
import os
from openai import OpenAI
from dotenv import load_dotenv
import httpx

AI_TASK_CACHE = {}
cache_lock = threading.Lock()


class TableCallback:
    """
    通用 Table 回调管理器
    """

    _instance = None

    def __new__(cls):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
            cls._instance._initialized = False
        return cls._instance

    def __init__(self):
        if not self._initialized:
            self._callback_registered = False
            self._initialized = True

    def setup_callback(self, app):
        """
        注册 table 回调（整个 app 只调用一次）
        """
        if self._callback_registered:
            return

        @app.callback(
            Output(
                {
                    "type": "dynamic-table",
                    "page": MATCH,
                    "table": MATCH,
                },
                "children",
            ),
            Input("current-theme", "data"),
            Input(
                {
                    "type": "dynamic-table",
                    "page": MATCH,
                    "table": MATCH,
                },
                "id",
            ),
            prevent_initial_call=False,  # ⚠️ 允许首屏加载
        )
        def render_table(theme, table_id):
            prefix = table_id["page"]
            table_name = table_id["table"]

            try:
                # ===== 根据 table 类型选择 datasets =====
                if table_name == "category":
                    datasets = ("overall", "category")
                elif table_name == "detail":
                    datasets = ("overall", "detail")
                elif table_name == "detail_short":
                    datasets = ("overall", "detail_short")
                elif table_name == "cn_etf":
                    datasets = ("overall", "cn_etf")
                else:
                    return dash.no_update

                data = ReportDataLoader.load(
                    prefix=prefix,
                    datasets=datasets,
                )

                df_overall = data.get("overall")

                # ===== 构造具体 table =====
                if table_name == "category":
                    if data["category"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["category"]["df"],
                        data["category"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

                if table_name == "detail":
                    if data["detail"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["detail"]["df"],
                        data["detail"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

                if table_name == "detail_short":
                    if data["detail_short"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["detail_short"]["df"],
                        data["detail_short"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

                if table_name == "cn_etf":
                    if data["cn_etf"]["df"].empty:
                        return html.Div(
                            "请执行回测",
                            style={
                                "color": "#999",
                                "padding": "20px",
                                "textAlign": "center",
                            },
                        )
                    return make_dash_format_table(
                        data["cn_etf"]["df"],
                        data["cn_etf"]["formats"],
                        prefix,
                        df_overall.at[0, "end_date"],
                        table_name,
                    )

            except Exception as e:
                print(f"⚠️ Table render error [{table_name}]: {e}")
                return dash.no_update

            return dash.no_update

        def register_table_ai_callback(app, table_type):
            """为指定类型的表格创建并注册专用的 AI 摘要抓取回调"""

            @app.callback(
                Output("store_selected_cell_info", "data", allow_duplicate=True),
                Input(
                    {"type": "auto-table", "page": ALL, "table": table_type},
                    "selected_cells",
                ),
                State({"type": "auto-table", "page": ALL, "table": table_type}, "id"),
                State(
                    {"type": "auto-table", "page": ALL, "table": table_type},
                    "derived_viewport_data",
                ),
                State("ai_is_loading", "data"),
                prevent_initial_call=True,
            )
            def _capture_cell(
                all_selected_list, all_table_id_list, all_virtual_data, is_loading
            ):
                if is_loading or not dash.ctx.triggered_id:
                    return dash.no_update

                # 物理下标单点切片，绝不全量循环
                triggered_id = dash.ctx.triggered_id
                try:
                    idx = all_table_id_list.index(triggered_id)
                    selected_cells = all_selected_list[idx]
                    virtual_data = all_virtual_data[idx]
                except (ValueError, IndexError):
                    return dash.no_update

                if not selected_cells or not virtual_data:
                    return None

                cell = selected_cells[0]
                if cell.get("column_id") != "NAME":
                    return None

                row_idx = cell.get("row")
                if row_idx is None or row_idx >= len(virtual_data):
                    return None

                row = virtual_data[row_idx]
                return {
                    "row": row_idx,
                    "page": triggered_id["page"],
                    "table": triggered_id["table"],
                    "symbol": row.get("SYMBOL_o", row.get("SYMBOL", "")),
                    "name": row.get("NAME_o", row.get("NAME", "")),
                }

        [register_table_ai_callback(app, t) for t in ["detail", "trade"]]

        @app.callback(
            Output("global_btn_ai_summary", "style"),
            Input("store_selected_cell_info", "data"),
            Input("ai_is_loading", "data"),
            prevent_initial_call=True,
        )
        def toggle_ai_button_show(cell_info, is_loading):
            hide_style = {"display": "none"}
            show_style = {
                "display": "inline-block",
                "cursor": "pointer",
            }

            # 条件1：AI加载中 → 强制隐藏按钮
            if is_loading:
                return hide_style

            # 条件2：没有选中有效NAME单元格 → 隐藏按钮
            if cell_info is None:
                return hide_style

            # 选中合法NAME行 → 显示按钮
            return show_style

        @app.callback(
            Output("ai_summary_box", "children", allow_duplicate=True),
            Output("ai_summary_loading_trigger", "children", allow_duplicate=True),
            Output("ai_is_loading", "data", allow_duplicate=True),
            Output("ai_polling_timer", "n_intervals"),
            Output("ai_polling_timer", "disabled", allow_duplicate=True),
            Input("global_btn_ai_summary", "n_clicks"),
            State("store_selected_cell_info", "data"),
            prevent_initial_call=True,
        )
        def start_ai(n_clicks, cell_info):
            if not n_clicks or not cell_info:
                return "未选中有效NAME列单元格", None, False, 0, True

            symbol = cell_info.get("symbol", "")
            name = cell_info.get("name", "")
            task_key = f"{symbol}"

            # 使用线程锁安全地写入初始化状态
            with cache_lock:
                AI_TASK_CACHE[task_key] = {"status": "loading", "result": None}

            def async_openai_worker(sym, nm, key):
                load_dotenv()
                try:
                    # 设定 20 秒超时
                    http_client = httpx.Client(timeout=120)
                    client = OpenAI(
                        api_key=os.getenv("API_KEY"),
                        base_url="https://ws-uaeaan6mql1ieioa.cn-beijing.maas.aliyuncs.com/compatible-mode/v1",
                        http_client=http_client,
                    )
                    prompt = (
                        f"你是专业股票分析师，联网检索股票{sym}最新财报与近期资讯。\n"
                        f"请在 150 字以内直接输出针对该股的专业投资结论，严禁包含任何思考过程或客套引导语。\n\n"
                        f"【核心分析要点】\n"
                        f"1. 财务状况（营收增速、毛利、现金流关键特征）\n"
                        f"2. 主营业务与行业排名/竞争力\n"
                        f"3. 核心多空因素（利好/利空/管理层核心动向）\n\n"
                        f"硬性红线（严格执行）：全文严禁出现A股、美股等任何市场归属描述，全程只分析企业经营基本面。"
                    )
                    response = client.responses.create(
                        model="qwen3.7-max",
                        input=prompt,
                        tools=[{"type": "web_search"}, {"type": "web_extractor"}],
                        max_output_tokens=300,
                        extra_body={"enable_thinking": True},
                    )
                    ai_result = response.output_text.strip()
                except httpx.TimeoutException:
                    ai_result = "大模型联网检索检索超时，请稍后重试。"
                except Exception as e:
                    ai_result = f"AI分析调用异常：{str(e)}"
                finally:
                    if "http_client" in locals():
                        http_client.close()

                # 计算完毕，使用线程锁安全地更新结果
                with cache_lock:
                    AI_TASK_CACHE[key] = {
                        "status": "success",
                        "result": f"股票代码：{sym} 股票名称：{nm.strip('88+')} {ai_result}",
                    }

            # 异步启动
            thread = threading.Thread(
                target=async_openai_worker, args=(symbol, name, task_key)
            )
            thread.daemon = True
            thread.start()

            return "智能分析中，请稍候...", None, True, 0, False

        @app.callback(
            Output("ai_summary_box", "children", allow_duplicate=True),
            Output("ai_summary_loading_trigger", "children", allow_duplicate=True),
            Output("ai_is_loading", "data", allow_duplicate=True),
            Output("ai_polling_timer", "disabled", allow_duplicate=True),
            Input("ai_polling_timer", "n_intervals"),
            State("store_selected_cell_info", "data"),
            prevent_initial_call=True,
        )
        def poll_ai_status(n_intervals, cell_info):
            if not cell_info:
                return "请选择股票", None, False, True

            task_key = f"{cell_info.get('symbol', '')}"

            with cache_lock:
                task_data = AI_TASK_CACHE.get(task_key)

            if not task_data or task_data.get("status") == "loading":
                if n_intervals > 120:  # 超时控制
                    return "⚠️ 分析超时，请确认网络并重试。", None, False, True

                return dash.no_update, dash.no_update, dash.no_update, dash.no_update

            if task_data.get("status") == "success":
                final_text = task_data.get("result", "未获取到有效内容")

                with cache_lock:
                    AI_TASK_CACHE.pop(task_key, None)

                return final_text, None, False, True

            return dash.no_update

        @app.callback(
            Output({"type": "auto-table-count", "page": ALL, "table": ALL}, "children"),
            Input(
                {"type": "auto-table", "page": ALL, "table": ALL},
                "derived_virtual_indices",
            ),
        )
        def update_row_count(indices_list):
            return [
                f"Total {len(idx) if idx is not None else 0} Rows"
                for idx in indices_list
            ]

        self._callback_registered = True
