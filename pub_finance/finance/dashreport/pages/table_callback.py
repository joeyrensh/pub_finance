# finance/dashreport/table_callback.py

from dash import (
    callback,
    Output,
    Input,
    MATCH,
    html,
    State,
    no_update,
    ctx,
    ALL,
    callback_context,
)
from finance.dashreport.data_loader import ReportDataLoader
from finance.dashreport.utils import Header, make_dash_format_table
import threading
import os
from openai import OpenAI
from dotenv import load_dotenv
import httpx
from flask import session
import re

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

        @callback(
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
                    return no_update

                data = ReportDataLoader.load(
                    prefix=prefix,
                    datasets=datasets,
                )

                df_overall = data.get("overall")

                # ===== 构造具体 table =====
                if table_name == "category":
                    if data["category"]["df"].empty:
                        return html.Div(
                            "Run Backtest",
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
                            "Run Backtest",
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
                            "Run Backtest",
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
                            "Run Backtest",
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
                return no_update

            return no_update

        def register_table_ai_callback(app, table_type):
            """为指定类型的表格创建并注册专用的 AI 摘要抓取回调"""

            @callback(
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
                if is_loading or not ctx.triggered_id:
                    return no_update

                # 物理下标单点切片，绝不全量循环
                triggered_id = ctx.triggered_id
                try:
                    idx = all_table_id_list.index(triggered_id)
                    selected_cells = all_selected_list[idx]
                    virtual_data = all_virtual_data[idx]
                except (ValueError, IndexError):
                    return no_update

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

        @callback(
            Output("ai_summary_wrapper", "style"),
            Output("global_btn_ai_summary", "style"),  # 控制 AI 按钮显隐
            Output("ai_summary_done", "data"),
            Input("store_selected_cell_info", "data"),
            Input("ai_is_loading", "data"),
            State("ai_summary_done", "data"),
            prevent_initial_call=True,
        )
        def toggle_ai_summary_section(cell_info, is_loading, ai_summary_done):
            page = (
                cell_info.get("page", "").lower() if isinstance(cell_info, dict) else ""
            )
            table = (
                cell_info.get("table", "").lower()
                if isinstance(cell_info, dict)
                else ""
            )

            is_main_page = page in ["cn", "us"]
            is_trade_table = table == "trade"
            is_secondary_page = page in ["cn_dynamic", "us_dynamic"]

            # 只有当 page 为 cn/us 且 table 不是 trade 时，margin_bottom 才为 "0px"
            margin_bottom = "0px" if (is_main_page and not is_trade_table) else "20px"
            margin_top = "-20px" if (is_secondary_page or is_trade_table) else "0px"

            hide_wrapper = {
                "display": "none",
                "marginBottom": margin_bottom,
                "marginTop": margin_top,
            }
            show_wrapper = {
                "position": "relative",
                "display": "block",
                "marginBottom": margin_bottom,
                "marginTop": margin_top,
            }

            hide_btn = {"display": "none"}
            show_btn = {"display": "inline-block", "cursor": "pointer"}

            if ctx.triggered and ctx.triggered_id == "store_selected_cell_info":
                ai_summary_done = False

            # 权限和加载状态
            if session.get("role") != "admin":
                return hide_wrapper, hide_btn, ai_summary_done
            if is_loading:
                return show_wrapper, hide_btn, ai_summary_done

            # 若摘要已完成（done=True），则隐藏按钮；否则正常显示
            if ai_summary_done:
                return show_wrapper, hide_btn, ai_summary_done

            if cell_info is None:
                return hide_wrapper, hide_btn, ai_summary_done

            return show_wrapper, show_btn, ai_summary_done

        @callback(
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
                return "No valid cell selected", None, False, 0, True

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
                        f"检索股票【{sym}】最新资讯，输出单段150字内无标题纯文本结论。\n"
                        f"融合字段：[财报]营收与利润增幅、自由现金流、折算动态PE及日期；[业务]主营与行业竞争；[动向]最新利好利空；[机构]大行评级。\n"
                        f"禁忌：禁序号/标题/前缀/综上所述；禁A股/美股等市场名称。格式：数字用阿拉伯；日期用X月X日或YYYY-MM-DD；价格保留2位小数；百分比用%。"
                    )
                    response = client.responses.create(
                        model="qwen3.7-max",
                        # model="qwen3.7-plus",
                        input=prompt,
                        tools=[{"type": "web_search"}, {"type": "web_extractor"}],
                        max_output_tokens=200,
                        extra_body={"enable_thinking": True},
                    )
                    ai_result = response.output_text.strip()
                except httpx.TimeoutException:
                    ai_result = "web search timed out; please try again later."
                except Exception as e:
                    ai_result = f"AI analysis call exception：{str(e)}"
                finally:
                    if "http_client" in locals():
                        http_client.close()

                # 计算完毕，使用线程锁安全地更新结果
                with cache_lock:
                    AI_TASK_CACHE[key] = {
                        "status": "success",
                        "result": f"Symbol: {sym} Name: {nm.strip('88+')} Core Insight: {ai_result}",
                    }

            # 异步启动
            thread = threading.Thread(
                target=async_openai_worker, args=(symbol, name, task_key)
            )
            thread.daemon = True
            thread.start()

            return "Performing analysis, please wait...", None, True, 0, False

        @callback(
            Output("ai_summary_box", "children", allow_duplicate=True),
            Output("ai_summary_loading_trigger", "children", allow_duplicate=True),
            Output("ai_is_loading", "data", allow_duplicate=True),
            Output("ai_polling_timer", "disabled", allow_duplicate=True),
            Output("ai_summary_done", "data", allow_duplicate=True),
            Input("ai_polling_timer", "n_intervals"),
            State("store_selected_cell_info", "data"),
            prevent_initial_call=True,
        )
        def poll_ai_status(n_intervals, cell_info):
            if not cell_info or not ctx.triggered_id:
                return no_update, no_update, no_update, no_update, no_update

            task_key = f"{cell_info.get('symbol', '')}"

            with cache_lock:
                task_data = AI_TASK_CACHE.get(task_key)

            if not task_data or task_data.get("status") == "loading":
                if n_intervals > 120:  # 超时控制
                    return (
                        "⚠️ Analysis timed out; please check and try again.",
                        None,
                        False,
                        True,
                        False,
                    )

                return no_update, no_update, no_update, no_update, no_update

            if task_data.get("status") == "success":
                final_text = task_data.get("result", "No valid content obtained")

                with cache_lock:
                    AI_TASK_CACHE.pop(task_key, None)

                return final_text, None, False, True, True

            return no_update, no_update, no_update, no_update, no_update

        @callback(
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

        target_tables = [("cn", "detail"), ("cn", "cn_etf"), ("us", "detail")]

        # 监听表格勾选/点击变动，更新对应的 selected-store
        for page, table in target_tables:

            @callback(
                Output(
                    {"type": "selected-store", "page": page, "table": table}, "data"
                ),
                # 1. 改为监听 selected_row_ids，不再依赖会因筛选错位的 selected_rows 行号
                Input(
                    {"type": "auto-table", "page": page, "table": table},
                    "selected_row_ids",
                ),
                State({"type": "auto-table", "page": page, "table": table}, "data"),
            )
            def sync_selected(selected_row_ids, data, page=page, table=table):
                if not selected_row_ids or not data:
                    return []

                # 建立 row_id (即 IDX) -> 真实 SYMBOL 的映射表
                id_to_symbol = {}
                for row in data:
                    row_id = str(row.get("id", ""))
                    sym = row.get("SYMBOL_o") or row.get("SYMBOL") or row.get("IDX")
                    if row_id and sym:
                        id_to_symbol[row_id] = str(sym).strip()

                symbols = []
                for row_id in selected_row_ids:
                    sym = id_to_symbol.get(str(row_id))
                    if sym and sym not in symbols:
                        symbols.append(sym)

                return symbols

            # 2. 点击 Subtitle 携带 symbols 跳转至回测页（保持原逻辑不变）
            @callback(
                [
                    Output("url", "pathname", allow_duplicate=True),
                    Output("selected-symbols-store", "data", allow_duplicate=True),
                ],
                Input(
                    {"type": "subtitle-click", "page": page, "table": table}, "n_clicks"
                ),
                State({"type": "selected-store", "page": page, "table": table}, "data"),
                prevent_initial_call=True,
            )
            def jump_to_backtest(n_clicks, symbols, page=page, table=table):
                if not n_clicks or not symbols:
                    return no_update, no_update
                target_market = "us" if page == "us" else "cn"
                return "/dash-financial-report/backtest", {
                    "market": target_market,
                    "symbols": symbols,
                }

            # 1. 设置 Table ID 匹配模式
            table_id = {"type": "auto-table", "page": page, "table": table}

            app.clientside_callback(
                """
                function(activeCell, currentSelectedIds, currentConditionalStyles) {
                    // 1. 安全拦截：如果 activeCell 为空或点击的不是 IDX 列，清空 activeCell 以备下次点击
                    if (!activeCell || activeCell.column_id !== 'IDX' || !activeCell.row_id) {
                        return [window.dash_clientside.no_update, window.dash_clientside.no_update, null];
                    }
                    
                    var rowId = activeCell.row_id;
                    var selectedIds = currentSelectedIds ? currentSelectedIds.slice() : [];
                    var idx = selectedIds.indexOf(rowId);
                    
                    // 2. 切换选中状态（选中 -> 取消；未选中 -> 选中）
                    if (idx > -1) {
                        selectedIds.splice(idx, 1);
                    } else {
                        selectedIds.push(rowId);
                    }
                    
                    // 3. 过滤并清除旧的 IDX 选中高亮规则
                    var existingStyles = currentConditionalStyles ? currentConditionalStyles.slice() : [];
                    var baseStyles = existingStyles.filter(function(rule) {
                        return !rule._is_idx_selected;
                    });

                    // 4. 构建新的 IDX 选中高亮规则
                    if (selectedIds.length > 0) {
                        var query = selectedIds.map(function(id) {
                            return '({id} = "' + id + '")';
                        }).join(" || ");

                        baseStyles.push({
                            "if": {
                                "filter_query": query,
                                "column_id": "IDX"
                            },
                            "color": "var(--highlight-symbol-color, #FF4D4F)",
                            "fontWeight": "bold",
                            "_is_idx_selected": true  // 标志位
                        });
                    }
                    
                    // 5. 返回更新后的 selected_row_ids、style_data_conditional，同时将 active_cell 重置为 null
                    // 重置 active_cell 为 null 是实现“同一个 Cell 连点瞬间响应”的关键！
                    return [selectedIds, baseStyles, null];
                }
                """,
                [
                    Output(table_id, "selected_row_ids"),
                    Output(table_id, "style_data_conditional"),
                    Output(table_id, "active_cell"),  # 核心增加：重置 active_cell
                ],
                Input(table_id, "active_cell"),
                [
                    State(table_id, "selected_row_ids"),
                    State(table_id, "style_data_conditional"),
                ],
                prevent_initial_call=True,
            )

        # ---------- 同步跳转的市场类型 ----------
        @callback(
            Output("backtest-market", "value"),
            Input("url", "pathname"),  # 监听页面路径加载
            State("selected-symbols-store", "data"),
            prevent_initial_call=False,
        )
        def sync_backtest_market(pathname, stored_data):
            if stored_data and isinstance(stored_data, dict):
                target_market = stored_data.get("market")
                if target_market:
                    return target_market

            return "cn"

        self._callback_registered = True
