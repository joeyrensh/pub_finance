from finance.dashreport.pages.page_layout import PageLayout


class Page:
    """
    Page 工具类：只需传prefix、可选显示chart/table即可生成 layout
    """

    def __init__(self, app, prefix, show_charts=None, show_tables=None):
        """
        :param app: Dash app 实例
        :param prefix: 页面 prefix, 例如 "cn" / "us"
        :param show_charts: 可选显示的 chart 类型列表
        :param show_tables: 可选显示的 table 名称列表
        """
        self.layout_obj = PageLayout(
            app, prefix, show_charts=show_charts, show_tables=show_tables
        )

    def get_layout(self):
        return self.layout_obj.get_layout()
