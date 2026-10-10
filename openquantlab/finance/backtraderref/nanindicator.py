import backtrader as bt


class NaNIndicator(bt.Indicator):
    """
    输出全长度 nan 的 line，用于数据不足占位
    """

    lines = ("nanline",)
    params = (("data", None),)  # 用 params 接收数据

    def __init__(self):
        data = self.p.data  # 从 params 获取
        if data is None:
            raise ValueError("NaNIndicator: 必须传入 data 参数")

        self.addminperiod(0)
        n = len(data)

        # 初始化 line
        for i in range(n):
            self.lines.nanline[i] = float("nan")
