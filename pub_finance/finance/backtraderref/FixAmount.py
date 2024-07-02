import backtrader as bt


class FixedAmount(bt.Sizer):
    params = (('amount', 10000),)  # Default fixed amount for each buy order

    def _getsizing(self, comminfo, cash, data, isbuy):
        if isbuy:
            return (lambda num: 1 if num < 1 else (num // 1))(
                self.params.amount / data.close[0]
            )
        else:
            position = self.broker.getposition(data)
            if not position.size:
                return 0  # No position to sell
            else:
                return position.size  # Sell the entire position
