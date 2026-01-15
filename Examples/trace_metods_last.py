from __future__ import (absolute_import, division, print_function, unicode_literals)
from datetime import datetime
import itertools
import backtrader as bt
import backtrader.indicators as btind

from BacktraderQuikJunior.QJStore import QKStore


_STEP = itertools.count(1)
_LAST_BAR_DT = None  # начало бара, для которого уже печатали заголовок/разделитель
IS_LIVE = False

def trace(msg: str) -> None:
    if not IS_LIVE:
        return
    print(f"{next(_STEP):04d} | {datetime.now().strftime('%H:%M:%S.%f')[:-3]} | {msg}")

def _bar_begin(bar_dt) -> None:
    """
    Печатаем разделитель между барами и сбрасываем _STEP.
    Вызывать из индикатора ПЕРЕД его trace().
    """
    global _LAST_BAR_DT, _STEP

    if not IS_LIVE:
        return

    if _LAST_BAR_DT is None or bar_dt != _LAST_BAR_DT:
        print("NEW BAR JUST RECEIVED!! ", "-" * 100, flush=True)
        _STEP = itertools.count(1)
        _LAST_BAR_DT = bar_dt


class TraceSMA(btind.SMA):

    def next(self):
        dt = self.data.datetime.datetime(0)
        _bar_begin(dt)
        trace(f"IND(SMA).next | SMA period={self.p.period} | bar start time = {dt:%H:%M} | sma={self[0]:.6f}")


class TraceTradeAnalyzer(bt.analyzers.TradeAnalyzer):

    def next(self):
        dt = bt.num2date(self.data.datetime[0])
        trace(f"ANALYZER.next | bar start time = {dt:%H:%M}")
        # пусть базовый TradeAnalyzer продолжает собирать статистику
        super().next()

    # ----- notify family (анализатор) -----
    # Будут вызываться только если есть исполнения (ордера/трейды)

    def notify_order(self, order):
        trace(f"ANALYZER.notify_order | status={order.getstatusname()} | ref={order.ref}")

    def notify_trade(self, trade):
        trace(f"ANALYZER.notify_trade | pnl={trade.pnl:.2f}")


class TraceStrat(bt.strategies.MA_CrossOver):
    params = (
        # period for the fast&slow Moving Average
        ('fast', 10),
        ('slow', 20),
        ('_movav', TraceSMA)  # moving average to use
    )

    def __init__(self):
        print(f"STRATEGY.__init__ | Start CASH = {self.broker.getcash()}")
        super().__init__()  # создаст 2 SMA и CrossOver

    # ----- notify family (стратегия) -----

    def notify_store(self, msg, *args, **kwargs):
        trace(f"STRATEGY.notify_store msg={msg}")

    def notify_data(self, data, status, *args, **kwargs):
        global IS_LIVE
        # Изменение статуса приходящих баров
        data_status = data._getstatusname(status)
        print(f"STRATEGY.notify_data | Источник данных: {data.p.dataname}, перешел в статус: {data_status}")
        IS_LIVE = data_status == 'LIVE'

    def notify_order(self, order):
        trace(f"STRATEGY.notify_order | status={order.getstatusname()} | ref={order.ref}")
        if order.status == bt.Order.Completed:
            # Сообщаем об исполнении ордера на вход в позицию
            direction = 'покупку' if order.isbuy() else 'продажу'
            trace(f'STRATEGY.notify_order | Ордер на {direction} '
                  f'{abs(order.executed.size)} бумаг '
                  f'{order.data._dataname} выполнен по цене '
                  f'{order.executed.price} за бумагу. Новая позиция '
                  f'по инструменту: {self.getposition().size}')

    def notify_trade(self, trade):
        trace(f"STRATEGY.notify_trade | pnl={trade.pnl:.2f} | "
              f"Just Opened = {trade.justopened}, Is Closed = {trade.isclosed}")

    def notify_cashvalue(self, cash, value):
        trace(f"STRATEGY.notify_cashvalue | cash = {cash:.2f} | "
              f"position = {self.getposition().size}")

    def next(self):
        if not IS_LIVE: # выходим, если идет чтение истории
            return

        dt = bt.num2date(self.data.datetime[0])
        trace(f'STRATEGY.next | bar start time = {dt:%H:%M} | '
              f'bar OHLCV: {self.data.open[0]}, {self.data.high[0]}, '
              f'{self.data.low[0]}, {self.data.close[0]}, '
              f'{self.data.volume[0]} | buysig = {self.buysig[0]}')
        super().next()  # здесь логика MA_CrossOver (buy/sell)


def main():
    # Создаем экземпляры cerebro и хранилища
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
    # cerebro = bt.Cerebro(stdstats=False)
    store = QKStore()

    # dataname = 'QJSIM.LKOH'
    dataname = 'QJSIM.SBER'
    # dataname = 'QJSIM.AFLT'
    # dataname = 'QJSIM.ROSN'
    # dataname = 'QJSIM.T'
    # dataname = 'SBER'
    # dataname = 'EQRP_INFO.SBER'
    # dataname = 'CETS.SBER'
    # dataname = 'SPBFUT.MMM5'
    # dataname = 'QJSIM.MХM5'
    # dataname = 'SPBFUT.SiM5'
    # dataname = 'SPBFUT.RIM6'
    # dataname = 'CETS.KZTRUB_TOM'


    broker = store.getbroker() # экземпляр брокера берем из хранилища
    cerebro.setbroker(broker)  # привязываем его к cerebro
    # Проверяем запрошенный источник данных на его наличие в QUIK Junior
    broker.check_data_names(dataname)

    fromdate = datetime.today().date() # с какой даты берем данные
    # Будем работать на тайм-фрейме 1 минута
    data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes,
                         compression=1, fromdate=fromdate, live_bars=True)

    # Добавляем в cerebro источник данных, сайзер, стратегию и
    # запускаем движок
    cerebro.adddata(data)
    # cerebro.addsizer(CustomSizer)
    # cerebro.addstrategy(VerySimpleJuniorStrat)
    cerebro.addstrategy(TraceStrat)
    cerebro.addanalyzer(TraceTradeAnalyzer, _name="tta")
    cerebro.run()


if __name__ == '__main__':
    main()