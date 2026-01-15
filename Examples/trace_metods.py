from __future__ import (absolute_import, division, print_function, unicode_literals)
from datetime import datetime, timedelta
import itertools
import backtrader as bt
import backtrader.indicators as btind
from BacktraderQuikJunior.QJStore import QKStore
from BacktraderQuikJunior.logger_config import logger, set_file_logging



class CustomSizer(bt.Sizer):
    '''
    Это класс пользовательского Сайзера - здесь определяется логика
    вычисления размера позиции при входе, неважно в лонг или шорт.
    Метод _getsizing возвращает size (количество бумаг/контрактов
    для покупки или продажи) любому выставленному Ордеру, если в
    самом Ордере аргумент size не будет задан в явном виде.
    '''
    def _getsizing(self, comminfo, cash, data, isbuy):
        # По умолчанию cash содержит сумму балансов всех доступных
        # счетов. Пробуем найти счет, на котором торгуется инструмент
        # data и получить в cash его баланс. Для этого определяем
        # режим торгов и код инструмента из имени источника данных data
        class_code, sec_code = data._dataname.split('.')
        for acc in self.broker.accounts:
            # По режиму торгов находим id счета
            if class_code in acc['class_codes']:
                acc_id = acc['account_id']  # нашли - хорошо
                break
        else:  # не нашли - плохо
            print(f'Sizer не смог определить счет, на котором торгуется '
                  f'инструмент {data._dataname}. Для расчета позиции '
                  f'взята общая сумма ДС на всех доступных в QUIK счетах.')
            acc_id = None

        # Если нашли счет, на котором торгуется инструмент,
        # получаем его баланс в cash
        if acc_id is not None:
            cash = self.broker.getcash(account_id=acc_id)

        # Данные, которые могут понадобиться для расчета size,
        # кроме cash, а могут и не понадобиться, это как пойдет...
        if data.derivative: # это если работаем с фьючерсами
            # Вот шаг цены фьючерса
            price_step = self.broker.get_price_step(class_code, sec_code)
            # Вот стоимость шага цены фьючерса
            cost_of_price_step = self.broker.get_cost_of_price_step(class_code, sec_code)
            # ГО покупателя
            bayer_go = self.broker.get_bayer_go(class_code, sec_code)
            # ГО продавца
            seller_go = self.broker.get_seller_go(class_code, sec_code)
            logger.info(f'{price_step = }, {cost_of_price_step = }, '
                        f'{bayer_go = }, {seller_go = }')
        else:
            # Для акций size должен быть кратен лоту, вот размер лота
            lot_size = self.broker.store.provider.symbols[(class_code, sec_code)]['lot_size']

        # Ниже пользовательская логика определения size
        size = 2  # вот например простая логика
        return size

_STEP = itertools.count(1)
IS_LIVE = False

def trace(msg: str) -> None:
    # flush=True чтобы порядок не “перемешивался” буферизацией
    # if not IS_LIVE:
    #     return
    print(f"{next(_STEP):04d} | {msg}", flush=True)


class TraceSMA(btind.SMA):
    """
        Эти методы вызываются из LineIterator._next():
      - индикаторы дергаются раньше стратегии: lineiterator.py:262–263
      - выбор next/nextstart/prenext для индикатора: lineiterator.py:279–284
    """
    def prenext(self):
        dt = self.data.datetime.datetime(0)
        trace(f"IND(SMA).prenext  period={self.p.period} dt={dt}  (lineiterator.py:284)")

    def nextstart(self):
        dt = self.data.datetime.datetime(0)
        trace(f"IND(SMA).nextstart period={self.p.period} dt={dt} sma={self[0]:.6f}  (lineiterator.py:282)")

    def next(self):
        dt = self.data.datetime.datetime(0)
        trace(f"IND(SMA).next     period={self.p.period} dt={dt} sma={self[0]:.6f}  (lineiterator.py:280)")


class TraceTradeAnalyzer(bt.analyzers.TradeAnalyzer):
    """
    Безопасный анализатор для live: не требует broker.getvalue().
    Нужен для демонстрации порядка вызовов:
    - Analyzer.next() идёт после Strategy.next()
    - notify_* анализатора вызывается из Strategy._notify()
    """

    def start(self):
        trace("ANALYZER.start (TradeAnalyzer)")

    def prenext(self):
        dt = self.strategy.data.datetime.datetime(0)
        trace(f"ANALYZER.prenext dt={dt}  (strategy._next_analyzers -> analyzer._prenext)")

    def nextstart(self):
        dt = self.strategy.data.datetime.datetime(0)
        trace(f"ANALYZER.nextstart dt={dt}  (strategy._next_analyzers -> analyzer._nextstart)")

    def next(self):
        dt = self.strategy.data.datetime.datetime(0)
        trace(f"ANALYZER.next dt={dt}  (analyzer.py:188 -> Analyzer.next())")

        # пусть базовый TradeAnalyzer продолжает собирать статистику
        return super().next()

    def stop(self):
        trace("ANALYZER.stop (TradeAnalyzer)")

    # ----- notify family (анализатор) -----
    # Будут вызываться только если есть исполнения (ордера/трейды)

    def notify_order(self, order):
        trace(f"ANALYZER.notify_order status={order.getstatusname()} ref={order.ref}  (strategy.py:593)")

    def notify_trade(self, trade):
        trace(f"ANALYZER.notify_trade pnl={trade.pnl:.2f} pnlcomm={trade.pnlcomm:.2f}  (strategy.py:599)")


class TraceStrat(bt.strategies.MA_CrossOver):
    params = (
        # period for the fast&slow Moving Average
        ('fast', 10),
        ('slow', 20),
        ('_movav', TraceSMA)  # moving average to use
    )

    def __init__(self):
        # Статус полученного бара: False - исторический, True - живой
        self.is_live = False
        # Логируем в файл стартовый cash
        logger.info(f'Стартовый CASH = {self.broker.getcash()}')
        trace("STRATEGY.__init__")
        super().__init__()  # создаст 2 SMA и CrossOver


    def start(self):
        trace("STRATEGY.start")

    def stop(self):
        trace("STRATEGY.stop")

    # ----- notify family (стратегия) -----

    def notify_store(self, msg, *args, **kwargs):
        trace(f"STRATEGY.notify_store msg={msg}")

    def notify_data(self, data, status, *args, **kwargs):
        global IS_LIVE
        trace(f"STRATEGY.notify_data status={data._getstatusname(status)}")
        # Изменение статуса приходящих баров
        data_status = data._getstatusname(status)
        logger.info(f'Источник данных: {data.p.dataname}, статус: {data_status}')
        # self.is_live = data_status == 'LIVE'
        IS_LIVE = data_status == 'LIVE'

    def notify_order(self, order):
        trace(f"STRATEGY.notify_order status={order.getstatusname()} ref={order.ref}  (strategy.py:590)")
        if order.status == bt.Order.Completed:
            # Сообщаем об исполнении ордера на вход в позицию
            direction = 'покупку' if order.isbuy() else 'продажу'
            logger.info(f'Ордер на {direction} {abs(order.executed.size)} бумаг '
                        f'{order.data._dataname} выполнен по цене '
                        f'{order.executed.price} за бумагу. Новая позиция '
                        f'по инструменту: {self.getposition().size}')

    def notify_trade(self, trade):
        trace(f"STRATEGY.notify_trade pnl={trade.pnl:.2f} pnlcomm={trade.pnlcomm:.2f}  (strategy.py:596)")

    def notify_cashvalue(self, cash, value):
        trace(f"STRATEGY.notify_cashvalue cash={cash:.2f} value={value:.2f}  (strategy.py:609)")

    # ----- next family (стратегия) -----

    def prenext(self):
        dt = self.data.datetime.datetime(0)
        trace(f"STRATEGY.prenext dt={dt}  (lineiterator.py:275)")

    def nextstart(self):
        dt = self.data.datetime.datetime(0)
        trace(f"STRATEGY.nextstart dt={dt}  (lineiterator.py:273)")
        super().next()

    def next(self):
        # if not self.is_live: # выходим, если идет чтение истории
        if not IS_LIVE: # выходим, если идет чтение истории
            return

        trace(f'STRATEGY.next '
            f'D-T-O-H-L-C-V: {bt.num2date(self.data.datetime[0])}, '
            f'{self.data.open[0]}, {self.data.high[0]}, {self.data.low[0]}, '
            f'{self.data.close[0]}, {self.data.volume[0]}; buysig = {self.buysig[0]}')
        super().next()  # это и есть логика MA_CrossOver (buy/sell)


def main():
    # Переключатель записи лога в файл:
    # True - пишем, False не пишем
    # Если True - лог пишем в файл app.log в папку Logs
    set_file_logging(False)

    # Создаем экземпляры cerebro и хранилища
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
    store = QKStore()

    # logger.debug(f'Проводим инвентаризацию счетов учетной записи, '
    #              f'смотрим остатки денег (money_limits) и активов '
    #              f'(depo_limits) в QUIK Junior.')
    # trade_accounts = store.provider.get_trade_accounts()['data']  # Все торговые счета
    # money_limits = store.provider.get_money_limits()['data']  # Все денежные лимиты (остатки на счетах)
    # depo_limits = store.provider.get_all_depo_limits()['data']  # Все лимиты по бумагам (позиции по инструментам)
    #
    # for n, ta in enumerate(trade_accounts):
    #     logger.debug(f'Торговый счет {n}: \n {ta}')
    # for n, ml in enumerate(money_limits):
    #     logger.debug(f'Денежный лимит {n}: \n {ml}')
    # for n, dl in enumerate(depo_limits):
    #     logger.debug(f'Депо лимит {n}: \n {dl}')
    # # Закончили инвентаризацию

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

    fromdate = datetime.today() - timedelta(minutes=30) # с какой даты берем данные
    print(fromdate)
    # Будем работать на тайм-фрейме 1 минута
    data = store.getdata(dataname=dataname, timeframe=bt.TimeFrame.Minutes,
                         compression=1, fromdate=fromdate, live_bars=True)

    # Добавляем в cerebro источник данных, сайзер, стратегию и
    # запускаем движок
    cerebro.adddata(data)
    cerebro.addsizer(CustomSizer)
    # cerebro.addstrategy(VerySimpleJuniorStrat)
    cerebro.addstrategy(TraceStrat)
    cerebro.addanalyzer(TraceTradeAnalyzer, _name="tta")
    cerebro.run()


if __name__ == '__main__':
    main()