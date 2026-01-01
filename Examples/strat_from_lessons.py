import backtrader as bt
import datetime

# Получаем исторические данные из csv файла.
# Добавляем индикатор SMA
# Стратегия:
#       только покупка, если цена закрытия > SMA
#       продажа, если цена закрытия < SMA.
# Установка стартового капитала = 200_000 руб.
# Учитываем комиссию 0,1%.
# Размер сделки = 200 акций.
# Оптимизируем параметр период SMA (значения от 6 до 15)
# по MAX финальному капиталу.

# Создаем Стратегию
class TestStrategy(bt.Strategy):
    params = (
        ('maperiod', 15),
    )

    def log(self, txt, dt=None, doprint=False):
        # Функция логирования событий Стратегии
        if doprint:
            dt = dt or self.data.datetime.date(0)
            print(f'{dt.isoformat()}, {txt}')

    def __init__(self):
        # Для отслеживания размещенных Ордеров
        self.order = None
        # Добавляем скользящую среднюю
        self.sma = bt.indicators.SMA(self.datas[0], period=self.params.maperiod)

    def notify_order(self, order):
        if order.status in [order.Submitted, order.Accepted]:
            # Ордер отправлен Брокеру или подтвержден Брокером - ничего не делаем
            return

        # Проверяем, был ли Ордер исполнен
        # Брокер может отвергнуть Ордер, если для его исполнения недостаточно денег
        if order.status in [order.Completed]:
            side = 'ПОКУПКА' if order.isbuy() else 'ПРОДАЖА'

            self.log(f'{side} ИСПОЛНЕНА, цена: {order.executed.price}, '
                     f'комиссия {order.executed.comm:.5f}')

        elif order.status in [order.Canceled, order.Margin, order.Rejected]:
            self.log('Ордер отвергнут Брокером')

        # Нет отслеживаемых Ордеров
        self.order = None

    def notify_trade(self, trade):
        # Если сделка еще не закрыта - выходим
        if not trade.isclosed:
            return

        self.log(f'Операционная ПРИБЫЛЬ, до начисления комиссии: '
                 f'{trade.pnl:.2f}, после комиссии: {trade.pnlcomm:.2f}')

    def next(self):
        # Просто выводим цену закрытия каждого дня
        self.log(f'Close: {self.data.close[0]}')

        # Проверяем, есть ли размещенные Ордера, если есть -
        # мы не можем разместить еще
        if self.order:
            return

        # Проверяем, есть ли у нас позиция на рынке
        if not self.position:

            # Еще нет, тогда если
            # текущее значение close больше текущего значения SMA
            if self.data.close[0] > self.sma[0]:
                self.log(f'Покупаем! {self.data.close[0]}')
                # Будем отслеживать созданный Ордер,
                # чтобы не создавать еще
                self.order = self.buy()
        else:
            # Уже в позиции, можем продавать, если
            if self.data.close[0] < self.sma[0]:
                self.log(f'Продаем! {self.data.close[0]}')
                # Будем отслеживать созданный Ордер,
                # чтобы не создавать еще
                self.order = self.sell()

    def stop(self):
        self.log(f'MA Period = {self.params.maperiod}, '
                 f'Финальный капитал = {self.broker.getvalue():.2f}',
                 doprint=True)

if __name__ == '__main__':
    cerebro = bt.Cerebro()
    # установим свой стартовый капитал
    cerebro.broker.setcash(200000.0)

    data = bt.feeds.GenericCSVData(
        dataname='SBER_010123_311224.csv',
        fromdate=datetime.datetime(2023, 1, 1),
        todate=datetime.datetime(2024, 12, 31),
        dtformat='%d/%m/%y',
        tmformat='%H:%M',
        datetime=0, time=1, open=2, high=3, low=4, close=5, volume=6,
        openinterest=-1
    )

    # добавляем источник данных в движок
    cerebro.adddata(data)

    strats = cerebro.optstrategy(
        TestStrategy,
        maperiod=range(3, 16))

    # разделите 0,1% на 100, чтобы убрать %
    cerebro.broker.setcommission(commission=0.001)
    cerebro.addsizer(bt.sizers.FixedSize, stake=200)

    cerebro.run()