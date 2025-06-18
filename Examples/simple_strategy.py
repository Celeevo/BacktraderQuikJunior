from __future__ import (absolute_import, division, print_function, unicode_literals)
from datetime import datetime
import backtrader as bt
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
        else:
            # Для акций size должен быть кратен лоту, вот размер лота
            lot_size = self.broker.store.provider.symbols[(class_code, sec_code)]['lot_size']

        # Ниже пользовательская логика определения size
        size = 30  # вот например простая логика
        return size


class VerySimpleJuniorStrat(bt.Strategy):
    '''
    Вход в лонг: если пришло 2 бычьих свечи подряд -
    на третьей входим в лонг рыночным ордером.
    Вход в шорт: если пришло 2 медвежьих свечи подряд -
    на третьей входим в шорт рыночным ордером.
    Выход из позиции: ждем 2 бара и выходим рыночным
    ордером (self.close()).
    '''
    def __init__(self):
        # Статус полученного бара: False - исторический, True - живой
        self.is_live = False
        self.entry_bar = 0  # Это бар входа в позицию
        # Логируем в файл стартовый cash и value
        logger.debug(f'Стартовый CASH = {self.broker.getcash()}')
        logger.debug(f'Стартовое VALUE = {self.broker.getvalue()}')

    def next(self):
        if not self.is_live: # выходим, если идет чтение истории
            return

        logger.info(
            f'Обработка живого бара в next(). '
            f'D-T-O-H-L-C-V: {bt.num2date(self.data.datetime[0])}, '
            f'{self.data.open[0]}, {self.data.high[0]}, {self.data.low[0]}, '
            f'{self.data.close[0]}, {self.data.volume[0]}')

        logger.debug(f'Работаем с источником данных: {self.data._dataname}, '
                    f'текущая позиция: {self.getposition(self.data).size}')

        if not self.position.size:
            if self.data.close[0] <= self.data.open[0] and self.data.close[-1] <= self.data.open[-1]:
                logger.info(f'Сигнал в Шорт!')
                self.sell() # рыночный ордер в шорт
            elif self.data.close[0] >= self.data.open[0] and self.data.close[-1] >= self.data.open[-1]:
                logger.info(f'Сигнал в Лонг!')
                self.buy()
            self.entry_bar = len(self)
        elif len(self) >= self.entry_bar + 2:  # Пора выходить, уже 2 бара сидим...
            logger.info(f'Сигнал на выход! Позиция-size: {self.getposition().size}, '
                        f'Позиция-price: {self.getposition().price}, '
                        f'бар входа: {self.entry_bar}, текущ. бар: {len(self)}')
            self.close() # Выход рыночным ордером

    def notify_data(self, data, status, *args, **kwargs):
        # Изменение статуса приходящих баров
        data_status = data._getstatusname(status)
        logger.info(f'Источник данных: {data.p.dataname}, статус: {data_status}')
        self.is_live = data_status == 'LIVE'

    def notify_order(self, order):
        if order.status == bt.Order.Completed:
            # Сообщаем об исполнении ордера на вход в позицию
            direction = 'покупку' if order.isbuy() else 'продажу'
            logger.info(f'Ордер на {direction} {abs(order.executed.size)} бумаг '
                        f'{order.data._dataname} выполнен по цене '
                        f'{order.executed.price} за бумагу. Новая позиция '
                        f'по инструменту: {self.getposition().size}')


    def stop(self):
        super(VerySimpleJuniorStrat, self).stop()


def main():
    # Переключатель записи лога в файл:
    # True - пишем, False не пишем
    # Если True - лог пишем в файл app.log в папку Logs
    set_file_logging(True)

    # Создаем экземпляры cerebro и хранилища
    cerebro = bt.Cerebro(stdstats=False, quicknotify=True)
    store = QKStore()

    logger.debug(f'Проводим инвентаризацию счетов учетной записи, '
                 f'смотрим остатки денег (money_limits) и активов '
                 f'(depo_limits) в QUIK Junior.')
    trade_accounts = store.provider.get_trade_accounts()['data']  # Все торговые счета
    money_limits = store.provider.get_money_limits()['data']  # Все денежные лимиты (остатки на счетах)
    depo_limits = store.provider.get_all_depo_limits()['data']  # Все лимиты по бумагам (позиции по инструментам)

    for n, ta in enumerate(trade_accounts):
        logger.debug(f'Торговый счет {n}: \n {ta}')
    for n, ml in enumerate(money_limits):
        logger.debug(f'Денежный лимит {n}: \n {ml}')
    for n, dl in enumerate(depo_limits):
        logger.debug(f'Депо лимит {n}: \n {dl}')
    # Закончили инвентаризацию

    # dataname = 'QJSIM.LKOH'
    # dataname = 'QJSIM.SBER'
    # dataname = 'QJSIM.AFLT'
    dataname = 'QJSIM.YDEX'
    # dataname = 'QJSIM.T'
    # dataname = 'SBER'
    # dataname = 'EQRP_INFO.SBER'
    # dataname = 'CETS.SBER'
    # dataname = 'SPBFUT.MMM5'
    # dataname = 'QJSIM.MMM5'
    # dataname = 'SPBFUT.SiM5'
    # dataname = ('CETS.KZTRUB_TOM')


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
    cerebro.addsizer(CustomSizer)
    cerebro.addstrategy(VerySimpleJuniorStrat)
    cerebro.run()


if __name__ == '__main__':
    main()