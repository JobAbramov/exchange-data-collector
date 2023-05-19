from abc import ABC, abstractmethod
from datetime import datetime
from binance.client import Client
from binance.enums import KLINE_INTERVAL_1MINUTE, HistoricalKlinesType as kline_type
from sys import exit


class Exchange(ABC):
    @abstractmethod
    def __init__(self, db_url, db_token, db_org, db_bucket):
        pass

    @abstractmethod
    def fetch(self, symbol, interval, limit, time_start, kline_type, time_end):
        pass

    @abstractmethod
    def close(self):
        pass
    

class Binance(Exchange):
    def __init__(self, api_key, api_secret):
        self.__api_key = api_key
        self.__api_secret = api_secret
        self.__connection = Client(api_key=self.__api_key, api_secret=self.__api_secret)
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def fetch(self, symbol, time_start, time_end = None, interval = KLINE_INTERVAL_1MINUTE, kline_type = kline_type.SPOT, limit = 500):
        '''Получение исторической информации о цене и объёме с Binance API'''
        fetch = []
        data = []

        #datetime в строку
        time_start_str = str(time_start) if time_start else None
        time_end_str = str(time_end) if time_end else None

        try:
            # get_historical_klines циклично достаёт данные с бинанса, пока не дойдёт до конечной даты (по умолчанию это текущая дата),
            # либо пока данные не исчерпаются. Возвращает весь список данных в диапазоне дат
            # limit - кол-во чанков за проход по циклу
            fetch = self.__connection.get_historical_klines(symbol=symbol, interval=interval, limit=limit, start_str= time_start_str, end_str = time_end_str, klines_type=kline_type)
        except Exception as e:
            print('Exception occured!', e)
            exit()

        for item in fetch:
            data.append({"symbol": symbol, "time": item[0] ,"open": float(item[1]), "high": float(item[2]), "low": float(item[3]), "close": float(item[4]), "volume": float(item[5])})

        return data
    
    def close(self):
        self.__connection.close_connection()
                               
            