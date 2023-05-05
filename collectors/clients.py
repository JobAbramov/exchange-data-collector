from abc import ABC, abstractmethod
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS 
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
    

class Binance(Exchange):
    def __init__(self, api_key, api_secret):
        self.__api_key = api_key
        self.__api_secret = api_secret

    
    def fetch(self, symbol, time_start, time_end = None, interval = KLINE_INTERVAL_1MINUTE, kline_type = kline_type.SPOT, limit = 500):
        fetch = []
        data = []

        # Костыль. Нормальное решение оставлю до лучших времён. Главное, что так работает
        time_start_str = str(datetime.fromtimestamp(time_start)) if type(time_start) == int else time_start
        time_end_str = str(datetime.fromtimestamp(time_end)) if type(time_end) == int else time_end

        try:
            # get_historical_klines циклично достаёт данные с бинанса, пока не дойдёт до конечной даты (по умолчанию это текущая дата),
            # либо пока данные не исчерпаются
            # limit - кол-во чанков за проход по циклу
            fetch = Client(api_key=self.__api_key, api_secret=self.__api_secret) \
            .get_historical_klines(symbol=symbol, interval=interval, limit=limit, start_str= time_start_str, end_str = time_end_str, klines_type=kline_type)
        except Exception as e:
            print('Exception occured!', e)
            exit()

        for item in fetch:
            data.append([item[0], {"symbol": symbol, "open": float(item[1]), "high": float(item[2]), "low": float(item[3]), "close": float(item[4]), "volume": float(item[5])}])

        return data
                               
            