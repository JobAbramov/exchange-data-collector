from abc import ABC, abstractmethod
from datetime import datetime
from influxdb_client import InfluxDBClient, Point
from influxdb_client.client.write_api import SYNCHRONOUS 
from binance.client import Client
from binance.enums import KLINE_INTERVAL_1MINUTE
from binance.helpers import date_to_milliseconds
from sys import exit

class Exchange(ABC):
    @abstractmethod
    def __init__(self, db_url, db_token, db_org, db_bucket):
        self._db_url = db_url
        self._db_token = db_token
        self._db_org = db_org
        self._db_bucket = db_bucket

    @abstractmethod
    def fetch(self, symbol, interval, limit, time_start, kline_type, time_end=datetime.now()):
        pass

    @abstractmethod
    def save(self, data):
        pass

    @abstractmethod
    def update(self, data):
        pass

class Binance(Exchange):
    def __init__(self, db_url, db_token, db_org, db_bucket, api_key, api_secret):
        super().__init__(db_url, db_token, db_org, db_bucket)
        self.__api_key = api_key
        self.__api_secret = api_secret

    
    def fetch(self, symbol, time_start, kline_type, limit=500, time_end=None, interval = KLINE_INTERVAL_1MINUTE):
        fetch = []
        data = []

        try:
            fetch = Client(api_key=self.__api_key, api_secret=self.__api_secret).get_historical_klines(symbol=symbol, interval=interval, limit=limit, start_str= time_start, end_str=time_end, klines_type=kline_type)
        except Exception as e:
            print('Exception occured!', e)
            exit()

        for item in fetch:
            data.append({"open_time": item[0], "open_price": item[1], "high_price": item[2], "low_price": item[3], "close_price": item[4], "volume": item[5]})
            
        return data

    def save(self, data):
        pass

    def update(self,data):
        pass
        
