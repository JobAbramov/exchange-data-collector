from abc import ABC, abstractmethod
from datetime import datetime
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS 
from binance.client import Client
from binance.enums import KLINE_INTERVAL_1MINUTE
from sys import exit

class Exchange(ABC):
    @abstractmethod
    def __init__(self, db_url, db_token, db_org, db_bucket):
        self._db_url = db_url
        self._db_token = db_token
        self._db_org = db_org
        self._db_bucket = db_bucket

    @abstractmethod
    def fetch(self, symbol, interval, limit, time_start, kline_type, time_end):
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

        #костыль. Нормальное решение оставлю до лучших времён. Главное, что так работает
        time_start_str = str(datetime.fromtimestamp(time_start)) if type(time_start) == int else time_start
        time_end_str = str(datetime.fromtimestamp(time_end)) if type(time_end) == int else time_end

        try:
            fetch = Client(api_key=self.__api_key, api_secret=self.__api_secret) \
            .get_historical_klines(symbol=symbol, interval=interval, limit=limit, start_str= time_start_str, end_str = time_end_str, klines_type=kline_type)
        except Exception as e:
            print('Exception occured!', e)
            exit()

        for item in fetch:
            data.append({"name": "quote", "symbol": symbol, "open_time": item[0], "open_price": float(item[1]), "high_price": float(item[2]), "low_price": float(item[3]), "close_price": float(item[4]), "volume": float(item[5])})

        return data

    def save(self, data):
        with InfluxDBClient(url = self._db_url, token= self._db_token, org= self._db_org) as db:
            w_api = db.write_api(write_options=SYNCHRONOUS)

            for item in data:
                point = Point.from_dict(item,
                        write_precision=WritePrecision.MS,
                        record_measurement_key="name",
                        record_time_key="open_time",
                        record_tag_keys = 
                            ["symbol"],
                        record_field_keys=
                            ["open_price",
                             "high_price",
                             "low_price", 
                             "close_price", 
                             "volume"])

                print('Writing', point)

                w_api.write(bucket = self._db_bucket, org = self._db_org, record = point)
                                   
            
    def update(self, symbol, kline_type, time_end=None, limit=500, interval = KLINE_INTERVAL_1MINUTE):
        with InfluxDBClient(url = self._db_url, token= self._db_token, org= self._db_org) as db:
            q_api = db.query_api()

            response = q_api.query('''from(bucket: "main")
                                    |> range(start: -7d)
                                    |> filter(fn: (r) => r["_measurement"] == "quote")
                                    |> filter(fn: (r) => r["_field"] == "open_price")
                                    |> last()
                                ''')

            time = int(response.to_values(columns=["_time"])[0][0].timestamp())
        data = self.fetch(symbol = symbol, time_start = time, kline_type = kline_type, time_end = time_end, limit = limit, interval= interval)

        self.save(data)

            