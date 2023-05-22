from abc import ABC, abstractmethod
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS
from datetime import datetime

class DB(ABC):
    @abstractmethod
    def __init__(self):
        pass

    @abstractmethod
    def insert(self, data, into):
        '''Запрос вставки. into - таблица, в которую заносятся данные'''
        pass

    @abstractmethod
    def select(self, body, *args):
        '''Запрос выборки. args - необходимые поля для выборки, фильтрации, группировки.
            Порядок определяется по порядку упоминания в теле запроса'''
        pass

    @abstractmethod
    def close(self):
        pass


class Influx(DB):
    def __init__(self, db_url, db_token, db_org, db_bucket):
        self._db_url = db_url
        self._db_token = db_token
        self._db_org = db_org
        self._db_bucket = db_bucket
        self.__connection = InfluxDBClient(url = self._db_url, token = self._db_token, org = self._db_org)

    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()
        
    def insert(self, data, **kwargs):
        '''Запрос вставки
            kwargs - measurement, tags, fields, time
        '''
        w_api = self.__connection.write_api(write_options=SYNCHRONOUS)
        
        for item in data:
            point = self._dict_to_point(item, measurement = str(kwargs.get("measurement")), tags = list(kwargs.get("tags", [])), fields = list(kwargs.get("fields")), time = kwargs.get("time"))
            print('Writing', point)
            w_api.write(bucket = self._db_bucket, org = self._db_org, record = point)

    def select(self, bucket, measurements, dt_start, dt_end = None, fields = None,to_json = False):
        '''Запрос выборки'''

        bucket_str = f'from(bucket: "{bucket}")'
        range_str = f'|> range(start:{dt_start}' + (f', stop: {dt_end}' if dt_end else '') + ')'
        measurements_str = '|> filter(fn: (r) => ' + (' or '.join([f'r["_measurement"] == "{measurement}"' for measurement in measurements])) + ')'
        fields_str ='|> filter(fn: (r) => ' + (' or '.join([f'r["_field"] == "{field}"' for field in fields])) + ')' if fields else ''
        query = '\n\t'.join([bucket_str, range_str, measurements_str, fields_str])

        q_api = self.__connection.query_api()                           
        response = q_api.query(query)

        return response.to_json() if to_json else response.to_values(columns=['_time', '_measurement', '_field', '_value'])


    def get_last_date(self, measurement, range_start = '-30d'):
        '''range_start - начальная дата, с которой надо начинать искать последнюю дату.
        Задаётся либо текстовой константой (например, -7d), либо через timestamp'''
        time = self.select('''from(bucket: "{}")
                                    |> range(start: {})
                                    |> filter(fn: (r) => r["_measurement"] == "{}")
                                    |> last()
                                ''', False, ["_time"], self._db_bucket, range_start, measurement)

        if len(time) > 0:
            return time[0][0]

    def _dict_to_point(self, dict, measurement, fields, tags = None, write_precision = WritePrecision.MS, time = None):
        return Point.from_dict(dict,
                        write_precision=WritePrecision.MS,
                        record_measurement_key = measurement,
                        record_time_key= time,
                        record_tag_keys = tags,
                        record_field_keys = fields)
    
    def close(self):
        self.__connection.close()