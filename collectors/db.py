from abc import ABC, abstractmethod
from influxdb_client import InfluxDBClient, Point, WritePrecision
from influxdb_client.client.write_api import SYNCHRONOUS


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


class Influx(DB):
    def __init__(self, db_url, db_token, db_org, db_bucket):
        self._db_url = db_url
        self._db_token = db_token
        self._db_org = db_org
        self._db_bucket = db_bucket
        
    def insert(self, data, **kwargs):
        '''Запрос вставки'''
        with InfluxDBClient(url = self._db_url, token= self._db_token, org= self._db_org) as db:
            w_api = db.write_api(write_options=SYNCHRONOUS)
            
            for item in data:
                point = self._dict_to_point(item[1], measurement = str(kwargs.get("measurement")), tags = list(kwargs.get("tags", "tag")), fields = list(kwargs.get("fields")), time = item[0])
                print('Writing', point)

                w_api.write(bucket = self._db_bucket, org = self._db_org, record = point)

    def select(self, body, to_json, columns, *args):
        '''Запрос выборки. args - measure, tags, fields
            Порядок определяется по порядку упоминания в теле запроса
            columns - Ключи, по которым будут извлекаться значения из выборки
            body = from(bucket: "{}")
                |> range(start: -7d)
                |> filter(fn: (r) => r["_measurement"] == "{}")
                ... 
                |> filter(fn: (r) => r["_field"] == {})
                ... 
                ... 
                |> filter(fn: (r) => r["{}"] == {})
                ...
                          
        '''
        with InfluxDBClient(url = self._db_url, token= self._db_token, org= self._db_org) as db:
            q_api = db.query_api()
            query = body.format(*args)                              
            response = q_api.query(query)

        return response.to_json() if to_json else response.to_values(columns=columns)


    def get_last_date(self, measurement):
        time = self.select('''from(bucket: "{}")
                                    |> range(start: -7d)
                                    |> filter(fn: (r) => r["_measurement"] == "{}")
                                    |> last()
                                ''', False, ["_time"], self._db_bucket, measurement)

        return int(time[0][0].timestamp())

    def _dict_to_point(self, dict, measurement, fields, tags = None, write_precision = WritePrecision.MS, time = None):
        return Point.from_dict(
                                dict, 
                                write_precision, 
                                record_measurement_key=measurement,
                                record_tag_keys=tags,
                                record_field_keys=fields
                              ).time(time)