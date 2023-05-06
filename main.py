from collectors import Binance, enums, klines_type
from datetime import datetime
from dotenv import get_key
from collectors.db import Influx

URL = get_key('.env', 'URL')
TOKEN = get_key('.env', 'TOKEN')
ORG = get_key('.env', 'ORG')
BUCKET = get_key('.env', 'BUCKET')
API_KEY = get_key('.env', 'API_KEY')
API_SECRET = get_key('.env', 'API_SECRET')


with Influx(db_url= URL, db_token = TOKEN, db_org = ORG, db_bucket = BUCKET) as db, Binance(API_KEY, API_SECRET) as bn:

    res = bn.fetch(symbol='BTCUSDT', time_start= '2023-05-06')

    db.insert(res, measurement="symbol", fields=["open", "high", "low", "close", "volume"], time = "time")
    print(datetime.fromtimestamp(db.get_last_date("BTCUSDT")))

    #update db
    last_date = db.get_last_date("BTCUSDT")
    res = bn.fetch(symbol='BTCUSDT', time_start = last_date)
    db.insert(res, measurement="symbol", fields=["open", "high", "low", "close", "volume"], time = "time")