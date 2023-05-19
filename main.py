from collectors import enums, klines_type
from collectors.clients import Binance
from datetime import datetime
from dotenv import get_key
from collectors.db import Influx
import sys

URL = get_key('.env', 'URL')
TOKEN = get_key('.env', 'TOKEN')
ORG = get_key('.env', 'ORG')
BUCKET = get_key('.env', 'BUCKET')
API_KEY = get_key('.env', 'API_KEY')
API_SECRET = get_key('.env', 'API_SECRET')


with Influx(db_url= URL, db_token = TOKEN, db_org = ORG, db_bucket = BUCKET) as db, Binance(API_KEY, API_SECRET) as bn:

    try:
        symbol = sys.argv[1]
        d_start = datetime.fromisoformat(sys.argv[2]) if len(sys.argv) >= 3 else None
        d_end = datetime.fromisoformat(sys.argv[3]) if len(sys.argv) >= 4 else None
    except (TypeError, ValueError) as e:
        print('Incorrect data:', e)
        sys.exit(1)

    print('fetching data')
    try:
        res = bn.fetch(symbol, time_start=d_start, time_end=d_end, limit=1000)
        db.insert(res, measurement="symbol", fields=["open", "high", "low", "close", "volume"], time = "time")
    except Exception as e:
        print(f'Error occured: {e}. {e.args}')
    
    print('Data is up to date')