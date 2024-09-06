import os
import config
from datetime import date, datetime, timedelta
from alpaca_trade_api.rest import REST,TimeFrame

os.environ['APCA_API_KEY_ID'] = config.API_KEY
os.environ['APCA_API_SECRET_KEY'] = config.SECRET_KEY

api = REST()
import config
import psycopg2
import psycopg2.extras

connection = psycopg2.connect(host=config.DB_HOST,database=config.DB_NAME,user=config.DB_USER,password=config.DB_PASS)
cursor = connection.cursor(cursor_factory=psycopg2.extras.DictCursor)
postgreSQL_select_Query = "SELECT symbol from assets WHERE status = 'active' AND class not in ('crypto') order by symbol"

start = (date.today()-timedelta(days=0)).isoformat()
end = (date.today()+timedelta(days=1)).isoformat()

def process_bar(bar):
    cursor.execute("""
    INSERT INTO bars (symbol,t,o,h,l,c,v,trade_count) VALUES (%s,%s,%s,%s,%s,%s,%s,%s) ON CONFLICT (symbol,t) DO NOTHING
    """,(asset[0],bar.t,bar.o,bar.h,bar.l,bar.c,bar.v,bar.n)
    )

cursor.execute(postgreSQL_select_Query)
print("Selecting rows from assets table using cursor.fetchall")
assets = cursor.fetchall()

for asset in assets:
    print(asset[0])
    bar_iter = api.get_bars_iter(asset[0], TimeFrame.Minute, start, end)
    for bar in bar_iter:
        process_bar(bar)
    connection.commit()
