import mysql.connector as MyConn
import os
import sys
from datetime import date, timedelta
from yahoo_fin.stock_info import get_data
import yfinance as yf
pwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(pwd)
from functions.list_of_data import get_tickers
from functions.insert_data_in_sql_yfinance import insert_yfinance_data

ticker_without_ns_bo,tickers = get_tickers()
today = date.today()
from_day = today - timedelta(days=(365 * 28) + 1)
print('total_tickers -->',len(tickers))
tickers = tickers[0:25]

for index,ticker in enumerate(tickers,start=1):
    try:
        data = get_data(ticker, start_date=str(from_day), end_date=str(today))
        data = data.reset_index()
        data = data.rename(columns={'index':'date'})
        data_stored = insert_yfinance_data(ticker=ticker,data=data)
        print(f'{index} -->{data_stored}')
    except:
        print('ticker not found')