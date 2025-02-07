
import pandas as pd
import os , sys
pwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(pwd)
from config import CONFIG


def get_tickers():
    try:
        file_path = r"/app/data/all_instruments.parquet"
        df = pd.read_parquet(file_path)
    except :
        file_path = f"{CONFIG.DATA_FOLDER}/all_instruments.parquet"
        df = pd.read_parquet(file_path)
    # yfinance data ticker
    nse_code = df['nse_code'].to_list()
    nse_code = list(filter(lambda x: x != '', nse_code))
    nse_code = [ns_ticker + '.NS' for  ns_ticker in  nse_code]
    bse_code = df['bse_code'].to_list()
    bse_code = list(filter(lambda x: x != '0', bse_code))
    bse_code = [bo_ticker + '.BO' for  bo_ticker in  bse_code]
    tickers_without_ns_bo = nse_code + bse_code
    ticker_with_ns_bo = [stock_name.replace('.NS', '').replace('.BO', '') for stock_name in tickers_without_ns_bo]
    return tickers_without_ns_bo,ticker_with_ns_bo