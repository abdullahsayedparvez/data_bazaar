# "C:\Users\abdul\ansaar-pipeline\lake_data\raw\screener_in\all_instruments.parquet"
import pandas as pd

# Path to the Parquet file
# file_path = r"D:/projects/docker_images/data_bazaar/data/all_instruments.parquet"  give this path or your respected path
def get_tickers(file_path = None):
    if file_path == None:
        file_path = r"/app/data/all_instruments.parquet"
    # yfinance data ticker
    df = pd.read_parquet(file_path)
    nse_code = df['nse_code'].to_list()
    nse_code = list(filter(lambda x: x != '', nse_code))
    nse_code = [ns_ticker + '.NS' for  ns_ticker in  nse_code]
    bse_code = df['bse_code'].to_list()
    bse_code = list(filter(lambda x: x != '0', bse_code))
    bse_code = [bo_ticker + '.BO' for  bo_ticker in  bse_code]
    tickers_without_ns_bo = nse_code + bse_code
    ticker_with_ns_bo = [stock_name.replace('.NS', '').replace('.BO', '') for stock_name in tickers_without_ns_bo]
    return tickers_without_ns_bo,ticker_with_ns_bo



