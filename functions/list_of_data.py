# "C:\Users\abdul\ansaar-pipeline\lake_data\raw\screener_in\all_instruments.parquet"
import pandas as pd

# Path to the Parquet file
def get_tickers():
    file_path = r"C:\Users\abdul\ansaar-pipeline\lake_data\raw\screener_in\all_instruments.parquet"
    df = pd.read_parquet(file_path)
    nse_code = df['nse_code'].to_list()
    nse_code = list(filter(lambda x: x != '', nse_code))
    nse_code = [ns_ticker + '.NS' for  ns_ticker in  nse_code]
    bse_code = df['bse_code'].to_list()
    bse_code = list(filter(lambda x: x != '0', bse_code))
    bse_code = [bo_ticker + '.BO' for  bo_ticker in  bse_code]
    tickers = nse_code + bse_code
    return tickers



