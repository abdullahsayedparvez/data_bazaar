import os
import pandas as pd
import time
import sys
import pyarrow
import fastparquet
from concurrent.futures import ThreadPoolExecutor
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
from datetime import datetime
from io import BytesIO
pwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(pwd)
from functions.list_of_data import get_tickers
from functions.statements_data_fucntions import get_modified_dates,screener_excel_data

stock_name_list,stock_name_list_with_ns_bo = get_tickers(file_path=r"D:\projects\docker_images\data_bazaar\data\all_instruments.parquet")

screener_excel_data("https://www.screener.in/login/?",stock_name_list_with_ns_bo,stock_name_list)