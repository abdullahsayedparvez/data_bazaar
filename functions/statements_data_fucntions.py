import os
import pandas as pd
import time
from concurrent.futures import ThreadPoolExecutor
# from pymongo import MongoClient
from playwright.sync_api import sync_playwright, TimeoutError as PlaywrightTimeoutError
# import boto3
from datetime import datetime
from io import BytesIO
def get_modified_dates(folder_path):
    if not os.path.exists(folder_path):
        raise FileNotFoundError(f"The folder '{folder_path}' does not exist.")
    
    modified_dates = []
    for file in os.listdir(folder_path):
        file_path = os.path.join(folder_path, file)
        if os.path.isfile(file_path):
            # Get the last modified timestamp
            timestamp = os.path.getmtime(file_path)
            # Convert timestamp to a readable format
            modified_date = datetime.fromtimestamp(timestamp).strftime('%Y-%m-%d')
            modified_dates.append(modified_date)
    return modified_dates
def screener_excel_data(url,stock_name_list:list,stock_name_list_with_ns_bo:list):
    with sync_playwright() as p:
        # stock_name_list = data[0]
        # stock_name_list_with_ns_bo = data[1]
        # stock_name_list 
        # stock_name_list_with_ns_bo 
        stock_count = 1
        browser = p.chromium.launch(headless=True)
        context = browser.new_context(
            accept_downloads=True, java_script_enabled=True
        )
        page = context.new_page()
        time.sleep(5)
        page.goto(url)
        time.sleep(2)
        username_input = page.locator('input[name="username"]')
        username_input.fill("abdullahsyed940@gmail.com")
        print('username fill')
        password_input = page.locator('input[type="password"]')
        password_input.fill("Abdullah@123")
        print('password')
        submit_button = page.locator('button[type="submit"]')
        print('press submit button')
        submit_button.click()
        search_stock_name = page.locator('#desktop-search .has-addon.left-addon.dropdown-typeahead input[aria-label="Search for a company"]')
        for index,(stock_name, stock_ns_bo) in enumerate(zip(stock_name_list, stock_name_list_with_ns_bo)):
            stock_count = stock_count + 1 
            try:
                file_names = get_modified_dates(fr'D:\projects\docker_images\data_bazaar\data\staements_data\{stock_ns_bo}')
                print(f'{stock_name}  is in checking phase')
                if len(file_names) == 0:
                    print(f"{stock_name} not found so it will scrape for the first time means it will move to except section")
                sorted_list_of_historical_data = []
                for name in file_names:
                    date_obj = datetime.strptime(name, "%Y-%m-%d").date()
                    sorted_list_of_historical_data.append(date_obj)
                previous_excel_file_date = max(sorted_list_of_historical_data)
                current_date = datetime.today().date()
                difference = (current_date - previous_excel_file_date).days
                if difference > 7 :
                    try:
                        print(f"Stock {stock_name} is greater than 7 than scrape the excel file again ")
                        search_stock_name.fill(stock_name)
                        time.sleep(5)
                        search_stock_name.press("Enter")
                        time.sleep(2)
                        # try :
                        button = page.locator('button[aria-label="Export to Excel"]')
                        button.click()
                        with page.expect_download() as download_info:
                            button.click(timeout=60000)
                        download = download_info.value
                        download_dir = f"D:/projects/docker_images/data_bazaar/data/staements_data/{stock_ns_bo}/"
                        os.makedirs(download_dir, exist_ok=True)
                        download_path = os.path.join(
                            download_dir, f"{current_date}.xlsx"
                        )
                        download.save_as(download_path)
                        print(f"{index}--> {stock_name} excel file uploaded to {download_dir}/{current_date}.xlsx") 
                    except:
                        print(f'{index}-->  {stock_ns_bo} not found in screener.in')
                        continue
                else:
                    print(f" {index}-->  {stock_name} will not scrape because difference > 7")
            except:
                print(f"Scraping for {stock_name}")
                time.sleep(2)
                search_stock_name.fill(stock_name)
                time.sleep(5)
                search_stock_name.press("Enter", delay=100)
                current_date = datetime.today().date()
                # try :
                save_path = r'D:\projects\docker_images\data_bazaar\screenshots\error.png'
                page.screenshot(path=save_path)
                button = page.locator('button[aria-label="Export to Excel"]')
                button.click()
                with page.expect_download() as download_info:
                    button.click(timeout=60000)
                download = download_info.value
                download_dir = f"D:/projects/docker_images/data_bazaar/data/staements_data/{stock_ns_bo}/"
                os.makedirs(download_dir, exist_ok=True)
                download_path = os.path.join(download_dir, f"{current_date}.xlsx")
                download.save_as(download_path)
                print(f"{index}--> {stock_name} excel file uploaded to {download_dir}/{current_date}.xlsx") 
                # except:
                #     print(f'{index}-->{stock_ns_bo} not found in screener.in')
        browser.close()
    
    return {'status':'done'} 