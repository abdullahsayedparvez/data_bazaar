import pandas as pd
import os , sys
from sqlalchemy import create_engine
import mysql.connector as MyConn
pwd = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
sys.path.append(pwd)
from config import CONFIG
def insert_yfinance_data(ticker,data):
    # username = os.getenv('USERNAME_SQL')
    # password = os.getenv('PASSWORD_SQL')
    # host = os.getenv('HOST_SQL')
    username  = 'abdullah'
    password = 'Abdullah@123'
    host = 'CONFIG.HOST_SQL'
    
    connection = MyConn.connect(
        host=f'{CONFIG.HOST_SQL}',
        user=f'{CONFIG.USERNAME_SQL}',
        password=f'{CONFIG.PASSWORD_SQL}'
    )
    
    if connection.is_connected():
        cursor = connection.cursor()

        create_database_query = "CREATE DATABASE IF NOT EXISTS stock_data_db;"
        cursor.execute(create_database_query)

        cursor.execute("USE stock_data_db")

        check_table_query = f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE() AND table_name = '{ticker}';
        """
        cursor.execute(check_table_query)
        table_exists = cursor.fetchone()[0]

        if table_exists:
            truncate_table_query = f"TRUNCATE TABLE `{ticker}`;"
            cursor.execute(truncate_table_query)
        else:
            create_table_query = f"""
            CREATE TABLE `{ticker}` (
                date DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                adjclose FLOAT,
                volume INT,
                ticker VARCHAR(50) NOT NULL,
                PRIMARY KEY (date, ticker)
            );
            """
            cursor.execute(create_table_query)
        for index, row in data.iterrows():
            insert_query = f"""
                INSERT INTO `{ticker}` (date, open, high, low, close, adjclose, volume, ticker)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
            """
            values = (
                row['date'],  # Assume 'date' is already in a valid datetime format
                float(row['open']) if pd.notna(row['open']) else None,
                float(row['high']) if pd.notna(row['high']) else None,
                float(row['low']) if pd.notna(row['low']) else None,
                float(row['close']) if pd.notna(row['close']) else None,
                float(row['adjclose']) if pd.notna(row['adjclose']) else None,
                int(row['volume']) if pd.notna(row['volume']) else None,
                row['ticker'] if pd.notna(row['ticker']) else None
            )
            cursor.execute(insert_query, values)
        connection.commit()
        cursor.close()
        connection.close()
    return f'{ticker} stored in SQL database'

def insert_latest_yfinance_data_(ticker, data):
    # Get database connection details from environment variables
    username = os.getenv('USERNAME_SQL')
    password = os.getenv('PASSWORD_SQL')
    host = os.getenv('HOST_SQL')

    # Establish connection to the database
    connection = MyConn.connect(
        host=host,
        user=username,
        password=password
    )
    
    if connection.is_connected():
        cursor = connection.cursor()

        # Create database if it doesn't exist
        create_database_query = "CREATE DATABASE IF NOT EXISTS stock_data_db;"
        cursor.execute(create_database_query)

        # Use the database
        cursor.execute("USE stock_data_db")

        create_table_query = f"""
            CREATE TABLE IF NOT EXISTS `{ticker}` (
                date DATE NOT NULL,
                open FLOAT,
                high FLOAT,
                low FLOAT,
                close FLOAT,
                adjclose FLOAT,
                volume INT,
                ticker VARCHAR(50) NOT NULL,
                PRIMARY KEY (date, ticker)
            );
            """
        cursor.execute(create_table_query)

        # Now, insert data that doesn't already exist in the table
        for index, row in data.iterrows():
            # Check if the date and ticker already exist in the table
            check_existing_query = f"""
            SELECT COUNT(*)
            FROM `{ticker}`
            WHERE date = %s AND ticker = %s;
            """
            cursor.execute(check_existing_query, (row['date'], row['ticker']))
            exists = cursor.fetchone()[0]

            if not exists:  # Only insert if the combination of date and ticker does not exist
                insert_query = f"""
                    INSERT INTO `{ticker}` (date, open, high, low, close, adjclose, volume, ticker)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s);
                """
                values = (
                    row['date'],  # Assume 'date' is already in a valid datetime format
                    float(row['open']) if pd.notna(row['open']) else None,
                    float(row['high']) if pd.notna(row['high']) else None,
                    float(row['low']) if pd.notna(row['low']) else None,
                    float(row['close']) if pd.notna(row['close']) else None,
                    float(row['adjclose']) if pd.notna(row['adjclose']) else None,
                    int(row['volume']) if pd.notna(row['volume']) else None,
                    row['ticker'] if pd.notna(row['ticker']) else None
                )
                cursor.execute(insert_query, values)

        # Commit the transaction
        connection.commit()
        cursor.close()
        connection.close()

    return f'{ticker} data stored in SQL database'

        