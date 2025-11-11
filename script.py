import requests
import os
from dotenv import load_dotenv
import time
# import csv
from datetime import datetime
import snowflake.connector
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000


def run_stock_job():
    url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"
    now = datetime.now()
    timestamp = now.strftime("%Y-%m-%d_%H-%M-%S")
    DS = now.strftime("%Y-%m-%d")
    print(f"Stock job started at {timestamp}")


    response = requests.get(url)
    # print(response)
    data = response.json()
    # print(data.keys())
    # dict_keys(['results', 'status', 'request_id', 'count', 'next_url'])
    tickers = []
    round = 1
    for ticker in data['results']:
        ticker['ds'] = DS
        tickers.append(ticker)

    while 'next_url' in data:
        if round == 5:
            print('wait for 1 min to prevent exceed max requests per minutes')
            time.sleep(60)
            round=0
        print('get next url: ' + data['next_url'])
        response = requests.get(data['next_url']+f'&apiKey={POLYGON_API_KEY}')
        data = response.json()
        for ticker in data['results']:
            ticker['ds'] = DS
            tickers.append(ticker)
        round += 1
    
    example_ticker = {'ticker': 'AAAU', 
                      'name': 'Goldman Sachs Physical Gold ETF Shares', 
                      'market': 'stocks', 
                      'locale': 'us',
                      'primary_exchange': 'BATS', 
                      'type': 'ETF', ''
                      'active': True, 
                      'currency_name': 'usd', 
                      'cik': '0001708646', 
                      'composite_figi': 'BBG00LPXX872', 
                      'share_class_figi': 'BBG00LPXX8Z1', 
                      'last_updated_utc': '2025-11-08T07:06:06.688448844Z',
                      'ds': '2025-11-08'}

    fieldnames = list(example_ticker.keys())
    load_to_snowflake(tickers, fieldnames)
    print(f'Loaded {len(tickers)} tickers to Snowflake.')

    # write tickers to csv with example_ticker schema
    # fieldnames = list(example_ticker.keys())
    # output_file = f'tickers_{timestamp}.csv'
    
    # Write tickers to CSV
    # with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
    #     writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore', restval=None)
    #     writer.writeheader()
    #     for ticker in tickers:
    #         # Ensure all fields exist in the dictionary, even if they're None
    #         row = {field: ticker.get(field, None) for field in fieldnames}
    #         # Clean any potential problematic characters
    #         row = {k: str(v).replace('\xa0', ' ').strip() if v is not None else None for k, v in row.items()}
    #         writer.writerow(row)
    # print(f"{len(tickers)} tickers written to {output_file}")


def load_to_snowflake(rows, fieldnames):
    connect_kwargs = {
        'user': os.getenv("SNOWFLAKE_USER"),
        'password': os.getenv("SNOWFLAKE_PASSWORD"),
        'account': os.getenv("SNOWFLAKE_ACCOUNT"),
        'warehouse': os.getenv("SNOWFLAKE_WAREHOUSE"),
        'database': os.getenv("SNOWFLAKE_DATABASE"),
        'schema': os.getenv("SNOWFLAKE_SCHEMA"),
        'role': os.getenv("SNOWFLAKE_ROLE"),
    }

    conn = snowflake.connector.connect(
        user = connect_kwargs['user'],
        password = connect_kwargs['password'],
        account = connect_kwargs['account'],
        warehouse = connect_kwargs['warehouse'],
        database = connect_kwargs['database'],
        schema = connect_kwargs['schema'],
        role = connect_kwargs['role'],
        session_parameters={
            "CLIENT_TELEMETRY_ENABLED": False
        }
    )
    try:
        cs = conn.cursor()
        try:
            table_name = os.getenv("SNOWFLAKE_TABLE", "stock_tickers")
            type_overrides = {
                'ticker': 'VARCHAR',
                'name': 'VARCHAR',
                'market': 'VARCHAR',
                'locale': 'VARCHAR',
                'primary_exchange': 'VARCHAR',
                'type': 'VARCHAR',
                'active': 'BOOLEAN',
                'currency_name': 'VARCHAR',
                'cik': 'VARCHAR',
                'composite_figi': 'VARCHAR',
                'share_class_figi': 'VARCHAR',
                'last_updated_utc': 'TIMESTAMP_NTZ',
                'ds': 'DATE'
            }
            columns_sql_parts = []
            for col in fieldnames:
                col_type = type_overrides.get(col, 'VARCHAR')
                columns_sql_parts.append(f'"{col.upper()}" {col_type}')
            create_table_sql = f'CREATE TABLE IF NOT EXISTS {table_name} (' +', '.join(columns_sql_parts) + ');'
            cs.execute(create_table_sql)

            column_list = ", ".join([f'"{col.upper()}"' for col in fieldnames])
            placeholders = ", ".join([f'%({col})s' for col in fieldnames])
            insert_sql = f'INSERT INTO {table_name} ({column_list}) VALUES ({placeholders});'

            transformed = []
            for t in rows:
                row = {}
                for k in fieldnames:
                    row[k] = t.get(k, None)
                # print(row)
                transformed.append(row)
            
            if transformed:
                cs.executemany(insert_sql, transformed)
        finally:
            cs.close()
    finally:
        conn.close()
        




if __name__ == "__main__":
    run_stock_job()

