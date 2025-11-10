import requests
import os
from dotenv import load_dotenv
import time
import csv
from datetime import datetime
load_dotenv()
POLYGON_API_KEY = os.getenv("POLYGON_API_KEY")
LIMIT = 1000


def run_stock_job():
    url = f"https://api.massive.com/v3/reference/tickers?market=stocks&active=true&order=asc&limit={LIMIT}&sort=ticker&apiKey={POLYGON_API_KEY}"

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M-%S")
    print(f"Stock job started at {timestamp}")


    response = requests.get(url)
    # print(response)
    data = response.json()
    # print(data.keys())
    # dict_keys(['results', 'status', 'request_id', 'count', 'next_url'])
    tickers = []
    round = 1
    for ticker in data['results']:
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
                      'last_updated_utc': '2025-11-08T07:06:06.688448844Z'}
    
    # write tickers to csv with example_ticker schema
    fieldnames = list(example_ticker.keys())
    output_file = f'tickers_{timestamp}.csv'
    
    # Write tickers to CSV
    with open(output_file, 'w', newline='', encoding='utf-8') as csvfile:
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore', restval=None)
        writer.writeheader()
        for ticker in tickers:
            # Ensure all fields exist in the dictionary, even if they're None
            row = {field: ticker.get(field, None) for field in fieldnames}
            # Clean any potential problematic characters
            row = {k: str(v).replace('\xa0', ' ').strip() if v is not None else None for k, v in row.items()}
            writer.writerow(row)
    print(f"{len(tickers)} tickers written to {output_file}")

if __name__ == "__main__":
    run_stock_job()

