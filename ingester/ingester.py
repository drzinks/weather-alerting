import credentials, requests, pprint
COMPANY_SYMBOL = 'AAPL'
URL = (f'https://www.alphavantage.co/query?function=TIME_SERIES_INTRADAY'
       f'&symbol={COMPANY_SYMBOL}'
       f'&interval=1min&apikey={credentials.api_key}')

response = requests.get(URL)
data = response.json()
pprint.pprint(data)