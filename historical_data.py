import requests as r
import finnhub
import json
import wget
from datetime import date, datetime, timedelta
import pandas as pd
import time


"""
api calls should be in the following format for aggregates (data over a specified time period)
API_AGG_BASE_URL/ticker/:ticker/range/:range/:timespan/:start-date/:end-date?adujsted=bool&sort=asc&limit=int&apiKey=API_KEY
if timespan = minute and range = 5 then 5-minute bars will be returned.

api calls should be in the following format for grouped daily 
API_AGG_BASE_URL/grouped/locale/us/market/stocks/:date?adjusted=bool&apiKey=API_KEY

api calls should be in the following format for daily open/close
API_DAILY_BASE_URL/:ticker/:date?adjusted=bool&apiKey=API_KEY

asc sorts in ascending order (oldest on top), and desc sorts in descending order (newest on top)
limit is an integer (default 5000, max 50000), number of instances


for other questions relating to the polygon api, visit https://polygon.io/docs/stocks/getting-started
"""

class GetData:
    def __init__(self):
        self.POLYGON_API_KEY = 'Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP'
        self.POLYGON_API_AGG_BASE_URL = 'https://api.polygon.io/v2/aggs'
        self.POLYGON_API_DAILY_BASE_URL = 'https://api.polygon.io/v1/open-close'

        self.FINNHUB_API_KEY = 'cluhrs1r01qv6nfuall0cluhrs1r01qv6nfuallg'
        self.finnhub_client = finnhub.Client(api_key=self.FINNHUB_API_KEY)

    def _create_api_call(self, query, params, call_type): ## type is daily, daily_aggregate, or time_period_aggregate
        for key in query.keys(): ## Makes sure that the query has all the necessary parameters
            if key not in params:
                print(params)
                raise ValueError('Query should have the specified params for the type of api call')
        print(params)
            
        if call_type == 'daily':
            api_call = f'{self.POLYGON_API_DAILY_BASE_URL}/{query["ticker"]}/{query["date"]}?adjusted={query["adjusted"]}&apiKey={self.POLYGON_API_KEY}'
        elif call_type == 'daily_aggregate':
            api_call = f'{self.POLYGON_API_AGG_BASE_URL}/grouped/locale/us/market/stocks/{query["date"]}?adjusted={query["adjusted"]}&apiKey={self.POLYGON_API_KEY}'
        elif call_type == 'time_period_aggregate':
            api_call = f'{self.POLYGON_API_AGG_BASE_URL}/ticker/{query["ticker"]}/range/{query["range"]}/{query["timespan"]}/{query["start_date"]}/{query["end_date"]}?adjusted={query["adjusted"]}&limit={query["limit"]}&apiKey={self.POLYGON_API_KEY}'

        return api_call

    def get_polygon_data(self, query, agg): 
        ## Query should be an object as input with what should be passed to the query string
        ## Agg should be false if calling for daily, otherwise it should be a string, either daily or time_period
        if type(query) != dict:
            print(type(query))
            raise ValueError("Query must be a dictionary")
        else:
            if agg != False: 
                if agg == 'time_period': ## Not really necessary, since daily_agg gets the same data. I don't want to get rid of it in case I find some use for it
                    params = ['ticker', 'range', 'timespan', 'start_date', 'end_date', 'adjusted', 'sort', 'limit']
                    call_type = 'time_period_aggregate'
                elif agg == 'daily_agg': ## Can only get data 2 years from present date, start getting and savind data sooner rather than later
                    params = ['date', 'adjusted']
                    call_type = 'daily_aggregate'
                else:
                    raise ValueError("If agg isn't false, it should either be time_period of daily_agg")
            else:
                params = ['ticker', 'date', 'adjusted']
                call_type = 'daily'

            api_call = self._create_api_call(query, params, call_type)
            response = r.get(api_call).json() ## remove .json() when errors come up to see status code
            print(response)
            return response
        
    def get_financials(self, ticker):
        
        financials = self.finnhub_client.company_basic_financials(ticker, 'all')
        ## Gets data in 3-month increments

        return financials
    
    def get_insider_sentiment(self, ticker, start_date, end_date): ## Dates in format YYYY-MM-DD
        insider_data = self.finnhub_client.stock_insider_sentiment(ticker, start_date, end_date)
        ## Gets data in 1-month increments

        return insider_data
    
    def get_vix_history(self):
        wget.download("https://cdn.cboe.com/api/global/us_indices/daily_prices/VIX_History.csv", "VIX_History.csv")



    
    ## Maybe for some feature engineering, engineer a % change feature (not given by api)



class SortData: ## Used to get the historical data necessary
    def __init__(self, data_class):
        self.data_class = data_class
    
    def _get_earliest_possible_date(self, date): 
        digits_to_change = int(date[2:4])
        digits_to_change -= 2
        new_date = date[:2] + str(digits_to_change) + date[4:]
        return new_date
    
    def _extract_date_info(self, date): 
        date = datetime.strptime(date, '%Y-%m-%d')
        year = int(date.year)
        month = int(date.month)
        day = int(date.day)
        return [year, month, day]
    
    def _daterange(self, start_date, end_date): ## Adapted from https://stackoverflow.com/questions/1060279/iterating-through-a-range-of-dates-in-python
        start_date = datetime.strptime(start_date, '%Y-%m-%d')
        end_date = datetime.strptime(end_date, '%Y-%m-%d')

        for n in range(int((end_date - start_date).days + 1)):
            yield str(start_date + timedelta(n))[:10]
    
    def _build_daily_agg_query(self): ## Builds out a list of all query dictionaries possible, starting 2 years from today
        queries = []
        todays_date = str(date.today())
        earliest_possible_date = self._get_earliest_possible_date(todays_date)

        for single_date in self._daterange(earliest_possible_date, todays_date): ## Iterate over all possible dates
            queries.append({ 
                'date': single_date, 
                'adjusted': 'true'  ## String for the future api call
                })
        
        return queries
    

    def _build_daily_open_close_query(self, ticker):
        queries = []
        todays_date = str(date.today())
        earliest_possible_date = self._get_earliest_possible_date(todays_date)

        for single_date in self._daterange(earliest_possible_date, todays_date): ## Iterate over all possible dates
            queries.append({ 
                'ticker': ticker,
                'date': single_date, 
                'adjusted': 'true'  ## String for the future api call
                })
            
        return queries
    

    def _build_time_period_agg_query(self): pass 
    ## Not really necessary right now so I'm not going to build it
    ## Mainly just here in case I find a need for it, since there's already the code for the steps before building queries

        
    def get_and_sort_data(self):
        data_df = pd.DataFrame()
        tickers = []

        daily_agg_queries = self._build_daily_agg_query()
        for i, query in enumerate(daily_agg_queries):
            time_diff = int((datetime.strptime(daily_agg_queries[-1]['date'], '%Y-%m-%d') - datetime.strptime(daily_agg_queries[i]['date'], '%Y-%m-%d')).days)
            print(f'time difference: {time_diff}')
            print(f"i = {i}")
            if i == 1: break ## For testing purposes

            print(f"i = {i}")
            if i % 5 == 4: 
                print('sleeping')
                time.sleep(60) ## Can only make 5 api calls per minute, so this stops the code from running for 1 minute until it can again
            ##if query['queryCount'] == 0: continue ## Skips over weekends
            data = self.data_class.get_polygon_data(query, 'daily_agg')
            if data['queryCount'] == 0: continue ## Skips over weekends/holidays
            if data_df.empty: ## Checks if the df is empty, using not and XOR
                for i in range(len(data['results'])):
                    tickers.append(data['results'][i]['T']) ## Should append all of the tickers from the first API call
                data_df = data_df.from_dict(data['results']) ## Base case
            else:
                data_df.loc[i] = data
            
        
        data_df.dropna(inplace=True)
        print(tickers)
        print(data_df)


