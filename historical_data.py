import requests as r
import finnhub
import wget
from datetime import date, datetime, timedelta
import pandas as pd
import time
import traceback
import os
import re
from warnings import simplefilter 
simplefilter(action="ignore", category=pd.errors.PerformanceWarning)
## Ignores the warnings raised by pandas when adding financial data columns to the data dataframe

pd.set_option('display.max_columns', None)



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
    def __init__(self, polygon_api_key, finnhub_api_key):
        self.POLYGON_API_KEY = polygon_api_key
        self.POLYGON_API_AGG_BASE_URL = 'https://api.polygon.io/v2/aggs'
        self.POLYGON_API_DAILY_BASE_URL = 'https://api.polygon.io/v1/open-close'

        self.FINNHUB_API_KEY = finnhub_api_key
        self.finnhub_client = finnhub.Client(api_key=self.FINNHUB_API_KEY)

    def _create_api_call(self, query, params, call_type): ## type is daily, daily_aggregate, or time_period_aggregate
        for key in query.keys(): ## Makes sure that the query has all the necessary parameters
            if key not in params:
                print(params)
                raise ValueError('Query should have the specified params for the type of api call')
            
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
            return response
        
    def get_financials(self, ticker):
        
        financials = self.finnhub_client.company_basic_financials(ticker, 'all')
        ## Gets basic financial data
        ## The data not stored in the dictionary mapped to metric won't be used due to inconsistency in the time-frames represented

        return financials
    
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

    def _get_time_diff(self, time1, format1, time2, format2):
        time_diff = int((datetime.strptime(time1, format1) - datetime.strptime(time2, format2)).days)
        return time_diff

    def _add_time_diff(self, original, time):
        new_str = f'{original}_{time}_market_days_before'
        return new_str
    
    def _rename_cols(self, df, time_diff): ## Calls the above function in order to properly rename the columns
        df.rename(columns={
            'T': 'ticker', ## To be removed 
            'c': self._add_time_diff('_close_price', time_diff), 
            'h': self._add_time_diff('_highest_price', time_diff),
            'l': self._add_time_diff('_lowest_price', time_diff),
            'n': self._add_time_diff('_num_transactions', time_diff),
            'o': self._add_time_diff('_open_price', time_diff),
            't': self._add_time_diff('_unix_timestamp', time_diff), ## To be removed
            'v': self._add_time_diff('_trading_volume', time_diff),
            'vw': self._add_time_diff('_trading_volume_weighted', time_diff)
        }, inplace=True)

        return df
    
    def _merge_on_tickers(self, df1, df2): 
        print(df1)
        print(df2)
        common_tickers = set.union(set(df1['ticker']), set(df2['ticker']))

        for ticker in df1['ticker']:
            if ticker not in common_tickers:
                df1.drop(ticker, axis=0, inplace=True)

        for ticker in df2['ticker']:
            if ticker not in common_tickers:
                df2.drop(ticker, axis=0, inplace=True)

        for col in df2.columns:
            if col == 'ticker': continue ## Surely there's a better solution to skip the ticker column, but it's late
            else:
                df1[col] = df2[col].copy()

        return df1
                
    def _check_union_of_data(self, l1, l2):
        common_data = set.union(set(l1), set(l2))
        return common_data
    
    def _get_index_by_row(self, df, row, value):
        return df.index.get_loc(df[df[row] == value].index[0])

    def make_polygon_call(self, query, time_diff):
        ## query is a query dict from self._build_daily_agg_query, each one is iterated through and passed into this function
        ## current_date is the date to subtract from (daily_agg_queries[-1]['date'])

        df = pd.DataFrame()

        data = self.data_class.get_polygon_data(query, 'daily_agg')
        if data['queryCount'] == 0: return None ## Allows for skipping over weekends/holidays

        df = df.from_dict(data['results']) ## Base case (first successful API call)
        df = self._rename_cols(df, time_diff) ## Renames the column accordingly

        return df



    def make_finnhub_call(self, ticker): 
    ## df_cols initialized to an empty list for the first time the function is called, then the financial columns should be passed to it every time thereafter

        try:
            financials_data = self.data_class.get_financials(ticker)['metric']
            if len(financials_data) < 100: 
                print('Not sufficient data')
                return None ## Including things that have less than 100 financial metrics would discount too many financial metrics and tickers
                ## returning None should trigger code to remove the ticker from the dataframe


            df = pd.DataFrame(financials_data, index=[0])

        except Exception as err: 
            print(f'error: {traceback.format_exc()}')
            ## This might be necessary later when it gets to where Finnhub doesn't have data for a ticker, so handling that case
            ## Haven't hit that in tests so far, so it's just here and empty for now
        print('it worked')
        return df
    

    def make_vix_call(self):
        self.data_class.get_vix_history() ## Gets the history and saves it to the folder

        if not os.path.isfile('VIX_History.csv'):
            raise Exception('Something went wrong with downloading the VIX History')
        
        else:
            df = pd.read_csv('VIX_History.csv')
            print(df['DATE'])

            df['DATE'] = df['DATE'].str.replace('/', '-') ## Makes sure dates are in a consistent format
            df['DATE'] = pd.to_datetime(df['DATE'], format='%m-%d-%Y').dt.strftime('%Y-%m-%d') ## Chagnes the date format fof consistency

            return df



        
    def get_and_sort_initial_data(self):
        data_df = pd.DataFrame()
        ## TODO Write tests (lame)

        queries = self._build_daily_agg_query()
        print(queries[0]['date'])
        query_start_index = 0
        query_end_index = 4

        financial_start_index = 0
        financial_end_index = 60
        df_cols = []
        i_offset = 0

        data_df = pd.DataFrame()

        ## Get the VIX historical data, add it to the df at the end
        vix_data = self.make_vix_call()
        time_offset = 730 ## Fix for adding 1 day to earliest_date, which was giving an error for some reason
                          ## Subtract one every time the while loop is run
        while True: ## Loop gets broken out of once the earliest possible date is found
            earliest_date = str((date.today() - timedelta(days=time_offset)).strftime('%Y-%m-%d')) ## Starts at 2 years before, consistent with polygon data
            print(f'\n{earliest_date}')
            if earliest_date in set(vix_data['DATE']):
                slicing_index = self._get_index_by_row(vix_data, 'DATE', earliest_date)
                break
            time_offset -= 1

        vix_data = vix_data[slicing_index:] ## Slices the VIX data so that it only includes necessary dates
        market_open_dates = vix_data['DATE'].tolist()

        for query in queries: ## Queries are in format {date: YYYY-MM-DD, adjusted: bool}
            if query['date'] not in set(market_open_dates): ## Checks to see if the market is NOT open on the given date
                queries.remove(query)
        queries = queries[::-1] ## The list is reversed to have more recent dates first

        polygon_done = False
        finnhub_done = False

        while not (polygon_done and finnhub_done): ## Runs until all of the data has been fetched

            ## Call the polygon API data, add it to the df
            for i in range(query_start_index, query_end_index+1):
                polygon_data = self.make_polygon_call(queries[i], i)
                if polygon_data is None or polygon_data.empty: continue ## Skips over weekends/holidays where there is no data to be collected
                ## ! Should be removed when I add market day counting based on VIX History
                elif data_df.empty:
                    data_df = polygon_data.copy() 
                    continue
                else:
                    data_df = self._merge_on_tickers(data_df, polygon_data)
                
            if i > len(queries): polygon_done = True

            ## Call the finnhub API, add it to the df
            ## On first iteration of the while loop (when query_start_index == 0), when making the financial data, first fill everything with None
            for i in range(financial_start_index, financial_end_index):
                print(i)
                print(i_offset)
                print(i-i_offset)
                ticker = data_df.at[i, 'ticker'] ## Gets the current ticker to work on
                financial_data = self.make_finnhub_call(ticker)
                if financial_data is None: 
                    i_offset += 1
                    ticker_index = self._get_index_by_row(data_df, 'ticker', ticker) ## Drop the ticker since it won't have any data
                    data_df.drop(ticker_index, axis=0).reset_index().drop('index', axis=1, inplace=True) ## resetting index and dropping index col fixes issues where dropped rows affect the offset, etc
                    continue
                if i - i_offset == 0: ## Better way of checking for first iteration, fixes bug where first round of collected data was reset to Noe
                    for key in financial_data.keys():
                        data_df[key] = [None for _ in range(len(data_df))]
                    df_cols = list(financial_data.keys()) ## using [] instead of list() gives an error
                
                else:
                    current_ticker_cols = set(financial_data.columns)
                    cols_to_remove = set(df_cols).difference(current_ticker_cols) ## Gets the columns that aren't in the current ticker's financial metrics so they can be removed
                    for col in cols_to_remove:
                        data_df.drop(col, axis=1, inplace=True) ## Removes the column from the dataframe inplace
                        df_cols.remove(col) ## Removes the column from the list of columns in the dataframe


                for col in df_cols: ## This for loop adds the recently fetched data to the dataframe
                    data_df.at[i - i_offset, col] = financial_data.at[0, col]

                if data_df['10DayAverageTradingVolume'].iloc[-1] != None: 
                    finnhub_done = True ## I'm pretty sure that column is in every response, so it's a good one to measure if we're done, since it won't be removed
                    break ## Break since we're done

                
            
                
            query_start_index += 5
            query_end_index += 5
            financial_start_index += 60
            financial_end_index += 60

            time.sleep(60)
        
       
        ## Get the VIX historical data, add it to the df
        vix_data = vix_data[::-1].reset_index() ## Reverses the dataframe to make counting market days easier
        for index, row in vix_data.iterrows(): ## Index not needed
            ## Set the same value for each ticker
            data_df[f'_vix_open_{index+1}_market_days_before'] = [row['OPEN'] for _ in range(len(data_df))]
            data_df[f'_vix_close_{index+1}_marketdays_before'] = [row['CLOSE'] for _ in range(len(data_df))]
            data_df[f'_vix_high_{index+1}_market_days_before'] = [row['HIGH'] for _ in range(len(data_df))]
            data_df[f'_vix_low_{index+1}_market_days_before'] = [row['LOW'] for _ in range(len(data_df))]
            ## Index is +1 because the VIX data doesn't include the data for the current day


            
        
        print(data_df.columns)
        print(data_df.head(50))
        print(len(data_df))
        #for col in data_df.columns:
            #print(f'{col}: {data_df[col].isna().sum()}')
        ## TODO Add code that checks for columns that have None, if they have too many NoneTypes, remove the column (see how many get removed)
        return data_df
        

        ## TODO Write a diagnostic function that finds what filtering condition for amount of financial data results in the least loss

    def _find_integers(self, string):
        indexes = [re.search(r'\d', string).start()] ## Get the index of the first integer in the string
            ## Index is a list so that if the number has multiple digits, the index of other digits can be added to the list
        for i in range(1, 6): ## 6 is just used as a placeholder. Realistically, the differences in dates won't get greater than 5 digits
            if string[indexes[0]+i] in {'0','1','2','3','4','5','6','7','8','9'}: ## Checks if the next character is an integer, search is O(1)
                indexes.append(indexes[0]+i)
            else: break ## Once the last digit has been found, break out of the loop
        return indexes

    def _change_time_diff(self, col, ):
        index = self._find_integers(col)
        num_value = int(col[index[0]:index[-1]+1]) + 1 ## Represents the numerical value we found, +1 to add the date as needed
        new_string = f'{col[:index[0]]}{str(num_value)}{col[index[-1]+1:]}' ## Assigns the new string
        return new_string
    
    def _remove_oldest_historical(self, df): ## Removes the historical columns that have the highest # of market days from current date, for the working_df
        cols = [[col for col in df.columns if col[0] == '_']] ## Get all historical columns
        highest_value = 0
        for col in cols:
            indexes = self._find_integers(col)
            num_value = int(col[indexes[0]:indexes[-1]+1]) ## Store the value of the integer in the string as an integer
            if num_value > highest_value: ## Assign the highest value to the value found if it is greater
                highest_value = num_value

        for col in cols:
            if str(highest_value) in col: ## Drop the column if that furthest date is in the column name
                df.drop(col, axis=1, inplace=True)

        ## I know this is really bad in terms of time complexity, but given that there aren't a ton of points to go through, it doesn't matter a ton
        ## I also can't think of another way to do it right now (listening to mr williams lecture)
        return df





    def get_todays_data(self, df): ## df should be the current working df
        ## ? This should only be called on days that the market is open
        ## Should return 2 dataframes: one with all historical data ever, one with same # of columns as original (drop the furthest date from price and vix history)
        all_time_df = df.copy() ## All historical data ever collected
        working_df = df.copy() ## Maintains # of columns and market day count

        for col in df.cols:
            if col[0] == '_': ## Ever wonder why the VIX and historical data column names are prepended by _ ? This is why!
                all_time_df.rename(columns={col: self.get_todays_data(col)})
                working_df.rename(columns={col: self.get_todays_data(col)})


        vix_data = self.make_vix_call().iloc[-1].reset_index() ## Get the most recent update to the VIX dataset
        ## Set the same value for each ticker
        ## TODO Test this code to make sure it works
        all_time_df[f'_vix_open_1_market_days_before'] = [vix_data['OPEN'] for _ in range(len(all_time_df))]
        all_time_df[f'_vix_close_1_marketdays_before'] = [vix_data['CLOSE'] for _ in range(len(all_time_df))]
        all_time_df[f'_vix_high_1_market_days_before'] = [vix_data['HIGH'] for _ in range(len(all_time_df))]
        all_time_df[f'_vix_low_1_market_days_before'] = [vix_data['LOW'] for _ in range(len(all_time_df))]

        working_df[f'_vix_open_1_market_days_before'] = [vix_data['OPEN'] for _ in range(len(all_time_df))]
        working_df[f'_vix_close_1_marketdays_before'] = [vix_data['CLOSE'] for _ in range(len(all_time_df))]
        working_df[f'_vix_high_1_market_days_before'] = [vix_data['HIGH'] for _ in range(len(all_time_df))]
        working_df[f'_vix_low_1_market_days_before'] = [vix_data['LOW'] for _ in range(len(all_time_df))]
        ## Index is +1 because the VIX data doesn't include the data for the current day

        polygon_data = self.make_polygon_call(str(date.today()), 1) 
        all_time_df = self._merge_on_tickers(all_time_df, polygon_data)
        working_df = self._merge_on_tickers(working_df, polygon_data)

        ## TODO I need to test this to make sure it works
        finnhub_done = False
        df_cols = [col for col in df.columns if col[0] != '_'] ## Add all of the financial columns to this list
        while not finnhub_done:
            for i in range(financial_start_index, financial_end_index):
                ticker = df.at[i, 'ticker'] ## Gets the current ticker to work on
                financial_data = self.make_finnhub_call(ticker)
                if financial_data is None: ## Just move on, and the data for this ticker will be None
                    continue
                elif i  == 0: ## Better way of checking for first iteration, fixes bug where first round of collected data was reset to Noe
                    for column in df_cols: ## Reset the financial data
                        all_time_df[column] = [None for _ in range(len(df))]
                        working_df[column] = [None for _ in range(len(df))]

                for col in df_cols: ## This for loop adds the recently fetched data to the dataframe
                    if col not in set(financial_data.columns): ## Skip over data that we don't actually have for the current ticker, so it stays as None
                        continue
                    all_time_df.at[i, col] = financial_data.at[0, col] ## No need for i_offset here since no tickers will be removed
                    working_df.at[i, col] = financial_data.at[0, col] 

                if i == len(df): ## Checks to see if we are done with getting all the new financial data
                    finnhub_done = True
                    break ## Break since we're done

            
            financial_start_index += 60
            financial_end_index += 60

            time.sleep(60)

        working_df = self._remove_oldest_historical(working_df)
        return all_time_df, working_df

