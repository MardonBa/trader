import get_realtime_prices
from  historical_data import GetData, SortData

polygon_api_key, finnhub_api_key = 'Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP', 'cluhrs1r01qv6nfuall0cluhrs1r01qv6nfuallg'

data = GetData(polygon_api_key, finnhub_api_key) ## Instance of the GetData class, to be passed into the SortData class to be called

data_sorted = SortData(data)
data_sorted.get_and_sort_initial_data()

"""
3 models to be trained right now:
Predictions per day, per month, per 3 months
"""

## Predictor should be hooked up to what is currently owned to predict if a sell is necessary
