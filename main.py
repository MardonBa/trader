import get_realtime_prices
from  historical_data import GetData, SortData
import pandas as pd

data = GetData() ## Instance of the GetData class, to be passed into the SortData class to be called

data_sorted = SortData(data)
data_sorted.get_and_sort_initial_data()


"""
3 models to be trained right now:
Predictions per day, per month, per 3 months
"""

## Predictor should be hooked up to what is currently owned to predict if a sell is necessary