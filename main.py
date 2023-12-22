import get_realtime_prices
from  historical_data import GetData, SortData
import pandas as pd

data = GetData() ## Instance of the GetData class, to be passed into the SortData class to be called

data_sorted = SortData(data)
data_sorted.get_and_sort_data()


"""
3 models to be trained right now:
Predictions per minute, per hour, per day
"""