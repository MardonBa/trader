import unittest
from historical_data import SortData, GetData

class TestSortData(unittest.TestCase):
    def setUp(self):
        polygon_api_key, finnhub_api_key = 'Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP', 'cluhrs1r01qv6nfuall0cluhrs1r01qv6nfuallg' ## Not necessary with mocks?
        data = GetData(polygon_api_key, finnhub_api_key) ## Instance of the GetData class, to be passed into the SortData class to be called
        self.sort_data_class = SortData(data)

    def test_create_api_call(self):
        pass