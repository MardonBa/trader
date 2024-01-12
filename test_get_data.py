import unittest
from historical_data import GetData


class TestGetDataMethods(unittest.TestCase):
    def setUp(self):
        polygon_api_key, finnhub_api_key = 'Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP', 'cluhrs1r01qv6nfuall0cluhrs1r01qv6nfuallg' ## Not necessary with mocks?
        self.get_data_class = GetData(polygon_api_key, finnhub_api_key) ## Instance of the GetData class, to be passed into the SortData class to be called