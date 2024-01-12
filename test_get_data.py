import unittest
from historical_data import GetData
from datetime import date, timedelta


class TestGetDataMethods(unittest.TestCase):
    def setUp(self):
        if 'test_create_api_call' in self._testMethodName: ## conditional setup for testing GetData._create-api-call
            polygon_api_key, finnhub_api_key = 'Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP', 'cluhrs1r01qv6nfuall0cluhrs1r01qv6nfuallg' ## Not necessary with mocks?
            self.get_data_class = GetData(polygon_api_key, finnhub_api_key) ## Instance of the GetData class, to be passed into the SortData class to be called
            if 'daily' in self._testMethodName:
                if 'aggregate' in self._testMethodName:
                    self.call_type = 'daily_aggregate'
                else:
                    self.call_type = 'daily'
            else:
                self.call_type = 'time_period_aggregate'

    ########################################
    ## The following methods GetData()._create_api_call(query, params, call_type)
    ########################################
            
    ## tests for when call_type = daily
    def test_create_api_call_daily_call_type(self):
        query = {
            'ticker': 'AAPL',
            'date': str(date.today()), ## Gets the current date
            'adjusted': 'true'
        }
        params = ['ticker', 'date', 'adjusted']

        real_output = self.get_data_class._create_api_call(query, params, self.call_type)
        expected_output = f'https://api.polygon.io/v1/open-close/AAPL/{str(date.today())}?adjusted=true&apiKey=Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP'

        self.assertEqual(real_output, expected_output)

    def test_create_api_call_daily_call_type_wrong_params(self):
        query = {
            'ticker': 'AAPL',
            'date': str(date.today()), ## Gets the current date
            'adjusted': 'true'
        }
        params = ['ticker', 'date']

        with self.assertRaises(ValueError):
            self.get_data_class._create_api_call(query, params, self.call_type)

    def test_create_api_call_daily_call_type_wrong_query(self):
        query = {
            'ticker': 'AAPL',
            'date': str(date.today()) ## Gets the current date
        }
        params = ['ticker', 'date', 'adjusted']

        with self.assertRaises(KeyError):
            self.get_data_class._create_api_call(query, params, self.call_type)

    
    ## Test for when call_type = daily_aggregate
    def test_create_api_call_daily_aggregate_call_type(self):
        query = {
            'date': str(date.today()), ## Gets the current date
            'adjusted': 'true'
        }
        params = ['date', 'adjusted']

        real_output = self.get_data_class._create_api_call(query, params, self.call_type)
        expected_output = f'https://api.polygon.io/v2/aggs/grouped/locale/us/market/stocks/{str(date.today())}?adjusted=true&apiKey=Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP'

        self.assertEqual(real_output, expected_output)

    def test_create_api_call_daily_aggregate_call_type_wrong_params(self):
        query = {
            'date': str(date.today()), ## Gets the current date
            'adjusted': 'true'
        }
        params = ['date']

        with self.assertRaises(ValueError):
            self.get_data_class._create_api_call(query, params, self.call_type)

    def test_create_api_call_daily_aggregate_call_type_wrong_query(self):
        query = {
            'date': str(date.today()), ## Gets the current date
        }
        params = ['date', 'adjusted']

        with self.assertRaises(KeyError):
            self.get_data_class._create_api_call(query, params, self.call_type)

    ## Tests for when call_type = time_period_aggregate
    def test_create_api_call_time_period_aggregate(self):
        query = {
            'ticker': 'APPL',
            'range': '1',
            'timespan': 'day',
            'start_date': str(date.today() - timedelta(days=7)),
            'end_date': str(date.today()),
            'adjusted': 'true',
            'sort': 'asc',
            'limit': '120'
        }
        params = ['ticker', 'range', 'timespan', 'start_date', 'end_date', 'adjusted', 'sort', 'limit']

        real_output = self.get_data_class._create_api_call(query, params, self.call_type)
        expected_output = f'https://api.polygon.io/v2/aggs/ticker/APPL/range/1/day/{str(date.today() - timedelta(days=7))}/{str(date.today())}?adjusted=true&limit=120&apiKey=Sl2fNRJQfFgu9Z6jS5vNmVzJ9zvnM1hP'

        self.assertEqual(real_output, expected_output)

    def test_create_api_call_time_period_aggregate_wrong_params(self):
        query = {
            'ticker': 'APPL',
            'range': '1',
            'timespan': 'day',
            'start_date': str(date.today() - timedelta(days=7)),
            'end_date': str(date.today()),
            'adjusted': 'true',
            'sort': 'asc',
            'limit': '120'
        }
        params = ['ticker', 'range', 'timespan', 'start_date', 'end_date', 'adjusted']

        with self.assertRaises(ValueError):
            self.get_data_class._create_api_call(query, params, self.call_type)

    
    def test_create_api_call_time_period_aggregate_wrong_query(self):
        query = {
            'ticker': 'APPL',
            'range': '1',
            'timespan': 'day',
            'start_date': str(date.today() - timedelta(days=7)),
            'end_date': str(date.today()),
            'adjusted': 'true',
        }
        params = ['ticker', 'range', 'timespan', 'start_date', 'end_date', 'adjusted', 'sort', 'limit']

        with self.assertRaises(KeyError):
            self.get_data_class._create_api_call(query, params, self.call_type)

    



    




if __name__ == '__main__':
    unittest.main(verbosity=2)