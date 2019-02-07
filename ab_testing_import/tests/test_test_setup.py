from ab_testing_import.test_setup import *

import unittest

import pandas as pd


class TestDailyRollup(unittest.TestCase):

    def setUp(self):
        self.base_df = pd.read_csv('tests/test_event_data.csv', parse_dates=['DT'])
        self.test_obj = ABTest('tests/test_config.yaml')

    @unittest.skip
    def test_daily_rollup(self):
        self.test_obj.daily_rollup(self.base_df)
        self.base_df

    def test_rolling_stats(self):
        print(self.test_obj.rolling_stats(self.base_df))
        


if __name__ == '__main__':
    unittest.main()
