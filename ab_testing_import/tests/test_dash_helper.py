from ab_testing_import.dash_data_helper import *
import ab_testing_import.sql_writer as sw

import unittest

import pandas as pd


class TestGetActiveTestList(unittest.TestCase):

    def setUp(self):
        self.test_name = 'test1'
        self.helper = DashDataHelper()
        # add a test to the list
        sw._verify_test_in_list(self.test_name)

    def test_returns_test_lists(self):
        test_list = self.helper.get_active_test_list()
        test_list = test_list.values
        self.assertIn(self.test_name, test_list)

    def tearDown(self):
        # remove the test from the list
        sw.deactivate_test(self.test_name)


class TestGetDailyRollup(unittest.TestCase):

    def setUp(self):
        # create rollup table
        self.test_name = 'test1'
        self.helper = DashDataHelper()
        self.df = pd.read_csv('tests/test_rollup_data.csv', parse_dates=['DT'])
        sw.insert_daily_rollup_data(self.df, self.test_name)

    def test_returns_table(self):
        df = self.helper.get_daily_rollup(self.test_name)
        self.assertIsInstance(df, pd.DataFrame)

    def test_table_matches_df(self):
        df = self.helper.get_daily_rollup(self.test_name)
        self.assertEqual(df.shape, self.df.shape)

    def test_data_types(self):
        df = self.helper.get_daily_rollup(self.test_name)

        self.assertTrue(pd.api.types.is_string_dtype(df['TEST_CELL']))
        self.assertTrue(pd.api.types.is_datetime64_any_dtype(df['DT']))
        for col in df.columns:
            if col not in ['TEST_CELL', 'DT']:
                self.assertTrue(pd.api.types.is_numeric_dtype(df[col]))

    def tearDown(self):
        # remove rollup table
        sw.deactivate_test(self.test_name)
        table_name = self.test_name + sw.DAILY_ROLLUP_EXT
        query = "drop table {}".format(table_name)
        with sw.sqlite_connection(sw.DATABASE_FILE) as conn:
            conn.execute(query)
            conn.commit()


class TestGetRollingStats(unittest.TestCase):

    def setUp(self):
        # read in test stats file, create table
        pass

    def test_returns_table(self):
        pass

    def test_table_matches_df(self):
        pass

    def test_data_types(self):
        pass

    def tearDown(self):
        # remove stats table
        pass


if __name__ == '__main__':
    unittest.main()
