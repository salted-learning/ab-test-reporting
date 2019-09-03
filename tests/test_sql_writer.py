from ab_testing_import.sql_writer import *

import unittest
import sqlite3

import pandas as pd


class TestInsertRollupData(unittest.TestCase):

    def setUp(self):
        self.base_df = pd.read_csv('tests/test_rollup_data.csv', parse_dates=['DT'])
        self.test_name = 'fake_test_1'

    def get_database_path(self):
        return DATABASE_FILE

    def test_fails_on_missing_date(self):
        df = self.base_df.drop('DT', axis=1)

        with self.assertRaises(KeyError):
            insert_daily_rollup_data(df, self.test_name)

    def test_fails_on_missing_test_cell(self):
        df = self.base_df.drop('TEST_CELL', axis=1)

        with self.assertRaises(KeyError):
            insert_daily_rollup_data(df, self.test_name)

    def test_fails_on_extra_str_col(self):
        df = self.base_df.copy()
        df['extra_string'] = 'not a test cell'

        with self.assertRaises(TypeError):
            insert_daily_rollup_data(df, self.test_name)

    def test_creates_table(self):
        insert_daily_rollup_data(self.base_df, self.test_name)

        with sqlite_connection(self.get_database_path()) as conn:
            query = "select count(*) from sqlite_master where type = 'table' and name = ?"
            table_name = self.test_name + DAILY_ROLLUP_EXT
            cur = conn.cursor()
            cur.execute(query, (table_name,))
            result = cur.fetchone()[0]

        self.assertEqual(result, 1)
    
    def test_inserts_all_data(self):
        insert_daily_rollup_data(self.base_df, self.test_name)

        with sqlite_connection(self.get_database_path()) as conn:
            table_name = self.test_name + DAILY_ROLLUP_EXT
            query = "select * from {}".format(table_name)

            df = pd.read_sql(query, conn)

        self.assertEqual(set(self.base_df.columns), set(df.columns))
        self.assertEqual(self.base_df.shape, df.shape)
        

    def test_sets_test_active(self):
        insert_daily_rollup_data(self.base_df, self.test_name)

        with sqlite_connection(self.get_database_path()) as conn:
            query = "select count(*) from {} where test_name = ? and active_fg = 'Y'".format(TEST_LIST_TABLE)
            cur = conn.cursor()
            cur.execute(query, (self.test_name,))
            result = cur.fetchone()[0]

        self.assertEqual(result, 1)

    def tearDown(self):
        # remove table from db and remove test from list
        with sqlite_connection(self.get_database_path()) as conn:
            query = "delete from {} where test_name = ?".format(TEST_LIST_TABLE)
            conn.execute(query, (self.test_name,))

            table_name = self.test_name + DAILY_ROLLUP_EXT
            query = "drop table if exists {}".format(table_name)
            conn.execute(query)

            conn.commit()


if __name__ == '__main__':
    unittest.main()
