import sqlite3
import pandas as pd

from . import sql_writer


class DashDataHelper(object):

    def __init__(self, db_path=sql_writer.DATABASE_FILE):
        self.db_path = db_path

    def get_active_test_list(self):
        query = "select * from {} where active_fg = 'Y'".format(sql_writer.TEST_LIST_TABLE)
        with sql_writer.sqlite_connection(self.db_path) as conn:
            df = pd.read_sql(query, conn)
        return df

    def get_daily_rollup(self, test_name):
        # At some point, we might want to cache test data. That way, we only
        # need to query the DB when it requests a test we've never seen. The 
        # trick would be to figure out how to decide when to refresh the cache.
        table_name = test_name + sql_writer.DAILY_ROLLUP_EXT
        query = "select * from {}".format(table_name)
        with sql_writer.sqlite_connection(self.db_path) as conn:
            df = pd.read_sql(query, conn, parse_dates=['DT'])
        return df

    def get_rolling_stats(self, test_name):
        # Same caching possibility as above
        table_name = test_name + sql_writer.STATS_EXT
        query = "select * from {}".format(table_name)
        with sql_writer.sqlite_connection(self.db_path) as conn:
            df = pd.read_sql(query, conn, parse_dates=['DT'])
        return df


    
