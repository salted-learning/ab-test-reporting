from contextlib import contextmanager
import os
import sqlite3
import pkg_resources

import pandas as pd
import numpy as np


DATABASE_FILE = 'ab_testing_data.db'
TEST_LIST_TABLE = 'ab_tests'
DAILY_ROLLUP_EXT = '_daily'
STATS_EXT = '_rolling_stats'
CREATE_TABLE_FILENAME = pkg_resources.resource_filename(__name__, 'res/create_test_list_table.sql')

@contextmanager
def sqlite_connection(filename):
    conn = sqlite3.connect(filename)
    yield conn
    conn.close()


def sqlify_test_name(test_name):
    return test_name.replace(' ', '_')


def _verify_test_in_list(test_name, config_file, description):
    """Checks whether the test is currently in the list
    of tests and active. If not, it will add or activate
    the test.

    Args:
        test_name (str): Name of the test
    """

    # first check that the ab_tests table exists and create if not
    check_query = """select name from sqlite_master where type='table' and name = ?"""
    with sqlite_connection(DATABASE_FILE) as conn:
        cur = conn.cursor()
        cur.execute(check_query, (TEST_LIST_TABLE,))
        if cur.fetchone() is None: # missing table
            _create_test_list_table()
    
    test_name = sqlify_test_name(test_name)
    config_file = os.path.basename(config_file)
    check_query = 'select count(*) from {} where test_name = ?'.format(TEST_LIST_TABLE)

    with sqlite_connection(DATABASE_FILE) as conn:
        cur = conn.cursor()
        cur.execute(check_query, (test_name,))
        result = cur.fetchone()[0]

    if result == 0:
        # test does not exist yet
        insert_query = """
        insert into {} (test_name, active_fg, config_file, description)
        values (?, 'Y', ?, ?)
        """.format(TEST_LIST_TABLE)

        with sqlite_connection(DATABASE_FILE) as conn:
            conn.execute(insert_query, (test_name, config_file, description))
            conn.commit()
    else:
        # test exists, just make sure it's active
        update_query = """
        update {} set active_fg = 'Y', config_file = ?, description = ?
        where test_name = ?
        """.format(TEST_LIST_TABLE)

        with sqlite_connection(DATABASE_FILE) as conn:
            conn.execute(update_query, (config_file, description, test_name))
            conn.commit()

def _create_test_list_table():
    """Creates the test list table"""
    with open(CREATE_TABLE_FILENAME, 'r') as f:
        query = f.read()
    with sqlite_connection(DATABASE_FILE) as conn:
        conn.execute(query)
        conn.commit()
    

def deactivate_test(test_name):
    """Sets the test_name to inactive if it exists in the test list.

    TODO: Decide whether, when a test is deactivated, to drop its corresponding tables.
    My current thought is that "deactivate" should set active_fg = 'N' and keep all
    tables, while "delete" should remove the tables and the record from the test list.

    Args:
        test_name (str): The name of the test    
    """
    test_name = sqlify_test_name(test_name)
    query = "update {} set active_fg = 'N' where test_name = ?".format(TEST_LIST_TABLE)

    with sqlite_connection(DATABASE_FILE) as conn:
        cur = conn.cursor()
        cur.execute(query, (test_name,))
        conn.commit()

        
def _insert_table(df, table_name):
    """Creates or replaces the table_name with the data
    in df.

    Args:
        df (DataFrame): The data to create/replace the table with
        table_name (str): The name of table    
    """
    with sqlite_connection(DATABASE_FILE) as conn:
        df.to_sql(table_name, conn, if_exists='replace', index=False,
                  chunksize=5000)
        
    
def insert_daily_rollup_data(df, test):
    """Creates or replaces the daily rollup table for test_name.

    This method will create or replace a table with name (test_name +
    DAILY_ROLLUP_EXT) using the data in df. It also checks that the
    input data matches the expected schema: one date column named DT,
    one test cell column named TEST_CELL, and the rest of the columns
    contain numeric data.
    
    Args:
        df (DataFrame): The data to create/replace the table with
        test_name (str): The name of the test
    """
    test_name = sqlify_test_name(test.test_name)
    # Check that the data conforms to the expected schema:
    #  - includes a DT column of type datetime
    #  - includes a TEST_CELL column of type string
    #  - all other columns are numeric
    required_columns = ['DT', 'TEST_CELL']
    for col in required_columns:
        if col not in df.columns:
            raise KeyError('{} column not found in rollup data'.format(col))

    other_columns = [i for i in df.columns if i not in required_columns]
    for col in other_columns:
        if not np.issubdtype(df[col].dtype, np.number):
            raise TypeError('{} column should be numeric, found {}'.format(col, df[col].dtype))

        
    _verify_test_in_list(test_name, test.config_file, test.description)

    table_name = test_name + DAILY_ROLLUP_EXT
    _insert_table(df, table_name)


def insert_rolling_stats_data(df, test):
    """Creates or replaces the rolling stats table for test_name.

    This method will create or replace a table with name (test_name +
    STATS_EXT) using the data in df. It also checks that the input
    data conforms to the expected schema: (DT date, TEST_CELL string,
    METRIC_NAME string, METRIC_VALUE number, ...)
    

    Args:
        df (DataFrame): The data to create/replace the table with
        test_name (str): The name of the test
    """
    test_name = sqlify_test_name(test.test_name)
    _verify_test_in_list(test_name, test.config_file, test.description)

    # TODO: Define expected schema and add checks
    
    table_name = test_name + STATS_EXT
    _insert_table(df, table_name)


if __name__ == '__main__':
    # _verify_test_in_list('test1')
    df = pd.read_csv('../Automate_AB_Testing.csv')
    df.drop('Unnamed: 0', axis=1, inplace=True)
    insert_daily_rollup_data(df, 'test1')
