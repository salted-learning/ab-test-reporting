import pandas as pd

from ab_testing_import.test_setup import *


def get_data_input_to_stats():
    df = pd.read_csv('tests/test_event_data.csv', parse_dates=['DT'])
    test_obj = ABTest('tests/test_config.yaml')

    return df, test_obj.metric_definitions
