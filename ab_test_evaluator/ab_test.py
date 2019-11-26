import logging

import yaml
import pandas as pd
import numpy as np

from . import sql_writer
from .stats import ContinuousTestEval, BinaryTestEval

logger = logging.getLogger(__name__)


def _clean_dict(config):
    if isinstance(config, str):
        return config.strip()
    elif isinstance(config, dict):
        for k, v in config.items():
            config[k] = _clean_dict(v)
        return config
    elif isinstance(config, list):
        for i in range(len(config)):
            config[i] = _clean_dict(config[i])
        return config
    else:
        return config


class ABTest(object):

    def __init__(self, df, config):
        """Create the test using the event-level df and the parsed contents of the
        config file. When subclassing, be sure to call this init method as well."""
        self.df = df
        self.config = config

        # validate the config file format
        logger.info("Parsing config file")
        assert 'test_name' in config
        assert 'description' in config
        assert 'metrics' in config
        for metric, metric_dict in config['metrics'].items():
            assert 'type' in metric_dict
            assert 'function' in metric_dict
            assert metric_dict['type'] in ('continuous', 'binary')

        # required fields
        self.test_name = config['test_name']
        self.description = config['description']
        self.metric_definitions = {k: self._get_metric_function(v)
                                   for k, v in config['metrics'].items()}

        # optional fields w/ defaults
        self.date_field = config.get('date_field', 'DT')
        self.test_cell_field = config.get('test_cell_field', 'TEST_CELL')

        
    def _get_metric_function(self, metric_dict):
        """Converts the metric string in the config file to a function.

        Takes the metric defined in the config file. Assumes the metric is defined as
        [summed metric 1] / [summed metric 2]. The summed metrics must either match a
        column name returned from the SQL or be "count" ("count" will be used to average
        a column). Returns a dictionary with 3 values: the function to apply to the
        DataFrame, the type of aggregation used (either ratio or average, where average
        means the denominator = "count"), the column name of the numerator, and the column
        name of the denominator.

        Args:
            metric_dict (dict): The metric dict as it's written in the YAML config file
        Returns:
            dict: 4 keys, {'function': the function which can be applied to the DataFrame,
                  'type': either 'ratio' or 'average' for the type of aggregation,
                  'numerator_column': the column name of the numerator for this metric,
                  'denominator_column': the column name of the denominator for this metric}
        """
        data = {}
        data['type'] = metric_dict['type']

        # parse out the function
        tok = metric_dict['function'].split('/')
        tok = [t.strip().upper() for t in tok]
        data['numerator_column'] = tok[0]
        if len(tok) > 1:
            data['denominator_column'] = tok[1]
            data['function'] = lambda x: x[tok[0]] / x[tok[1]] if x[tok[1]] > 0 else None
        else: # this is an average
            data['denominator_column'] = 'COUNT'
            data['function'] = lambda x: x[tok[0]] / x['COUNT'] if x['COUNT'] > 0 else None

        return data
        

    def refresh_test_data(self):
        """Performs a complete refresh of the test's data using the DataFrame.
        """
        # standardize the column names and add count
        self.df['COUNT'] = 1
        self.df = self.df.rename({self.date_field: 'DT',
                                  self.test_cell_field: 'TEST_CELL'},
                                 axis=1)
        self.df['DT'] = pd.to_datetime(self.df['DT'])

        # get test cells, check that there's only 2 now
        self.test_cells = self.df['TEST_CELL'].unique()
        assert self.test_cells.shape == (2,)

        logger.info('Creating daily rollup')
        daily_df = self.daily_rollup()
        sql_writer.insert_daily_rollup_data(daily_df, self)

        logger.info('Creating rolling stats')
        stats_df = self.rolling_stats()
        sql_writer.insert_rolling_stats_data(stats_df, self)


    def daily_rollup(self):
        """Turns the event-level DataFrame into a daily rollup.
        
        Takes the event-level DataFrame and the metric definitions and
        returns a daily rollup DataFrame. The daily rollup will have
        columns for DT, TEST_CELL, and every metric in the metric
        definitions.
        
        Args:
            df (DataFrame): The event-level DataFrame
        Returns:
            DataFrame: The daily rollup DataFrame
        """
        # copy dataframe so we don't modify the original
        df = self.df.copy()

        # grouping by day, so trunc
        df['DT'] = df['DT'].dt.floor('d')
        df = df.groupby(['DT', 'TEST_CELL']).sum().reset_index()
        
        for k, v in self.metric_definitions.items():
            df[k] = df.apply(v['function'], axis=1)
            
        # Limit to metrics and DT/TEST_CELL
        columns_to_keep = [k for k in self.metric_definitions.keys()]
        columns_to_keep.append('DT')
        columns_to_keep.append('TEST_CELL')
        df = df[columns_to_keep]

        return df


    def rolling_stats(self):
        """Turns the event-level DataFrame into a rolling stat table.

        Takes the event-level DataFrame and turns it into a DataFrame which has cumulative
        stats per day. The resulting table will have columns for DT, TEST_CELL, METRIC_NAME, 
        METRIC_VALUE, P_VALUE, LOWER_CI, and UPPER_CI. The granularity of the result is one
        row per day*test cell*metric.

        Args:
            df (DataFrame): The event-level DataFrame
        Returns:
            DataFrame: The rolling stat DataFrame, with one row per day per test
                       cell per metric
        """
        # don't modify the original
        df = self.df.copy()

        df['DT'] = df['DT'].dt.floor('d')
        start_date = df['DT'].min()
        end_date = df['DT'].max()
        end_dates = np.arange(start_date, end_date, np.timedelta64(1, 'D'))

        df_list = []
        for date in end_dates:
            run = df[df['DT'] <= date]
            df_list.append(self._run_stats(run))
            
        return pd.concat(df_list).reset_index(drop=True)
        

    def _run_stats(self, df):
        """Creates a stats table from the event-level DataFrame for a single time range.

        Args:
            df (DataFrame): The event-level DataFrame, limited to the relevant date range
        Returns:
            DataFrame: The rolling stat DataFrame, with DT set to the max DT of the event-
                       level DataFrame
        """
        binary_metrics = []
        cont_metrics = []

        for metric, m_dict in self.metric_definitions.items():
            if m_dict['type'] == 'continuous':
                cont_metrics.append(metric)
            else:
                binary_metrics.append(metric)
                    
        metric_dfs = []
        for metric in binary_metrics:
            metric_dfs.append(self._run_binary_stat(df, metric))
        for metric in cont_metrics:
            metric_dfs.append(self._run_cont_stat(df, metric))
        
        output = pd.concat(metric_dfs).reset_index(drop=True)
        # set the date to the max date, since this is a cumulative calculation
        output['DT'] = df['DT'].max()
        return output


    def _run_binary_stat(self, df, metric):
        # decide later how to actually define which cell is test and
        # which is control
        test = self.test_cells[0]
        ctrl = self.test_cells[1]

        numerator = self.metric_definitions[metric]['numerator_column']
        denominator = self.metric_definitions[metric]['denominator_column']

        data = {'TEST_CELL': [],
                'METRIC_NAME': [],
                'METRIC_VALUE': []}
        trial_data = {}
        for cell in [test, ctrl]:
            successes = df[df['TEST_CELL'] == cell][numerator].sum()
            trials = df[df['TEST_CELL'] == cell][denominator].sum()
            failures = trials - successes

            # save data
            data['TEST_CELL'].append(cell)
            data['METRIC_NAME'].append(metric)
            data['METRIC_VALUE'].append(successes / trials)
            
            # convert to 0s and 1s for BinaryTestEval
            trial_data[cell] = np.concatenate((np.ones(successes), np.zeros(failures)))

        # send to stats
        b = BinaryTestEval(trial_data[test], trial_data[ctrl])
        # calculate p-val, append to both test and control rows
        p_val = b.binary_pval()
        data['P_VALUE'] = [p_val, p_val]
        lower, upper = b.binary_ci()
        # calculate CIs, append to both
        data['LOWER_CI'] = [lower, lower]
        data['UPPER_CI'] = [upper, upper]

        return pd.DataFrame(data)

    def _run_cont_stat(self, df, metric):
        # assumes denominator is COUNT
        test = self.test_cells[0]
        ctrl = self.test_cells[1]

        numerator = self.metric_definitions[metric]['numerator_column']

        data = {'TEST_CELL': [],
                'METRIC_NAME': [],
                'METRIC_VALUE': []}
        trial_data = {}
        for cell in [test, ctrl]:
            individ_events = df[df['TEST_CELL'] == cell][numerator]
            # save data
            data['TEST_CELL'].append(cell)
            data['METRIC_NAME'].append(metric)
            data['METRIC_VALUE'].append(individ_events.mean())
            
            # convert to 0s and 1s for BinaryTestEval
            trial_data[cell] = individ_events

        # send to stats
        b = ContinuousTestEval(trial_data[test], trial_data[ctrl])
        # calculate p-val, append to both test and control rows
        p_val = b.continuous_pval()
        data['P_VALUE'] = [p_val, p_val]
        lower, upper = b.mean_diff_continuous_ci()
        # calculate CIs, append to both
        data['LOWER_CI'] = [lower, lower]
        data['UPPER_CI'] = [upper, upper]

        return pd.DataFrame(data)
        


class NewABTestFromPath(ABTest):

    def __init__(self, config_file, csv_file):
        """Creates a test defined in config_file using data from csv_file.

        Args:
            config_file (str): The filepath of the YAML config file for this test
            csv_file (str): The filepath of the CSV file containing the event-level data for this test
        """
        with open(config_file) as f:
            config = yaml.safe_load(f.read())

        # remove newlines from config
        config = _clean_dict(config)

        df = pd.read_csv(csv_file)

        super(NewABTestFromPath, self).__init__(df, config)



class ExistingABTestFromDB(ABTest):

    def __init__(self, df, test_name):
        """Refresh an existing test. Load the config from the DB by using
        the test name."""

        config = sql_writer.get_config_for_test(test_name)
        super(ExistingABTestFromDB, self).__init__(df, config)
