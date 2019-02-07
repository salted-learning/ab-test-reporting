import yaml
import pandas as pd
import numpy as np
# need to figure out a work-around here
# import ha_tools
import statsmodels.api as sm
import logging
import math
import scipy.stats as stats

from . import sql_writer


logger = logging.getLogger(__name__)

DB_USERNAME = 'dalkheraiji'
DB_PASSWORD = 'ZXCVBNM0987'


class ABTest(object):

    def __init__(self, config_file):
        """Creates a test defined in config_file

        Args:
            config_file (str): The filepath of the YAML config file for this test
        """

        logger.info("Parsing config file {}".format(config_file))
        with open(config_file) as f:
            y = yaml.load(f.read())

        self.config_file = config_file
        self.test_name = y['test_name']
        self.description = y['description']
        self.sql = y['sql']
        self.refresh_fg = y['refresh_data']
        self.metric_definitions = {k: self._get_metric_function(v)
                                   for k, v in y['metrics'].items()}
        

    def _get_metric_function(self, metric_string):
        """Converts the metric string in the config file to a function.

        Takes the metric defined in the config file. Assumes the metric is defined as
        [summed metric 1] / [summed metric 2]. The summed metrics must either match a
        column name returned from the SQL or be "count" ("count" will be used to average
        a column). Returns a dictionary with 3 values: the function to apply to the
        DataFrame, the type of aggregation used (either ratio or average, where average
        means the denominator = "count"), the column name of the numerator, and the column
        name of the denominator.

        Args:
            metric_string (str): The metric string as it's written in the YAML config file
        Returns:
            dict: 4 keys, {'function': the function which can be applied to the DataFrame,
                  'type': either 'ratio' or 'average' for the type of aggregation,
                  'numerator_column': the column name of the numerator for this metric,
                  'denominator_column': the column name of the denominator for this metric}
        """
        tok = metric_string.split('/')
        tok = [t.strip().upper() for t in tok]

        data = {}
        if tok[-1] == 'COUNT':
            data['type'] = 'average'
        else:
            data['type'] = 'ratio'

        data['numerator_column'] = tok[0]
        data['denominator_column'] = tok[1]

        data['function'] = lambda x: x[tok[0]] / x[tok[1]] if x[tok[1]] > 0 else None

        return data


    def refresh_test_data(self):
        """Performs a complete refresh of the test's data.

        The SQL gets run against RPT01, the test gets activated, the daily
        rollup gets dumped out to the SQLite database, and the stats are
        run and also dumped out.        
        """
        if self.refresh_fg == 'N':
            return # don't refresh, based on setting in config file

        logger.info('Running SQL')
        df = self.fetch_event_data()

        logger.info('Creating daily rollup')
        daily_df = self.daily_rollup(df)
        sql_writer.insert_daily_rollup_data(daily_df, self)

        logger.info('Creating rolling stats')
        stats_df = self.rolling_stats(df)
        sql_writer.insert_rolling_stats_data(stats_df, self)

    
    def fetch_event_data(self):
        """Runs SQL in RPT01 and returns a DataFrame
        
        Args:
            sql (str): The SQL to be run in RPT01
        Returns:
            DataFrame: The table returned from RPT01
        """
        with ha_tools.db_connection(DB_USERNAME, DB_PASSWORD) as conn:
            df = pd.read_sql(self.sql, conn)
            
        # Validate expected columns exist and data types are correct.
        # Ideally this step would be done before actually running the SQL,
        # similar to how Tableau loads metadata first, but I couldn't find
        # documentation on how to do that.
        required_columns = ['DT', 'TEST_CELL']
        for col in required_columns:
            if col not in df.columns:
                raise KeyError('{} column not found in event data'.format(col))
        other_columns = [i for i in df.columns if i not in required_columns]
        for col in other_columns:
            if not np.issubdtype(df[col].dtype, np.number):
                raise TypeError('{} column should be numeric in event data, found {}'.format(col, df[col].dtype))

        df['COUNT'] = 1

        return df


    def daily_rollup(self, df):
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
        df = df.copy()
        df['COUNT'] = 1
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


    def rolling_stats(self, df):
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

        for k, v in self.metric_definitions.items():
            if (v['type']) == 'ratio':
                binary_metrics.append(k)
            else:
                if len(df[v['numerator_column']].unique()) == 2:
                    binary_metrics.append(k)
                else:
                    cont_metrics.append(k)
                    
        metric_dfs = []
        for metric in binary_metrics:
            metric_dfs.append(self._prop_test(df, metric))
        for metric in cont_metrics:
            metric_dfs.append(self._bootstrapped_ci(df, metric))
        
        output = pd.concat(metric_dfs).reset_index(drop=True)
        output['DT'] = df['DT'].max()
        return output


    def _prop_test(self, df, metric):
        """Given a single binary metric, returns the stats for each test cell.

        Args:
            df (DataFrame): The event-level DataFrame
        Returns:
            DataFrame: A stats table with one row per test cell
        """
        metric_definition = self.metric_definitions[metric]
        test_cells = df['TEST_CELL'].unique()
        num = metric_definition['numerator_column']
        denom = metric_definition['denominator_column']
        
        df = df.groupby('TEST_CELL').sum()
        df[metric] = df.apply(metric_definition['function'], axis=1)
        z_score, p_value = sm.stats.proportions_ztest([df.loc[test_cells[0], num], df.loc[test_cells[1], num]],
                                                      [df.loc[test_cells[0], denom], df.loc[test_cells[1], denom]])

        rows = []
        for test_cell in test_cells:
            p = df.loc[test_cell, num] /df.loc[test_cell, denom]
            se = math.sqrt(p*(1-p)/df.loc[test_cell, denom])
            upper = p + (1.96 * se)
            lower = p - (1.96 * se)
        
            data = {'TEST_CELL': test_cell, 'METRIC_NAME' : metric, 'METRIC_VALUE' : df.loc[test_cell, metric],
                    'P_VALUE': p_value, 'UPPER_CI': upper, 'LOWER_CI' : lower}
            rows.append(data)
        
        return pd.DataFrame(rows)

    def _bootstrapped_ci(self, df, column_name, ci=95):
        df = df.copy()
        old_col_name = self.metric_definitions[column_name]['numerator_column']
        df = df[['TEST_CELL', old_col_name]]
        
        df_list1 = []
        
        for col in df['TEST_CELL'].unique():
            sample_means = []
            
            temp_df = df.loc[df['TEST_CELL'] == col]
            
            for i in range(1000):
                sample = temp_df[old_col_name].sample(temp_df.shape[0], replace = True)
                sample_means.append(sample.mean())
        
            alpha = 100 - ci

            average = temp_df[old_col_name].mean()
            
            lb = np.percentile(sample_means, alpha / 2)
            ub = np.percentile(sample_means, 100 - (alpha / 2))
            a = pd.DataFrame([[col, column_name, average, ub, lb, None]],
                             columns=['TEST_CELL','METRIC_NAME','METRIC_VALUE',
                                      'UPPER_CI','LOWER_CI', 'P_VALUE'])
            df_list1.append(a)
        out_df = pd.concat(df_list1)

        x = df.loc[df['TEST_CELL'] == df['TEST_CELL'].unique()[0]][old_col_name]
        y = df.loc[df['TEST_CELL'] == df['TEST_CELL'].unique()[1]][old_col_name]
        out_df['P_VALUE'] = stats.ttest_ind(x, y)[1]
        
        return out_df



          
    
if __name__ == '__main__':
    t = ABTest('tests/test_config.yaml')
        

    
