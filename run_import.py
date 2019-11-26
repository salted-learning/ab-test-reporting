import os
import datetime
import logging
import argparse

import yaml

from ab_test_evaluator import NewABTestFromPath


def _setup_args():
    desc = 'Load an AB test by passing a config file and a CSV file'
    parser = argparse.ArgumentParser(description=desc)
    parser.add_argument('--config', dest='config_file', type=str,
                        nargs=1, default='examples/sample_config.yml',
                        help='the path to the config file (default: examples/sample_config.yml)')
    parser.add_argument('--csv', dest='csv_file', type=str,
                        nargs=1, default='examples/sample_data.csv',
                        help='the path to the event-level CSV file (default: examples/sample_data.csv)')
    args = parser.parse_args()
    return args.config_file, args.csv_file


def import_test_data(config_file, csv_file):
    a = NewABTestFromPath(config_file, csv_file)
    a.refresh_test_data()


if __name__ == '__main__':
    config, csv = _setup_args()
    print(f"Using {config} as config file and {csv} as CSV file")
    import_test_data(config, csv)
