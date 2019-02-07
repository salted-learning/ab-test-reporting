import os
import datetime
import logging
from logging.handlers import SMTPHandler

import yaml
import cx_Oracle

from ab_testing_import import sql_writer
from ab_testing_import import DashDataHelper
from ab_testing_import import ABTest


logger = logging.getLogger(__name__)

CONFIG_FOLDER = '../config'
LOG_FOLDER = '../log'

EMAIL_SERVER = 'webmail.servicemagic.com'
EMAIL_PORT = 25
EMAIL_FROM = 'Product Intelligence Team <michael.schulte@homeadvisor.com>'
EMAIL_TO = ['klaus.sade@homeadvisor.com', 'michael.schulte@homeadvisor.com',
            'deemah.alkheraiji@homeadvisor.com']


def setup_logging():
    """Setup the logging: detailed logs and error-triggered emails."""
    fname = '{}.log'.format(datetime.date.today().strftime('%Y%m%d'))
    filepath = os.path.join(LOG_FOLDER, fname)
    config = '%(asctime)s||%(name)s||%(threadName)s||%(levelname)s||%(message)s'
    logging.basicConfig(filename=filepath, level=logging.INFO, format=config)

    # Set email handler at root level to send for any exception
    root_logger = logging.getLogger()
    handler = SMTPHandler((EMAIL_SERVER, EMAIL_PORT),
                          EMAIL_FROM, EMAIL_TO,
                          'Error in AB Testing Process')
    handler.setLevel(logging.ERROR)
    root_logger.addHandler(handler)
    

def run():
    # read all files from config folder
    files = get_active_config_files()
    logger.info("Got all active config files")
    deactivate_tests(files)
    for f in files:
        try:
            do_refresh(f)
        except Exception as e: # Wrap in general except so error doesn't impact other tests
            logger.exception('Error refreshing test {}'.format(f))

        
def do_refresh(f):
    path_to_file = os.path.join(CONFIG_FOLDER, f)
    
    # try/except for config file errors; send email on error
    try:
        a = ABTest(path_to_file)
    except yaml.parser.ParserError as e:
        logger.exception('Malformed config file: {}\n\n'.format(f))
        return
    
    # try/except for query kill; send email on error
    try:
        a.refresh_test_data()
    except cx_Oracle.DatabaseError as e:
        logger.exception('Encountered database error while executing SQL for {}\n\n'.format(f))


def get_active_config_files():
    files = []
    for f in os.listdir(CONFIG_FOLDER):
        path_to_file = os.path.join(CONFIG_FOLDER, f)
        if os.path.isfile(path_to_file):
            logger.info("Found active config file {}".format(f))
            files.append(f)
    return files


def deactivate_tests(active_files):
    helper = DashDataHelper()
    active_tests = helper.get_active_test_list()
    tests_to_deactivate = active_tests[active_tests.apply(lambda x: x['config_file'] not in active_files,
                                                          axis=1)]
    for test in tests_to_deactivate['test_name'].values:
        logger.info("Deactivating test {}; not seen in config folder".format(test))
        sql_writer.deactivate_test(test)


if __name__ == '__main__':
    setup_logging()

    # Wrap all exceptions to log to file, send alert
    try:
        run()
    except Exception as e:
        logger.exception('Exception in AB Testing Setup')
        raise
