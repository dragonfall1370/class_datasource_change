# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)
#
# loading configuration
cf = configparser.RawConfigParser()
cf.read('_edenscott_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
fr = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    ddbconn = psycopg2.connect(host=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), port=fr.get('port'))
    ddbconn.set_client_encoding('UTF8')

    # mylog.info('position_candidate is being deleted')
    # vincere_common.clean_job_application(ddbconn)
    # mylog.info('position_candidate is deleted')
    #
    mylog.info('candidate is being deleted')
    vincere_custom_migration.clean_candidate(ddbconn)
    mylog.info('candidate is deleted')

    # mylog.info('job is being deleted')
    # vincere_custom_migration.clean_job(ddbconn)
    # mylog.info('job is deleted')

    # mylog.info('contact is being deleted')
    # vincere_common.clean_contact(ddbconn)
    # mylog.info('contact is deleted')
    #
    # mylog.info('company is being deleted')
    # vincere_common.clean_company(ddbconn)
    # mylog.info('company is deleted')
    #
    # mylog.info('recent_record is being deleted')
    # vincere_common.clean_recent_record(ddbconn)
    # mylog.info('recent_record is deleted')
    #
    # mylog.info('bulk_upload is being deleted')
    # vincere_common.clean_bulk_upload(ddbconn)
    # mylog.info('bulk_upload is deleted')
    #
    # mylog.info('unsupper_users are being deleted')
    # vincere_common.clean_unsupper_users(ddbconn)
    # mylog.info('unsupper_users are deleted')
    #
    # mylog.info('candidate_gdpr_compliance are being deleted')
    # vincere_custom_migration.clean_candidate_gdpr_compliance(ddbconn)
    # mylog.info('candidate_gdpr_compliance are deleted')
