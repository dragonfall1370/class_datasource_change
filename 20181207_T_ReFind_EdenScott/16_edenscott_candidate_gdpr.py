# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import pymssql
import datetime
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
from common import thread_pool as thread_pool
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
locator = os.path.join(data_folder, 'locator')
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
pathlib.Path(locator).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    logger = log.get_info_logger("edenscott.log")
    sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')

    df_raw = pd.read_csv(os.path.join(data_input, 'candidate.csv'), dtype=dtype_cand, parse_dates=dtype_cand_date_parse)
    df = df_raw[['CAnum', 'CAGDPRholdData', 'CAGDPRsendClients', 'CAGDPRjobs', 'CAGDPRupdates', 'CAGDPRrestricted', 'CAGDPRobtained', 'CAcons', ]]
    df_user_options = pd.read_csv(os.path.join(data_input, 'user_options.csv'))
    df_user_options_st = df_user_options[['UOemail', 'UOloginname', ]]
    df_vin_users = pd.read_sql("select id, email from user_account", ddbconn)
    df['CAcons'] = df['CAcons'].str.strip()
    df_user_options_st['UOloginname'] = df_user_options_st['UOloginname'].str.strip()
    df = df.merge(df_user_options_st, left_on='CAcons', right_on='UOloginname', how='left')
    df = df.merge(df_vin_users, left_on='UOemail', right_on='email', how='left')

    df['external_id'] = [str(x).strip() for x in df['CAnum']]
    df['exercise_right'] = 3
    df['request_through'] = 6
    df['request_through_date'] = [datetime.datetime.now() if str(x) == 'NaT' else x for x in df['CAGDPRobtained']]
    df['obtained_through'] = 6
    df['obtained_through_date'] = [datetime.datetime.now() if str(x) == 'NaT' else x for x in df['CAGDPRobtained']]
    df['expire'] = 0
    df['obtained_by'] = [-10 if str(x) == 'nan' else int(x) for x in df['id']]
    df['consent_level'] = [3 if x['CAGDPRupdates'] else (2 if x['CAGDPRholdData'] else 0) for idx, x in df.iterrows()]
    df['portal_status'] = [1 if x else 2 for x in df['CAGDPRholdData']]
    df['notes'] = ['\n'.join(['GDPR - Send to Clients: %s' % ('Y' if x['CAGDPRsendClients'] else 'N'),
                              'GDPR - Send Job Mailings: %s' % ('Y' if x['CAGDPRjobs'] else 'N'),
                              'GDPR - Restricted: %s' % ('Y' if x['CAGDPRrestricted'] else 'N'),
                              'GDPR - Date Obtained: %s' % ('N/A' if str(x['CAGDPRobtained']) == 'NaT' else x['CAGDPRobtained']),
                              ]) for idx, x in df.iterrows()]

    df['insert_timestamp'] = datetime.datetime.now()
    df['country_specific'] = 'GB'

    vincere_custom_migration.insert_candidate_gdpr_compliance(df, ddbconn)
    vincere_custom_migration.update_candidate_country_specific(df, ddbconn)
