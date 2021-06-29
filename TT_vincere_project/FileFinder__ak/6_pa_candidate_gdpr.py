# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
from functools import reduce
from common import vincere_job_application
import pandas as pd

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ak_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# assert False
# %% load data input
df1 = pd.read_csv('gdpr.csv', encoding='cp1252')
df2 = pd.read_csv('gdpr_mapping.csv')

tem = df1[['idPerson', 'FromDate', 'ToDate', 'CreatedOn', 'Last event', 'Status']]
tem1 = tem.merge(df2, on='Status')

tem1['external_id'] = tem1['idPerson']
tem1['exercise_right'] = tem1['Person informed how to exercise their rights']  # 3: Other [Person informed how to exercise their rights]
tem1['request_through'] = tem1['Request through']  # 6: Other
tem1['obtained_through'] = tem1['Obtained through'] # 6: Other
tem1['obtained_through_date'] = pd.to_datetime(tem1['FromDate'])
tem1['expire'] = None
tem1.loc[tem1['Expires'] == 1.0, 'expire'] = 1
tem1['expire'] = tem1['expire'].apply(lambda x: int(x) if x else x)
tem1['expire_date'] = pd.to_datetime(tem1['ToDate'])
tem1['portal_status'] = 1  # 1:Consent given / 2:Pending [Consent to keep] / 3:To be forgotten / 4:Contract / 5:Legitimate interest
tem1['notes'] = tem1['Last event']  # [Notes | Journal]*
tem1['insert_timestamp'] = pd.to_datetime(tem1['CreatedOn'])
cols = ['candidate_id',
        'exercise_right',  # 3: Other [Person informed how to exercise their rights]
        'request_through',  # 6: Other
        'obtained_through',  # 6: Other
        'obtained_through_date',
        'expire',
        'expire_date',  # 0: No
        'portal_status',  # 1:Consent given / 2:Pending [Consent to keep]
        'notes',  # [Notes | Journal]
        'insert_timestamp']
# assert False
vincere_custom_migration.insert_candidate_gdpr_compliance(tem1, connection, cols)
