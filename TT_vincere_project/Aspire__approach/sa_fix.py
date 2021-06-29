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
cf.read('sa_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
# data_folder = '/Users/truongtung/Desktop'
sqlite_path = cf['default'].get('sqlite_path')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
activities_ori = pd.read_sql("""
select * from vincere_activity_2
""", engine_sqlite)

activities_prod = pd.read_sql("""
select id, company_id, contact_id, position_id, candidate_id, content, insert_timestamp from activity where content is not null
""", engine_postgre_review)
assert False
activities_ori['company_id'] = activities_ori['company_id'].astype(str)
activities_ori['contact_id'] = activities_ori['contact_id'].astype(str)
activities_ori['candidate_id'] = activities_ori['candidate_id'].astype(str)
activities_ori['position_id'] = activities_ori['position_id'].astype(str)
activities_ori['insert_timestamp'] = activities_ori['insert_timestamp'].astype(str)
activities_ori['insert_timestamp'] = activities_ori['insert_timestamp'].apply(lambda x: x.split('.')[0])
activities_ori['matcher'] = activities_ori['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

activities_prod['company_id'] = activities_prod['company_id'].astype(str)
activities_prod['contact_id'] = activities_prod['contact_id'].astype(str)
activities_prod['candidate_id'] = activities_prod['candidate_id'].astype(str)
activities_prod['position_id'] = activities_prod['position_id'].astype(str)
activities_prod['insert_timestamp'] = activities_prod['insert_timestamp'].astype(str)
activities_prod['insert_timestamp'] = activities_prod['insert_timestamp'].apply(lambda x: x.split('.')[0])
activities_prod['matcher'] = activities_prod['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

activities_prod_2 = activities_prod.merge(activities_ori, on=['company_id','contact_id','position_id','candidate_id','matcher','insert_timestamp'])
activities_prod_2 =activities_prod_2.drop_duplicates()
activities_prod_2['rn'] = activities_prod_2.groupby('id').cumcount()
activities_prod_2.loc[activities_prod_2['rn']>0]

tem = activities_prod_2[['id','insert_timestamp_2']].dropna().drop_duplicates()
# tem['insert_timestamp'] = pd.to_datetime(tem['insert_timestamp_2'], format='%d/%m/%Y %H:%M:%S')
tem['insert_timestamp'] = pd.to_datetime(tem['insert_timestamp_2'], format='%Y/%m/%d %H:%M:%S')
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, connection, ['insert_timestamp', ], ['id', ], 'activity', mylog)