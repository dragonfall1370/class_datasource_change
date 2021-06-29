# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
import datetime
from pandas.io.json import json_normalize
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('tt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
connection = engine_postgre.raw_connection()
from common import vincere_job
vjob = vincere_job.Job(engine_postgre.raw_connection())
# assert False
# %% candidate
job = pd.read_sql("""
select j.id as job_externalid
, j."_embedded.custom_fields"
, "_embedded.status.mapping" as status
from jobs_new j
""", engine_sqlite)

job1 = job[['job_externalid', '_embedded.custom_fields']].dropna()
job1['json_value'] = job1['_embedded.custom_fields'].map(eval)

df = []
for index, row in job1.iterrows():
    for json in row['json_value']:
        df.append(json_normalize(json))

job_custom = pd.concat(df)
job_custom = job_custom.loc[job_custom['value'].notnull()]
job_custom['job_id'] = job_custom['_links.self.href'].apply(lambda x: x.split('/')[2])
job_custom_value = job_custom[['job_id','value','_embedded.definition.name']]

job_value = pd.pivot_table(job_custom_value, values='value',columns='_embedded.definition.name',aggfunc='first', index='job_id')
job_value['job_externalid'] = job_value.index
job_value.reset_index()

job = job.merge(job_value, on='job_externalid', how='left')
# assert False
tem = job[['job_externalid', 'status', 'Closed Reason']]
tem1 = tem.loc[tem['status'] == 'closed']
tem2 = tem.loc[tem['status'] == 'filled']
tem3 = pd.concat([tem1, tem2])

tem4 = tem3[['job_externalid', 'Closed Reason']].dropna()
api = '9d9e0c0e60384d85d4e982147e0c0cc4'
vincere_custom_migration.insert_job_text_field_values(tem4, 'job_externalid', 'Closed Reason', api, connection)