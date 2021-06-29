# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
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
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
#conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
#engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
#connection = engine_postgre.raw_connection()


def split_dataframe_to_chunks(df, n):
   df_len = len(df)
   count = 0
   dfs = []
   while True:
      if count > df_len - 1:
         break

      start = count
      count += n
      # print("%s : %s" % (start, count))
      dfs.append(df.iloc[start: count])
   return dfs

job_app = pd.read_sql("""
select
c.CreatedDate
, c.ts2__Job__c as job_externalid
, c.ts2__Candidate_Contact__c as candidate_externalid
, c.ts2__Stage__c as application_stage
from ts2__Application__c c where c.IsDeleted=0 and c.LastModifiedDate >= '2017-10-31'
and c.ts2__Stage__c in ('Application', 'Submittal', 'Offer', 'Placement',
                        'UA Interview', 'Second Client Interview',
                        'Final Client Interview', 'Client Interview')
""", engine_sqlite)

# %% mapping stage
tem1 = pd.DataFrame(job_app['application_stage'].value_counts().keys(), columns=['application_stage'])
tem1['matcher'] = tem1['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem2 = pd.read_csv('jobapp_stage_mapping.csv')
tem2['matcher'] = tem2['JobScience CRM Stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'])

tem4 = job_app.merge(tem3, on='application_stage')

# %%
tem5 = tem4[['job_externalid', 'candidate_externalid', 'CreatedDate', 'Vincere Stage']]
tem5['Vincere Stage'].value_counts()
tem5['Vincere Stage'].unique()
tem5['application-stage'] = tem5['Vincere Stage']

from common import vincere_job_application
ja = vincere_job_application.JobApplication(None)
ja.jobapp_map_only(tem5)
tem5.stage.unique()

# %%
tem5['application-positionExternalId'] = tem5['job_externalid']
tem5['application-candidateExternalId'] = tem5['candidate_externalid']
tem5['application-actionedDate'] = tem5['CreatedDate']
jobapp_result = ja.process_jobapp_v2(tem5)

# %% job application separate to: not placement and placement
df_jobapplication_placement = jobapp_result[jobapp_result['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-stage-orgi'] = df_jobapplication_placement['application-stage']
df_jobapplication_placement['application-stage'] = 'OFFERED'
df_jobapplication_other = jobapp_result[~jobapp_result['application-stage'].str.contains('PLACEMENT')]

df_jobapplication_placement['application-actionedDate'] = df_jobapplication_placement['application-actionedDate'].map(lambda x: str(x)[:10])
df_jobapplication_other['application-actionedDate'] = df_jobapplication_other['application-actionedDate'].map(lambda x: str(x)[:10])

split_df_to_chunks1 = split_dataframe_to_chunks(df_jobapplication_placement, 1000000)
for idx, val in enumerate(split_df_to_chunks1):
    val.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement'+'_'+str(idx)+'.csv'), index=False)

split_df_to_chunks2 = split_dataframe_to_chunks(df_jobapplication_other, 1000000)
for idx, val in enumerate(split_df_to_chunks2):
    val.to_csv(os.path.join(standard_file_upload, '7_jobapplication_other'+'_'+str(idx)+'.csv'), index=False)

#df_jobapplication_placement.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement.csv'), index=False)
#df_jobapplication_other.to_csv(os.path.join(standard_file_upload, '7_jobapplication_other.csv'), index=False)