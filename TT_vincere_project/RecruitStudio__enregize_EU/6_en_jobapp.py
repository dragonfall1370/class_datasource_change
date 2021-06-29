# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('en_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

jobapp = pd.read_sql("""
with jobapp as(
select JobNumber as job_externalid
     , ContactId as candidate_externalid
     , coalesce(CASE when  Accepted = 'TRUE' then 'Accepted' else NULL end
     , CASE when  cl.StartDate is not null then 'Placed' else NULL end
     , CASE when  Offered = 'TRUE' then 'Offered' else NULL end
     , CASE when  RejectedOffer = 'TRUE' then 'Rejected Offer' else NULL end
     , CASE when  Interview1 is not null then 'Arrange Interview' else NULL end
     , CASE when  CVSent = 'TRUE' then 'CV Sent' else NULL end
     , CASE when  Interested = 'TRUE' then 'Interested' else NULL end
     , CASE when  OnHold = 'TRUE' then 'On Hold' else NULL end) as application_stage
     , Rejected
     , cl.LastUpdate
from CandidatesList2 cl
where nullif(JobNumber,'') is not null and nullif(ContactId,'') is not null)
select * from jobapp
""", engine_mssql)
# assert False
# jobapp['application_stage'] = 'Interested'
jobapp.loc[(jobapp['application_stage'].isnull()), 'application_stage'] = 'Interested'
jobapp.loc[(jobapp['Rejected'] == True), 'application_stage'] = jobapp['application_stage']+'Rejected'
jobapp['application_stage'].unique()
jobapp.loc[jobapp['application_stage']=='PlacedRejected']
assert False
# %% mapping stage
tem1 = pd.DataFrame(jobapp['application_stage'].value_counts().keys(), columns=['application_stage'])
tem1['matcher'] = tem1['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('jobapp_stage_mapping.csv')
tem2['matcher'] = tem2['Recruit Studio Stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
tem3['Vincere Stage'].unique()
tem3 = tem3.loc[tem3['Vincere Stage'].notnull()]
tem4 = jobapp.merge(tem3, on='application_stage')
tem4['application_stage'].unique()
tem4['timestamp'] = tem4['LastUpdate'].astype(str)
tem4['vincere Stage'] = tem4['Vincere Stage'].astype(str)
# tem4.loc[tem4['candidate_externalid']=='608682-8469-1893']
# %%
tem5 = tem4[['candidate_externalid', 'job_externalid', 'timestamp', 'vincere Stage']]
tem5['vincere Stage'].value_counts()
tem5['application-stage'] = tem5['vincere Stage']
from common import vincere_job_application
ja = vincere_job_application.JobApplication(None)
# ja.jobapp_map_only(tem5)
tem5['application-positionExternalId'] = tem5['job_externalid']
tem5['application-candidateExternalId'] = tem5['candidate_externalid']
tem5['application-actionedDate'] = tem5['timestamp']
# jobapp_result = ja.process_jobapp_v2(tem5)
jobapp_result = ja.process_jobapp_v3(tem5)
# %% job application separate to: not placement and placement
df_jobapplication_placement = jobapp_result[jobapp_result['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-stage-orgi'] = df_jobapplication_placement['application-stage']
df_jobapplication_placement['application-stage'] = 'OFFERED'
df_jobapplication_other = jobapp_result[~jobapp_result['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-actionedDate'] = df_jobapplication_placement['application-actionedDate'].map(lambda x: str(x)[:10])
df_jobapplication_other['application-actionedDate'] = df_jobapplication_other['application-actionedDate'].map(lambda x: str(x)[:10])
df_jobapplication_placement['application-positionExternalId'] = 'EUK'+df_jobapplication_placement['application-positionExternalId']
df_jobapplication_placement['application-candidateExternalId'] = 'EUK'+df_jobapplication_placement['application-candidateExternalId']
df_jobapplication_other['application-positionExternalId'] = 'EUK'+df_jobapplication_other['application-positionExternalId']
df_jobapplication_other['application-candidateExternalId'] = 'EUK'+df_jobapplication_other['application-candidateExternalId']
df_jobapplication_placement.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement.csv'), index=False)
df_jobapplication_other.to_csv(os.path.join(standard_file_upload, '7_jobapplication_other.csv'), index=False)