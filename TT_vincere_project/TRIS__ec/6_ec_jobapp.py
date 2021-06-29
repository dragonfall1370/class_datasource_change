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
cf.read('ec_config.ini')
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

tem1 = pd.read_sql("""
select JobID as job_externalid
     , CandidateID as candidate_externalid
     , JobCanDateSelected as date
from JobCandidate jc
where JobCanDateSelected is not null
and JobCanInterviewDate  is null
and JobCanResumeStart = 0
and JobCanContractDate is null
""", engine_mssql)
tem1['job_externalid'] = tem1['job_externalid'].astype(str)
tem1['candidate_externalid'] = tem1['candidate_externalid'].astype(str)
tem1['application_stage'] = 'Selected'

tem2 = pd.read_sql("""
select JobID as job_externalid
     , CandidateID as candidate_externalid
     , coalesce(JobCanDateSubmitted,JobCanDateSelected) as date
from JobCandidate jc
where JobCanInterviewDate  is null
and JobCanResumeStart = 1
and JobCanContractDate is null
""", engine_mssql)
tem2['job_externalid'] = tem2['job_externalid'].astype(str)
tem2['candidate_externalid'] = tem2['candidate_externalid'].astype(str)
tem2['application_stage'] = 'Start'

tem3 = pd.read_sql("""
select JobID as job_externalid
     , CandidateID as candidate_externalid
     , JobCanInterviewDate as date
from JobCandidate jc
where JobCanInterviewDate  is not null
and JobCanContractDate is null
""", engine_mssql)
tem3['job_externalid'] = tem3['job_externalid'].astype(str)
tem3['candidate_externalid'] = tem3['candidate_externalid'].astype(str)
tem3['application_stage'] = 'Interview'

tem4 = pd.read_sql("""
select JobID as job_externalid
     , CandidateID as candidate_externalid
     , JobCanContractDate as date
from JobCandidate jc
where JobCanContractDate is not null
""", engine_mssql)
tem4['job_externalid'] = tem4['job_externalid'].astype(str)
tem4['candidate_externalid'] = tem4['candidate_externalid'].astype(str)
tem4['application_stage'] = 'Contract Placement'

jobapp = pd.concat([tem1,tem2,tem3,tem4])
assert False
# %% mapping stage
tem1 = pd.DataFrame(jobapp['application_stage'].value_counts().keys(), columns=['application_stage'])
tem1['matcher'] = tem1['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('jobapp_stage_mapping.csv')
tem2['matcher'] = tem2['TRIS Stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
tem3['Vincere Stage'].unique()
tem3 = tem3.loc[tem3['Vincere Stage'].notnull()]
tem4 = jobapp.merge(tem3, on='application_stage')
tem4['application_stage'].unique()
tem4['timestamp'] = tem4['date'].astype(str)
tem4['vincere Stage'] = tem4['Vincere Stage'].astype(str)
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
df_jobapplication_placement.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement.csv'), index=False)
df_jobapplication_other.to_csv(os.path.join(standard_file_upload, '7_jobapplication_other.csv'), index=False)