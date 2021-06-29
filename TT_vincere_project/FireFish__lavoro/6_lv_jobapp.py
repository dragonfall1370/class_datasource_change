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
cf.read('lv_config.ini')
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

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

jobapp_shortlisted = pd.read_sql("""
select cjp.CandidateID as candidate_externalid,
       cjp.JobID as job_externalid,
       cjp.CreatedDate,
       cjs.Description as application_stage,
       case when StatusID = 1 then 'Considering' when StatusID = 2 then 'Not Progressing' when  StatusID = 3 then 'Withdraw' end as type
from CandidateJobProgress cjp
left join CandidateJobStage cjs on cjp.StageID = cjs.CandidateJobStageID
where application_stage = 'Considering'
""", engine_sqlite)

jobapp_offer = pd.read_sql("""
select cjp.CandidateID as candidate_externalid,
       cjp.JobID as job_externalid,
       cjp.CreatedDate,
       cjs.Description as application_stage,
       case when StatusID = 1 then 'Offer' when StatusID = 2 then 'Offer Declined' when  StatusID = 3 then 'Offer Declined' end as type
from CandidateJobProgress cjp
left join CandidateJobStage cjs on cjp.StageID = cjs.CandidateJobStageID
where application_stage = 'Offer'
""", engine_sqlite)

jobapp_interview = pd.read_sql("""
select interview.*, application_stage from
(select cjp.CandidateID,
       cjp.JobID,
       cjp.CreatedDate,
       cjs.Description as application_stage
from CandidateJobProgress cjp
left join CandidateJobStage cjs on cjp.StageID = cjs.CandidateJobStageID) app_stage
join (select i.JobID as job_externalid,
             i.CandidateID as candidate_externalid,
             i.CreatedDate,
             it.Description as type
from Interview i
left join InterviewType it on it.InterviewTypeID = i.InterviewTypeID) interview on interview.candidate_externalid = app_stage.CandidateID and interview.job_externalid = app_stage.JobID
where application_stage in ('Employer Interview','Recruiter Interview')
""", engine_sqlite)

jobapp_rest = pd.read_sql("""
select cjp.CandidateID as candidate_externalid,
       cjp.JobID as job_externalid,
       cjp.CreatedDate,
       cjs.Description as application_stage,
       NULL as type
from CandidateJobProgress cjp
left join CandidateJobStage cjs on cjp.StageID = cjs.CandidateJobStageID
where application_stage not in  ('Offer','Considering','Employer Interview','Recruiter Interview')
""", engine_sqlite)
jobapp = pd.concat([jobapp_shortlisted, jobapp_interview, jobapp_offer, jobapp_rest])
jobapp = jobapp.drop_duplicates()
jobapp['application_stage'] = jobapp[['application_stage', 'type']].apply(lambda x: ''.join([e for e in x if e]), axis=1)
assert False
# %% mapping stage
tem1 = pd.DataFrame(jobapp['application_stage'].value_counts().keys(), columns=['application_stage'])
tem1['matcher'] = tem1['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('jobapp_stage_mapping.csv')
tem2 = tem2.where(tem2.notnull(),None)
tem2['matcher'] = tem2[['FireFish Stage', 'Type / Action Name']].apply(lambda x: ''.join([e for e in x if e]), axis=1)
tem2['matcher'] = tem2['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
tem3['Vincere Stage'].unique()
tem3 = tem3.loc[tem3['Vincere Stage'].notnull()]
tem4 = jobapp.merge(tem3, on='application_stage')
tem4['application_stage'].unique()
tem4['timestamp'] = tem4['CreatedDate'].astype(str)
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