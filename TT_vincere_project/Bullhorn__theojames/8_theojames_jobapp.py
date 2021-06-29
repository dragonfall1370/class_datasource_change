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
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('theo_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %%
job_app = pd.read_sql("""
select jr.jobPostingID as job_externalid
	, ca.candidateID as candidate_externalid
	, jr.status as application_stage
	, jr.dateAdded
	from bullhorn1.BH_JobResponse jr
	left join (select c.candidateID, c.userID as CandidateUserID, uc.userID, uc.name, uc.email as candidate_email
						from bullhorn1.Candidate c
						left join bullhorn1.BH_UserContact uc on c.userID = uc.userID) ca on ca.CandidateUserID = jr.userID
	left join bullhorn1.BH_JobPosting jp on jp.jobPostingID = jr.jobPostingID
union all
-- placement
select pl.jobPostingID as job_externalid
	, ca.candidateID as candidate_externalid
	, 'PLACED' as application_stage
	, pl.dateAdded --can be used as placed date / offered date
	from bullhorn1.BH_Placement pl
	left join (select c.candidateID, c.userID as CandidateUserID, uc.userID, uc.name, uc.email as candidate_email
						from bullhorn1.Candidate c
						left join bullhorn1.BH_UserContact uc on c.userID = uc.userID) ca on ca.CandidateUserID = pl.userID
	left join bullhorn1.BH_JobPosting jp on jp.jobPostingID = pl.jobPostingID

""", engine_mssql)

# %% mapping stage
tem1 = pd.DataFrame(job_app['application_stage'].value_counts().keys(), columns=['application_stage'])
tem1['matcher'] = tem1['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem2 = pd.read_csv('job_application_stage_mapping_theo.csv')
tem2['matcher'] = tem2['Bullhorn CRM Stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'])

tem4 = job_app.merge(tem3, on='application_stage')

# %%
tem5 = tem4[['job_externalid', 'candidate_externalid', 'dateAdded', 'Vincere Stage']]
tem5['Vincere Stage'].value_counts()
tem5['Vincere Stage'].unique()
tem5['application-stage'] = tem5['Vincere Stage']

from common import vincere_job_application
ja = vincere_job_application.JobApplication(None)
ja.jobapp_map_only(tem5)
tem5.stage.unique()

# %% filter out some unneeded jobs and candidates
job = pd.read_csv(os.path.join(standard_file_upload, '5_job.csv'))
cand = pd.read_csv(os.path.join(standard_file_upload, '6_candidate.csv'))

tem5 = tem5.loc[tem5.job_externalid.isin(job['position-externalId'])]
tem5 = tem5.loc[tem5.candidate_externalid.isin(cand['candidate-externalId'])]
# assert False

# %%
tem5['application-positionExternalId'] = tem5['job_externalid']
tem5['application-candidateExternalId'] = tem5['candidate_externalid']
tem5['application-actionedDate'] = tem5['dateAdded']
jobapp_result = ja.process_jobapp_v2(tem5)

# %% job application separate to: not placement and placement
df_jobapplication_placement = jobapp_result[jobapp_result['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-stage-orgi'] = df_jobapplication_placement['application-stage']
df_jobapplication_placement['application-stage'] = 'OFFERED'
df_jobapplication_other = jobapp_result[~jobapp_result['application-stage'].str.contains('PLACEMENT')]

df_jobapplication_placement['application-actionedDate'] = df_jobapplication_placement['application-actionedDate'].map(lambda x: str(x)[:10])
df_jobapplication_other['application-actionedDate'] = df_jobapplication_other['application-actionedDate'].map(lambda x: str(x)[:10])

df_jobapplication_placement.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement.csv'), index=False)
df_jobapplication_other.to_csv(os.path.join(standard_file_upload, '7_jobapplication_other.csv'), index=False)