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
cf.read('ca_config.ini')
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

jobapp1 = pd.read_sql("""
select Candidate_Id as candidate_externalid, Role_Id as job_externalid, Description as application_stage
from Vacancy_Role_Candidate vrc
left join (select code, Description from Lookup where Table_Name = 'VACANCY_ROLE_CANDIDATE_STATUS') s on s.Code = Status_Code
""", engine_mssql)
jobapp1['Created_DTTM'] = None
jobapp1['job_externalid'] = 'VC'+jobapp1['job_externalid']

jobapp2 = pd.read_sql("""
select Booking_Role_Id as job_externalid, Candidate_Id as candidate_externalid, Created_DTTM
from Placement
where Booking_Role_Id is not null and Candidate_Id is not null
and Placement_Code not in ('PL41909'
,'PL41910'
,'PL44078'
,'PL43810'
,'PL44075'
,'PL 43186'
,'PL 42798'
,'PL41961'
,'PL44292'
,'PL41958'
,'PL42798'
,'PL42816'
,'PL42826'
,'PL42960'
,'PL43049'
,'PL43343'
,'PL43100'
,'PL43186'
,'PL43641'
)
""", engine_mssql)
jobapp2['job_externalid'] = 'BK'+jobapp2['job_externalid']
jobapp2['application_stage'] = 'Placed'
jobapp2['Created_DTTM_1'] = jobapp2['Created_DTTM'].apply(lambda x: x[0:10] if x else x)
jobapp2['Created_DTTM_2'] = jobapp2['Created_DTTM'].apply(lambda x: x[11:19] if x else x)
jobapp2['Created_DTTM'] = jobapp2['Created_DTTM_1'] +' '+jobapp2['Created_DTTM_2']
jobapp2['Created_DTTM'] = jobapp2['Created_DTTM'].apply(lambda x: x.replace('.',':') if x else x)
jobapp2 = jobapp2.drop(['Created_DTTM_1','Created_DTTM_2'],axis=1)
jobapp = pd.concat([jobapp1,jobapp2])
assert False
# %% mapping stage
tem1 = pd.DataFrame(jobapp['application_stage'].value_counts().keys(), columns=['application_stage'])
tem1['matcher'] = tem1['application_stage'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('jobapp_stage_mapping.csv')
tem2['matcher'] = tem2['Vacancy status'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = tem1.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
tem3['Vincere Stage'].unique()
tem3 = tem3.loc[tem3['Vincere Stage'].notnull()]
tem4 = jobapp.merge(tem3, on='application_stage')
tem4['application_stage'].unique()
tem4['timestamp'] = tem4['Created_DTTM'].astype(str)
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
# jobapp_result['application-actionedDate'] = jobapp_result['application-actionedDate'].apply(lambda x: x.replace('NaT',''))
# jobapp_result = jobapp_result.loc[jobapp_result['application-positionExternalId'] != '']
# %% job application separate to: not placement and placement
df_jobapplication_placement = jobapp_result[jobapp_result['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-stage-orgi'] = df_jobapplication_placement['application-stage']
df_jobapplication_placement['application-stage'] = 'OFFERED'
df_jobapplication_other = jobapp_result[~jobapp_result['application-stage'].str.contains('PLACEMENT')]
df_jobapplication_placement['application-actionedDate'] = df_jobapplication_placement['application-actionedDate'].map(lambda x: str(x)[:10])
df_jobapplication_other['application-actionedDate'] = df_jobapplication_other['application-actionedDate'].map(lambda x: str(x)[:10])
df_jobapplication_placement.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement.csv'), index=False)
df_jobapplication_other.to_csv(os.path.join(standard_file_upload, '7_jobapplication_other.csv'), index=False)