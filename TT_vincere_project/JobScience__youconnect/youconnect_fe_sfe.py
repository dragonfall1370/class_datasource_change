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
cf.read('yc_config.ini')
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
function = pd.read_csv('fe-sfe.csv')
assert False

# %% contact
cont = pd.read_sql("""
select cont.Id as contact_externalid
, cont.ts2__Skill_Codes__c
from Contact cont
join RecordType r on (cont.RecordTypeId || 'AA2') = r.Id
left join Account com on (cont.AccountId = com.Id and com.IsDeleted=0)
where cont.IsDeleted = 0
and r.Name = 'Contact'
""", engine_sqlite)
cont = cont.dropna()
cont = cont.ts2__Skill_Codes__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(cont[['contact_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['contact_externalid'], value_name='skills') \
   .drop('variable', axis='columns') \
   .dropna()
cont = cont.merge(function, left_on='skills', right_on='Skill Codes Jobscience')
cont['fe'] = cont['Functional Expertise']
cont['sfe'] = cont['Sub-Functional Expertise']
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont, mylog)

# %% JOB
job = pd.read_sql("""
select j.Id as job_externalid
     , j.ts2__Skill_Codes__c 
from ts2__Job__c j
""", engine_sqlite)
job = job.dropna()
job = job.ts2__Skill_Codes__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(job[['job_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['job_externalid'], value_name='skills') \
   .drop('variable', axis='columns') \
   .dropna()
job['skills'].unique()
job = job.merge(function, left_on='skills', right_on='Skill Codes Jobscience')
job['fe'] = job['Functional Expertise']
job['sfe'] = job['Sub-Functional Expertise']
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(job, mylog)

# %% candidate
cand = pd.read_sql("""
select
      c.Id as candidate_externalid,
       c.ts2__Skill_Codes__c
from Contact c
join RecordType r on (c.RecordTypeId || 'AA2') = r.Id
left join User u on c.OwnerId = u.Id
left join "User" m on c.LastModifiedById = m.Id
where c.IsDeleted = 0
and r.Name = 'Candidate'
""", engine_sqlite)
cand = cand.dropna()
cand = cand.ts2__Skill_Codes__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(cand[['candidate_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['candidate_externalid'], value_name='skills') \
   .drop('variable', axis='columns') \
   .dropna()
cand = cand.merge(function, left_on='skills', right_on='Skill Codes Jobscience')
cand['fe'] = cand['Functional Expertise']
cand['sfe'] = cand['Sub-Functional Expertise']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand, mylog)