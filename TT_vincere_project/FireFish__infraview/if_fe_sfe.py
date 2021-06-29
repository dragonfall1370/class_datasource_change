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
cf.read('if_config.ini')
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
assert False

# %% JOB
job = pd.read_sql("""
select j.ID as job_externalid, PrimaryJobCategoryWSIID, SecondaryJobCategoryWSIID, Discipline, Role
from Job j
left join (select j1.id, j2.Value as Discipline, j1.Value as Role
from Drop_Down___JobCategories j1
left join Drop_Down___JobCategories j2 on j1.ParentID = j2.ID
where j1.ParentID is not null) l on j.SecondaryJobCategoryWSIID = l.ID
where SecondaryJobCategoryWSIID is not null
""", engine_sqlite)
job = job.dropna()
job['job_externalid'] = job['job_externalid'].astype(str)
job['matcher'] = job['Discipline']+job['Role']
job['matcher'] = job['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func = pd.read_csv('fe-sfe.csv')
func = func.loc[func['FireFish list']=='Discipline and Role']
func['matcher'] = func['Value']+func['Sub value']
func['matcher'] = func['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job = job.merge(func, on='matcher')
job['fe'] = job['Functional Expertise']
job['sfe'] = job['Sub Functional Expertise']
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(job, mylog)

# %% JOB
func = pd.read_csv('fe-sfe.csv')
job1 = pd.read_sql("""
select j.ID as job_externalid, PrimaryExpertiseWSIID, SecondaryExpertiseWSIID, Speciality, Sub_specialty
from Job j
left join (select j1.id, j2.Value as Speciality, j1.Value as Sub_specialty
from Drop_Down___Expertise j1
left join Drop_Down___Expertise j2 on j1.ParentID = j2.ID
where j1.ParentID is not null) l on j.SecondaryExpertiseWSIID = l.ID
where SecondaryExpertiseWSIID is not null
""", engine_sqlite)

job1['job_externalid'] = job1['job_externalid'].astype(str)
job1['matcher'] = job1['Speciality']+job1['Sub_specialty']
job1['matcher'] = job1['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func = func.loc[func['FireFish list']=='Speciality and Sub Speciality']
func['matcher'] = func['Value']+func['Sub value']
func['matcher'] = func['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job1 = job1.merge(func, on='matcher')
job1['fe'] = job1['Functional Expertise']
job1['sfe'] = job1['Sub Functional Expertise']
j1 = job1[['job_externalid','fe','sfe']]

job2 = pd.read_sql("""
select j.ID as job_externalid, PrimaryExpertiseWSIID, SecondaryExpertiseWSIID, Value as Speciality
from Job j
left join Drop_Down___Expertise l on j.PrimaryExpertiseWSIID = l.ID
where PrimaryExpertiseWSIID is not null and SecondaryExpertiseWSIID is null
""", engine_sqlite)
job2['job_externalid'] = job2['job_externalid'].astype(str)
func = func.loc[func['FireFish list']=='Speciality and Sub Speciality']
job2 = job2.merge(func, left_on='Speciality', right_on='Value')
job2['fe'] = job2['Functional Expertise']
job2['sfe'] = ''
j2 = job2[['job_externalid','fe','sfe']].drop_duplicates()
job = pd.concat([j1,j2])
job = job.drop_duplicates()
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(job, mylog)

# %% CONTACT DIS
func = pd.read_csv('fe-sfe.csv')
func = func.loc[func['FireFish list']=='Discipline and Role']
cont = pd.read_sql("""
select PersonID as contact_externalid, Discipline, Role
from ContactJobCategory cjc
left join (select j1.id, j2.Value as Discipline, j1.Value as Role
from Drop_Down___JobCategories j1
left join Drop_Down___JobCategories j2 on j1.ParentID = j2.ID) j on cjc.JobCategoryWSIID = j.ID
""", engine_sqlite)
cont = cont.drop_duplicates()
cont['contact_externalid'] = cont['contact_externalid'].astype(str)
cont1 = cont.loc[cont['Discipline'].notnull()]
cont2 = cont.loc[cont['Discipline'].isnull()]

cont1['matcher'] = cont1['Discipline']+cont1['Role']
cont1['matcher'] = cont1['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func['matcher'] = func['Value']+func['Sub value']
func['matcher'] = func['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont1 = cont1.merge(func, on='matcher')
cont1['fe'] = cont1['Functional Expertise']
cont1['sfe'] = cont1['Sub Functional Expertise']
c1 = cont1[['contact_externalid','fe','sfe']]

cont2 = cont2.merge(func, left_on='Role', right_on='Value')
cont2['fe'] = cont2['Functional Expertise']
cont2['sfe'] = ''
c2 = cont2[['contact_externalid','fe','sfe']].drop_duplicates()
cont = pd.concat([c1,c2])
cont = cont.drop_duplicates()
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont, mylog)

# %% CONTACT SPE
func = pd.read_csv('fe-sfe.csv')
func = func.loc[func['FireFish list']=='Speciality and Sub Speciality']
cont = pd.read_sql("""
select PersonID as contact_externalid, Speciality, Sub_specialty
from ContactExpertise ce
left join (select j1.id, j2.Value as Speciality, j1.Value as Sub_specialty
from Drop_Down___Expertise j1
left join Drop_Down___Expertise j2 on j1.ParentID = j2.ID) j on ce.ExpertiseWSIID = j.ID
""", engine_sqlite)
cont = cont.drop_duplicates()
cont['contact_externalid'] = cont['contact_externalid'].astype(str)
cont1 = cont.loc[cont['Speciality'].notnull()]
cont2 = cont.loc[cont['Speciality'].isnull()]

cont1['matcher'] = cont1['Speciality']+cont1['Sub_specialty']
cont1['matcher'] = cont1['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func['matcher'] = func['Value']+func['Sub value']
func['matcher'] = func['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont1 = cont1.merge(func, on='matcher')
cont1['fe'] = cont1['Functional Expertise']
cont1['sfe'] = cont1['Sub Functional Expertise']
c1 = cont1[['contact_externalid','fe','sfe']]

cont2 = cont2.merge(func, left_on='Sub_specialty', right_on='Value')
cont2['fe'] = cont2['Functional Expertise']
cont2['sfe'] = ''
c2 = cont2[['contact_externalid','fe','sfe']].drop_duplicates()
cont = pd.concat([c1,c2])
cont = cont.drop_duplicates()
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont, mylog)

# %% candidate dis
func = pd.read_csv('fe-sfe.csv')
func = func.loc[func['FireFish list']=='Discipline and Role']
cand1 = pd.read_sql("""
select p.ID as candidate_externalid, PrimaryJobCategoryWSIID, SecondaryJobCategoryWSIID, Discipline, Role
from Person p
left join (select j1.id, j2.Value as Discipline, j1.Value as Role
from Drop_Down___JobCategories j1
left join Drop_Down___JobCategories j2 on j1.ParentID = j2.ID
where j1.ParentID is not null) l on p.SecondaryJobCategoryWSIID = l.ID
where SecondaryJobCategoryWSIID is not null
""", engine_sqlite)
cand1['matcher'] = cand1['Discipline']+cand1['Role']
cand1['matcher'] = cand1['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func['matcher'] = func['Value']+func['Sub value']
func['matcher'] = func['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand1 = cand1.merge(func, on='matcher')
cand1['fe'] = cand1['Functional Expertise']
cand1['sfe'] = cand1['Sub Functional Expertise']
c1 = cand1[['candidate_externalid','fe','sfe']]

cand2 = pd.read_sql("""
select p.ID as candidate_externalid, PrimaryJobCategoryWSIID, Value
from Person p
left join Drop_Down___JobCategories l on p.PrimaryJobCategoryWSIID = l.ID
where SecondaryJobCategoryWSIID is null and PrimaryJobCategoryWSIID is not null
""", engine_sqlite)
cand2['candidate_externalid'] = cand2['candidate_externalid'].astype(str)
cand2 = cand2.merge(func, on='Value')
cand2['fe'] = cand2['Functional Expertise']
cand2['sfe'] = ''
c2 = cand2[['candidate_externalid','fe','sfe']].drop_duplicates()
cand = pd.concat([c1,c2])
cand = cand.drop_duplicates()
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand, mylog)

# %% candidate spe
func = pd.read_csv('fe-sfe.csv')
func = func.loc[func['FireFish list']=='Speciality and Sub Speciality']
cand1 = pd.read_sql("""
select p.ID as candidate_externalid, ExpertiseWSIID, ExpertiseSecondaryWSIID, Speciality, Sub_specialty
from Candidate p
left join (select j1.id, j2.Value as Speciality, j1.Value as Sub_specialty
from Drop_Down___Expertise j1
left join Drop_Down___Expertise j2 on j1.ParentID = j2.ID
where j1.ParentID is not null) l on ExpertiseSecondaryWSIID = l.ID
where ExpertiseSecondaryWSIID is not null
""", engine_sqlite)
cand1['matcher'] = cand1['Speciality']+cand1['Sub_specialty']
cand1['matcher'] = cand1['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func['matcher'] = func['Value']+func['Sub value']
func['matcher'] = func['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand1 = cand1.merge(func, on='matcher')
cand1['fe'] = cand1['Functional Expertise']
cand1['sfe'] = cand1['Sub Functional Expertise']
c1 = cand1[['candidate_externalid','fe','sfe']]

cand2 = pd.read_sql("""
select p.ID as candidate_externalid, ExpertiseWSIID, Value
from Candidate p
left join Drop_Down___Expertise l on p.ExpertiseWSIID = l.ID
where ExpertiseSecondaryWSIID is null and ExpertiseWSIID is not null
""", engine_sqlite)
cand2['candidate_externalid'] = cand2['candidate_externalid'].astype(str)
cand2 = cand2.merge(func, on='Value')
cand2['fe'] = cand2['Functional Expertise']
cand2['sfe'] = ''
c2 = cand2[['candidate_externalid','fe','sfe']].drop_duplicates()
cand = pd.concat([c1,c2])
cand = cand.drop_duplicates()
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand, mylog)