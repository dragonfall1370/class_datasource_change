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
cf.read('rt_config.ini')
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
select j.Id as job_externalid
     , d."Level 1" as discipline
     , d."Level 2" as role
from Job j
left join rytons_mapping_discipline d on (lower(j.SecondaryJobCategoryWSIID) = lower(d."Secondary ID"))
""", engine_sqlite)
job = job.dropna()
job['job_externalid'] = job['job_externalid'].astype(str)
job['fe'] = job['discipline']
job['sfe'] = job['role']
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(job, mylog)

# %% JOB
job = pd.read_sql("""
select j.Id as job_externalid
     , i."Level 1" as specialty
from Job j
left join rytons_mapping_industries_speciality i on(lower(j.PrimaryExpertiseWSIID) = lower(i."Primary ID"))
""", engine_sqlite)
job = job.dropna()
job['job_externalid'] = job['job_externalid'].astype(str)
job['fe'] = job['specialty']
job['sfe'] = None
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(job, mylog)

# %% candidate
cand = pd.read_sql("""
select cp.CandidateID as candidate_externalid
     , d."Level 1" as discipline
     , d."Level 2" as role
from CandidatePreference cp
left join rytons_mapping_discipline d on (lower(cp.SecondaryPreferenceWSIID) = lower(d."Secondary ID")) and (lower(cp.PrimaryPreferenceWSIID) = lower(d."Primary ID"))
""", engine_sqlite)
cand = cand.dropna()
cand['candidate_externalid'] = cand['candidate_externalid'].astype(str)
cand['fe'] = cand['discipline']
cand['sfe'] = cand['role']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand, mylog)

# %% candidate
cand = pd.read_sql("""

select cp.CandidateID as candidate_externalid
     , i."Level 1" as speciality
from CandidatePreference cp
left join rytons_mapping_industries_speciality i on(lower(cp.PrimaryPreferenceWSIID) = lower(i."Primary ID"))""", engine_sqlite)
cand = cand.dropna()
cand['candidate_externalid'] = cand['candidate_externalid'].astype(str)
cand['fe'] = cand['speciality']
cand['sfe'] = None
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand, mylog)