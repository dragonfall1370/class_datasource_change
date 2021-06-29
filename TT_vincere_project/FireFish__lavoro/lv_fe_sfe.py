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
assert False

# %% JOB
func = pd.read_csv('fesfe.csv')
job = pd.read_sql("""
select j.ID as job_externalid, l.Value as role
from Job j
left join Drop_Down___JobCategories l on j.SecondaryJobCategoryWSIID = l.ID
where SecondaryJobCategoryWSIID is not null
""", engine_sqlite)
job = job.dropna()
job = job.merge(func, left_on='role', right_on='Discipline')
job['fe'] = job['Functional Expertise Values']
job['sfe'] = job['Sub Functional Expertise Values']
j1 = job[['job_externalid','fe','sfe']]
j1 = j1.where(j1.notnull(), None)
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(j1, mylog)

# %% candidate dis
func = pd.read_csv('fesfe.csv')
cand1 = pd.read_sql("""
select p.ID as candidate_externalid, l.Value as role
from Person p
left join Drop_Down___JobCategories l on p.SecondaryJobCategoryWSIID = l.ID
where SecondaryJobCategoryWSIID is not null
""", engine_sqlite)
cand1 = cand1.merge(func, left_on='role', right_on='Discipline')
cand1['fe'] = cand1['Functional Expertise Values']
cand1['sfe'] = cand1['Sub Functional Expertise Values']
c1 = cand1[['candidate_externalid','fe','sfe']]
c1 = c1.where(c1.notnull(), None)
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(c1, mylog)