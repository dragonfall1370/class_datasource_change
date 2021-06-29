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
cf.read('bower_config.ini')
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

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'),
                                                                  dest_db.get('server'), dest_db.get('port'),
                                                                  dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8',
                                          use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_job
vjob = vincere_job.Job(connection)

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# # %%
# job = pd.read_csv(r'C:\Users\tony\Desktop\rytons\bower-Fix Salary.csv')
# job['job_externalid'] = job['ID'].apply(lambda x: str(x) if x else x)
#
# cand = pd.read_csv(r'C:\Users\tony\Desktop\rytons\bower - Fix candidate telephone.csv')
# candidate = pd.read_sql("""
# select ID as candidate_externalid
#      , Email
# from Candidate
# """, engine_sqlite)
# candidate = candidate.merge(cand, on='Email')
# candidate['candidate_externalid'] = candidate['candidate_externalid'].apply(lambda x: str(x) if x else x)
#
#
# # %% annual salary
# job['actual_salary'] = job['Salary']
# cp7 = vjob.update_actual_salary(job, mylog)
#
# # %% phones
# indt = candidate[['candidate_externalid', 'Telephone']].dropna()
# indt['primary_phone'] = indt['Telephone']
# cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)
assert False
job = pd.read_csv(r'C:\Users\tony\Desktop\rytons\bower - Fix Contract Pay.csv')
job['job_externalid'] = job['ID'].apply(lambda x: str(x) if x else x)
job['actual_salary'] = job['Salary']
cp7 = vjob.update_actual_salary(job, mylog)

# %% job type
job['job_type'] = 'contract'
cp5 = vjob.update_job_type(job, mylog)