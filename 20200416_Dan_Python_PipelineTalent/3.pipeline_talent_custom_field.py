# %% package config
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
pd.set_option('show_dimensions', True)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('pt_config.ini')
# file storage config
data_folder = cf['default'].get('data_folder')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
# db config
review_db = cf['review_db']
sqlite_url = cf['default'].get('sqlite_url')
# log config
log_file = cf['default'].get('log_file')
mylog = log.get_info_logger(log_file)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_url, encoding='utf8')
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %% Init the Vincere Candidate Object
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
import importlib
importlib.reload(vincere_candidate)
importlib.reload(vincere_custom_migration)
assert False

#%% custom fields
sql = """
select "Personnel ID
[link to folder]" as candidate_externalid,
"First Aid
[list]" as "First Aid",
"Working with Vulnerable People
[list]" as "Working with Vulnerable People",
"Security Clearance
[list]" as "Security Clearance",
"Psychometric Testing
[list]" as "Psychometric Testing"
from CRM_v05___2020_04_06_12PM___Main_List
"""
cus_field = pd.read_sql(sql, engine_sqlite)

# First Aid
tmp = cus_field[['candidate_externalid', 'First Aid']]
tmp['First Aid'].replace('yes', 'Yes', inplace=True)
tmp.dropna(subset=['First Aid'], inplace=True)
tmp = tmp.append({'candidate_externalid': '-10', 'First Aid': 'Applied'}, ignore_index=True)
field_key = 'd137290ebf655c3d134bf5a0cd3a34c0'
vincere_custom_migration.insert_candidate_drop_down_list_values(tmp, 'candidate_externalid', 'First Aid', field_key, connection)

# Working with Vulnerable People
tmp = cus_field[['candidate_externalid', 'Working with Vulnerable People']]
tmp.dropna(subset=['Working with Vulnerable People'], inplace=True)
tmp = tmp.append({'candidate_externalid': '-10', 'Working with Vulnerable People': 'Applied'}, ignore_index=True)
field_key = 'e5e5c22679804099f08084b555e9d278'
vincere_custom_migration.insert_candidate_drop_down_list_values(tmp, 'candidate_externalid', 'Working with Vulnerable People', field_key, connection)

# Security Clearance
tmp = cus_field[['candidate_externalid', 'Security Clearance']]
tmp['Security Clearance'].replace('none', 'None', inplace=True)
tmp['Security Clearance'].value_counts()
tmp.dropna(subset=['Security Clearance'], inplace=True)
tmp = tmp.append({'candidate_externalid': '-10', 'Security Clearance': 'Standard police check'}, ignore_index=True)
field_key = '31d2b70e131eaaf8c6cbe89128198286'
vincere_custom_migration.insert_candidate_drop_down_list_values(tmp, 'candidate_externalid', 'Security Clearance', field_key, connection)

# Psycometric Testing
tmp = cus_field[['candidate_externalid', 'Psychometric Testing']]
tmp = tmp.append([{'candidate_externalid': '-10', 'Psychometric Testing': 'Completed'},
                  {'candidate_externalid': '-10', 'Psychometric Testing': 'Not Completed'}]
                  , ignore_index=True)
tmp.dropna(subset=['Psychometric Testing'], inplace=True)
field_key = 'ad516fa5781f9c9a8d5d3baba350ffde'
vincere_custom_migration.insert_candidate_drop_down_list_values(tmp, 'candidate_externalid', 'Security Clearance', field_key, connection)

#%%
# FirstAid = d137290ebf655c3d134bf5a0cd3a34c0
# Working with Vulnerable People = e5e5c22679804099f08084b555e9d278
# Security Clearance = 31d2b70e131eaaf8c6cbe89128198286
# Psycometric Testing = ad516fa5781f9c9a8d5d3baba350ffde