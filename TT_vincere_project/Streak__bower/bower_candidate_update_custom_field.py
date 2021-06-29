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

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

assert False
# %% calendar test results
sql = """
select ID as candidate_externalid
     , Calendar
from Candidate
"""
candidate = pd.read_sql(sql, engine_sqlite)

api = 'd80740da0d9d6cca610c672b6a75ded3'
cand = candidate[['candidate_externalid', 'Calendar']].dropna().drop_duplicates()
cand['candidate_externalid'] = cand['candidate_externalid'].astype(str)
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'Calendar', api, connection)

# %% mindx test results
sql = """
select ID as candidate_externalid
     , MindX
from Candidate
"""
candidate = pd.read_sql(sql, engine_sqlite)
api = '2a221554c9759ea0697c244f72837b7a'
cand = candidate[['candidate_externalid', 'MindX']].dropna().drop_duplicates()
cand['candidate_externalid'] = cand['candidate_externalid'].astype(str)
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'MindX', api, connection)

# %% contact source
sql = """
select ID as contact_externalid
     , "Vincere source"
from Contacts
"""
contact = pd.read_sql(sql, engine_sqlite)
api = '322bc09c8fd08ee0f601d0f2c596cbc3'
cont = contact[['contact_externalid', 'Vincere source']].dropna().drop_duplicates()

src = pd.read_csv('source.csv')
src['matcher'] = src['Vincere value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont['matcher'] = cont['Vincere source'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem = cont.merge(src, on='matcher')
tem2 = tem[['contact_externalid', 'Vincere source']]
tem2['Vincere source'].unique()
tem3 = pd.DataFrame({"contact_externalid":['C','D','E','F','G', 'H', 'I'],
                    "Vincere source":['Events','Indeed','Reed',
                              'Online Job Board','SITC','CV Library', 'ibLE']})
source = pd.concat([tem2,tem3])
source['contact_externalid'] = source['contact_externalid'].astype(str)
vincere_custom_migration.insert_contact_drop_down_list_values(source, 'contact_externalid', 'Vincere source', api, connection)