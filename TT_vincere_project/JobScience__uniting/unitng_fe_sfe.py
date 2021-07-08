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
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///uniting.db', encoding='utf8')
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# assert False

# %% contact function
cont = pd.read_sql("""
select c.Id as contact_externalid
, c.Function__c
, c.Specialism__c
from Contacts c
""", engine_sqlite)
cont['fe'] = cont['Function__c']
cont['sfe'] = cont['Specialism__c']
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont, mylog)

# %% contact sector
cont = pd.read_sql("""
select c.Id as contact_externalid
, c.Sector__c
from Contacts c where c.Sector__c is not null
""", engine_sqlite)
cont['fe'] = cont['Sector__c']
cont['sfe'] = ''
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont, mylog)

# %% cand func
cand = pd.read_sql("""
select c.Id as candidate_externalid
, c.Function__c
, c.Specialism__c
from Contacts c
""", engine_sqlite)
cand['fe'] = cand['Function__c']
cand['sfe'] = cand['Specialism__c']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand, mylog)

# %% candidate
cand = pd.read_sql("""
select c.Id as candidate_externalid
, c.Major_Market__c
, c.Sub_Market__c
from Contacts c
""", engine_sqlite)
tem = cand[['candidate_externalid', 'Sub_Market__c']]
tem = tem.dropna()
tem = tem.Sub_Market__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(tem[['candidate_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['candidate_externalid'], value_name='Sub_Market') \
   .drop('variable', axis='columns') \
   .dropna()
cand = cand.merge(tem, on='candidate_externalid')
cand['fe'] = cand['Major_Market__c']
cand['sfe'] = cand['Sub_Market']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

vcand.insert_fe_sfe2(cand, mylog)