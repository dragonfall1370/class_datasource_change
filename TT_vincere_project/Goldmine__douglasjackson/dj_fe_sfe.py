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
cf.read('dj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
func = pd.read_csv('fe-sfe.csv')
assert False
# %% candidate
cand_fe = pd.read_sql("""select c1.ACCOUNTNO as candidate_externalid, nullif(U_KEY3,'') as func
from CONTACT1 c1
where KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
""", engine_mssql)
cand_fe = cand_fe.dropna().drop_duplicates()
cand_fe = cand_fe.merge(func, left_on='func',right_on='GM Value')
cand_fe['fe'] = cand_fe['Vincere Industry']
cand_fe['sfe'] = ''
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
cp1 = vcand.insert_fe_sfe2(cand_fe, mylog)

# %% contact
cont_fe = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid, nullif(U_KEY3,'') as func
from CONTACT1 c1
where KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
cont_fe = cont_fe.dropna().drop_duplicates()
cont_fe = cont_fe.merge(func, left_on='func',right_on='GM Value')
cont_fe['fe'] = cont_fe['Vincere Industry']
cont_fe['sfe'] = ''
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont_fe, mylog)
# df = cont_fe
# logger = mylog
# tem2 = df[['contact_externalid', 'fe', 'sfe']]
# tem2['matcher_fe'] = tem2['fe'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem2['matcher_sfe'] = tem2['sfe'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# fe = pd.read_sql('select id as functional_expertise_id, name as fe from functional_expertise', vcont.ddbconn)
# fe['matcher_fe'] = fe['fe'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem2 = tem2.merge(fe, on='matcher_fe', how='left')
# tem2 = tem2.where(tem2.notnull(), None)
#
# tem2 = tem2.merge(vcont.contact, on=['contact_externalid'])
# tem2['contact_id'] = tem2['id']
# tem2['insert_timestamp'] = datetime.datetime.now()
# tem3 = tem2[['functional_expertise_id', 'contact_id', 'insert_timestamp']].dropna()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem3, vcont.ddbconn, ['functional_expertise_id', 'contact_id', 'insert_timestamp'], 'contact_functional_expertise', logger)