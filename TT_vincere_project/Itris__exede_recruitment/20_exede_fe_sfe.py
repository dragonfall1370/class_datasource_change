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
cf.read('exede_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre.raw_connection()

conn_str = 'mssql+pymssql://{0}:{1}@{2}:{3}/{4}?charset=utf8'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_pymssql = sqlalchemy.create_engine(conn_str, pool_size=100, max_overflow=200)

sqlite_db = cf['default'].get('sqlite_db')
engine_sqlite = sqlalchemy.create_engine('sqlite:///{}'.format(sqlite_db), encoding='utf8')

# %% load data for fe sfe
entity_keyword = pd.read_sql("""
select
   b.Id,
   b.KeywordId,
   b.FormId, --FormId: 1-candidate 2-company 3-contact
   case b.FormId 
   		when 1 then 'candiate'
   		when 2 then 'company'
   		when 3 then 'contact'
   		else concat('', b.FormId) 
   		end as entity_type,
   b.RecordId as entity_id,
   b.CreatedDateTime,
   b.CreatedUserID,
   k.KEYWORD 
from KeywordRecordLink b 
left join Keywords k on b.KeywordId=k.DICT_ID
""", engine_pymssql)
# assert False

# %% inject funtion experties and sub
fe = entity_keyword[['KEYWORD']].drop_duplicates().dropna()
vincere_custom_migration.append_functional_expertise_subfunctional_expertise(fe.drop_duplicates(), 'KEYWORD', None, 'sfe', ddbconn)


# %% get fe_sfe have just been inserted
fe_sfe = pd.read_sql("""
select fe.id as functional_expertise_id
       , sfe.id as sub_functional_expertise_id
       , fe.name as fe_name
       , sfe.name as sfe_name 
from functional_expertise fe left join sub_functional_expertise sfe on fe.id = sfe.functional_expertise_id;
""", ddbconn)

candkey = entity_keyword.loc[entity_keyword.entity_type=='candiate']
contkey = entity_keyword.loc[entity_keyword.entity_type=='contact']

candkey['fe'] = candkey.KEYWORD
candkey['candidate_externalid'] = candkey.entity_id
candkey['sfe'] = ''
contkey['fe'] = contkey.KEYWORD
contkey['contact_externalid'] = contkey.entity_id
contkey['sfe'] = ''

# %% contact fe
from common import vincere_contact
import importlib
importlib.reload(vincere_contact)
vc = vincere_contact.Contact(ddbconn)
cp2 = vc.insert_fe_sfe2(contkey, mylog)

# %% candidate fe
from common import vincere_candidate
import importlib
importlib.reload(vincere_candidate)
vca = vincere_candidate.Candidate(ddbconn)
cp1 = vca.insert_fe_sfe2(candkey, mylog)


