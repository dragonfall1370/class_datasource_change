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
csv_path = r""

# %% data connection
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

assert False
# %% Availability LIM
sql = """
select
      c.Id as user_id,
      c.Availability__c as avai_lim
from Contact c
join RecordType r on (c.RecordTypeId || 'AA2') = r.Id
left join User u on c.OwnerId = u.Id
left join "User" m on c.LastModifiedById = m.Id
where c.IsDeleted = 0
and r.Name = 'Candidate'
"""
candidate_dropdown_text = pd.read_sql(sql, engine_sqlite)

api = 'c6ad81d3f561bc8c09a1a9292d0ab52b'
cand = candidate_dropdown_text[['user_id', 'avai_lim']]
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand['avai_lim'].notnull()]
cand['avai_lim'].unique()
vincere_custom_migration.insert_candidate_drop_down_list_values(cand, 'user_id', 'avai_lim', api, connection)

# %% Started Working
sql = """
select
      c.Id as user_id,
      c.Started_Working__c as start_date
from Contact c
join RecordType r on (c.RecordTypeId || 'AA2') = r.Id
left join User u on c.OwnerId = u.Id
left join "User" m on c.LastModifiedById = m.Id
where c.IsDeleted = 0
and r.Name = 'Candidate'
"""
candidate_date = pd.read_sql(sql, engine_sqlite)

api = '6e906f78c77acab3f3d53af26a47858f'
cand = candidate_date[['user_id', 'start_date']]
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand['start_date'].notnull()]
vincere_custom_migration.insert_candidate_date_field_values(cand, 'user_id', 'start_date', api, connection)

# %% Table
sql = """
select r.ts2__Candidate__c as candidate_externalid
     , r.ts2__Name__c
     , r.ts2__Company__c
     , r.ts2__Role_Title__c
     , r.ts2__Phone__c
     , r.ts2__Description__c
 from ts2__Reference__c r
"""
candidate_date = pd.read_sql(sql, engine_sqlite)
candidate_date = candidate_date.fillna('')
parent_api = '7a67dc61d60b204de76bf7b1ec49ed0b'

api_name = '412e8115f586a4999c0eb8ff23c99688'
api_comp = '8bd4113a39b4dab7f24e7029a842f969'
api_role = 'f3213cfe6f43a7f90cc9ff39dbb8ca7c'
api_phone = '9c673023a60d68671052ee74d975b518'
api_des = 'fa861b6fc1a89cf8f807d7d7337637d9'

name = candidate_date[['candidate_externalid', 'ts2__Name__c']].rename(columns={'ts2__Name__c': 'value'})
comp = candidate_date[['candidate_externalid', 'ts2__Company__c']].rename(columns={'ts2__Company__c': 'value'})
role = candidate_date[['candidate_externalid', 'ts2__Role_Title__c']].rename(columns={'ts2__Role_Title__c': 'value'})
phone = candidate_date[['candidate_externalid', 'ts2__Phone__c']].rename(columns={'ts2__Phone__c': 'value'})
des = candidate_date[['candidate_externalid', 'ts2__Description__c']].rename(columns={'ts2__Description__c': 'value'})

# vcand.insert_custom_field_table_value_v2(name, dest_db, parent_api, api_name, mylog)
# vcand.insert_custom_field_table_value_v2(comp, dest_db, parent_api, api_comp, mylog)
# vcand.insert_custom_field_table_value_v2(role, dest_db, parent_api, api_role, mylog)
# vcand.insert_custom_field_table_value_v2(phone, dest_db, parent_api, api_phone, mylog)
# vcand.insert_custom_field_table_value_v2(des, dest_db, parent_api, api_des, mylog)

vcand.insert_custom_field_table_value(name, parent_api, api_name, mylog)
vcand.insert_custom_field_table_value(comp, parent_api, api_comp, mylog)
vcand.insert_custom_field_table_value(role, parent_api, api_role, mylog)
vcand.insert_custom_field_table_value(phone, parent_api, api_phone, mylog)
vcand.insert_custom_field_table_value(des, parent_api, api_des, mylog)

