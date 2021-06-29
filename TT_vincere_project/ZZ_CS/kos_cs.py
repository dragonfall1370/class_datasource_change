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
cf.read('dn_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db_sin')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

def split_data(list):
    arr = []
    for item in list:
        owner = item['ownerId']
        arr.append(str(owner))
    return ','.join(arr)

# %%
# candidate = pd.read_sql("""select candidate_owner_json,c.id,current_location_id, first_name, last_name
# from candidate c join common_location cl on c.current_location_id=cl.id
# where candidate_owner_json is not null and country_code != 'CN'""",connection)
# candidate['candidate_owner_json'] = candidate['candidate_owner_json'].apply(lambda x: x.replace('true','"true"'))
# candidate['candidate_owner_json'] = candidate['candidate_owner_json'].apply(lambda x: x.replace('false','"false"'))
# candidate['candidate_owner_json'] = candidate['candidate_owner_json'].apply(lambda x: x.replace('""true""','"true"'))
# candidate['candidate_owner_json'] = candidate['candidate_owner_json'].apply(lambda x: x.replace('""false""','"false"'))
# candidate['candidate_owner_json'] = candidate['candidate_owner_json'].apply(lambda x: eval(x))
# candidate['candidate_owner_json'] = candidate['candidate_owner_json'].apply(lambda x: split_data(x))
# candidate = vincere_common.splitDataFrameList(candidate, 'candidate_owner_json',',')
# candidate = candidate.loc[candidate['candidate_owner_json']!='']
# candidate['candidate_owner_json'] = candidate['candidate_owner_json'].apply(lambda x: int(x))
#
# user = pd.read_sql("""select id from user_account where name in
# ('Calvin Lam'
# ,'David Hu'
# ,'Eason Zhang'
# ,'Ervin Wang'
# ,'Flora Guo'
# ,'Gary Guo'
# ,'Jophy Zhu'
# ,'Joyce Zhou'
# ,'Juliet Wang'
# ,'Justin Zhong'
# ,'Lily Zhang'
# ,'Lucy Lin'
# ,'Moon Li'
# ,'Nancy Guo'
# ,'Rachel Lun'
# ,'Ring Huang'
# ,'Sean Song'
# ,'Sherry Liao'
# ,'Valerie Li')""",connection)
#
# tem = candidate.loc[candidate['candidate_owner_json'].isin(user['id'])]
# tem1 = tem[['id','current_location_id','first_name', 'last_name']].drop_duplicates()
# tem1['candidate_id'] = tem1['id']
# tem1['id'] = tem1['current_location_id'].apply(lambda x: int(str(x).split('.')[0]))
# tem1['city'] = 'Shenzhen'
# tem1['country_code'] = 'CN'
# # tem1.to_csv('kos.csv',index=False)
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem1, connection, ['city','country_code' ], ['id', ], 'common_location', mylog)

# %%
candidate = pd.read_sql("""select candidate_id from candidate_group_candidate where candidate_group_id in (
select id from candidate_group where name in ('China Tech Coding 2_量化_24'))""",connection)

# %%
candidate['functional_expertise_id'] = 3535
candidate['insert_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(candidate, connection, ['functional_expertise_id','candidate_id','sub_functional_expertise_id','insert_timestamp' ], ['id', ], 'candidate_functional_expertise', mylog)
vincere_custom_migration.psycopg2_bulk_insert(candidate, connection, ['functional_expertise_id','candidate_id','insert_timestamp' ], 'candidate_functional_expertise')

