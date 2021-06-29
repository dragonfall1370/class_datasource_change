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
cf.read('en_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
# data_folder = '/Users/truongtung/Desktop'
sqlite_path = cf['default'].get('sqlite_path')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
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
assert False
#%%
cont_grp = pd.read_sql("""select * from contact_group_20201007 where insert_timestamp = '2020-08-19 16:16:06.292223'""", engine_postgre_review)
col_cont_grp = list(cont_grp.columns)
# col_activity_cand.pop()
vincere_custom_migration.psycopg2_bulk_insert_tracking(cont_grp, connection,col_cont_grp, 'contact_group', mylog)

cont_grp_cont = pd.read_sql("""select * from contact_group_contact_20201007 where contact_group_id in (
select id from contact_group_20201007 where insert_timestamp = '2020-08-19 16:16:06.292223')""", engine_postgre_review)
col_cont_grp_cont = list(cont_grp_cont.columns)
vincere_custom_migration.psycopg2_bulk_insert_tracking(cont_grp_cont, connection,col_cont_grp_cont, 'contact_group_contact', mylog)

cont_grp_user = pd.read_sql("""select * from contact_group_user_account_20201007 where contact_group_id in (
select id from contact_group_20201007 where insert_timestamp = '2020-08-19 16:16:06.292223')""", engine_postgre_review)
col_cont_grp_user = list(cont_grp_user.columns)
vincere_custom_migration.psycopg2_bulk_insert_tracking(cont_grp_user, connection,col_cont_grp_user, 'contact_group_user_account', mylog)

#%%
cand_grp = pd.read_sql("""select * from candidate_group_20201007 where insert_timestamp = '2020-08-19 17:40:05.834950'""", engine_postgre_review)
col_cand_grp = list(cand_grp.columns)
# col_activity_cand.pop()
vincere_custom_migration.psycopg2_bulk_insert_tracking(cand_grp, connection,col_cand_grp, 'candidate_group', mylog)

cand_grp_cand = pd.read_sql("""select * from candidate_group_candidate_20201007 where candidate_group_id in (
select id from candidate_group_20201007 where insert_timestamp = '2020-08-19 17:40:05.834950')""", engine_postgre_review)
col_cand_grp_cand = list(cand_grp_cand.columns)
vincere_custom_migration.psycopg2_bulk_insert_tracking(cand_grp_cand, connection,col_cand_grp_cand, 'candidate_group_candidate', mylog)

cand_grp_user = pd.read_sql("""select * from candidate_group_user_account_20201007 where candidate_group_id in (
select id from candidate_group_20201007 where insert_timestamp = '2020-08-19 17:40:05.834950')""", engine_postgre_review)
col_cand_grp_user = list(cand_grp_user.columns)
vincere_custom_migration.psycopg2_bulk_insert_tracking(cand_grp_user, connection,col_cand_grp_user, 'candidate_group_user_account', mylog)