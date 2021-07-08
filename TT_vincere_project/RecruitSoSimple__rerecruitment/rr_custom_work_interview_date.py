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
import phonenumbers
from dateutil.relativedelta import relativedelta
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('rr_config.ini')
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
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
first_interview = pd.read_sql("""select id as position_candidate_id, position_description_id, candidate_id, associated_date
from position_candidate
where status = 104 """, connection)
first_interview['interview_no'] = 1
first_interview['id'] = first_interview.index
first_interview['insert_timestamp'] = datetime.datetime.now()
first_interview['user_account_id'] = -10

cols_int = ['id','position_candidate_id','position_description_id','candidate_id','interview_no','insert_timestamp','user_account_id','time_zone']
vincere_custom_migration.psycopg2_bulk_insert(first_interview, connection, cols_int, 'interview')

first_interview_date = pd.read_sql("""select id as interview_id, position_description_id, position_candidate_id, candidate_id from interview where interview_no = 1 """, connection)
first_interview_date = first_interview_date.merge(first_interview, on=['position_description_id','position_candidate_id','candidate_id'])
# first_interview_date['interview_id'] = first_interview_date['id']
first_interview_date['interview_date_time_from'] = first_interview_date['associated_date']
first_interview_date['interview_date_time_to'] = first_interview_date['interview_date_time_from']
first_interview_date['creator_id'] = -10
first_interview_date['modifier_id'] = -10
first_interview_date['rescheduled'] = 0
first_interview_date['insert_timestamp'] = datetime.datetime.now()
cols_int_time = ['interview_id','interview_date_time_from','insert_timestamp','interview_date_time_to','creator_id','modifier_id','rescheduled','interview_no']
vincere_custom_migration.psycopg2_bulk_insert(first_interview_date, connection, cols_int_time, 'interview_history')

# %%
second_interview = pd.read_sql("""select id as position_candidate_id, position_description_id, candidate_id, associated_date
from position_candidate
where status = 105 """, connection)
max_id = pd.read_sql("""select max(id) from interview""", connection)
second_interview['interview_no'] = 2
second_interview['id'] = max_id['max'].values[0]+1+second_interview.index
second_interview['insert_timestamp'] = datetime.datetime.now()
second_interview['user_account_id'] = -10

cols_int = ['id','position_candidate_id','position_description_id','candidate_id','interview_no','insert_timestamp','user_account_id']
vincere_custom_migration.psycopg2_bulk_insert(second_interview, connection, cols_int, 'interview')

second_interview_date = pd.read_sql("""select id as interview_id, position_description_id, position_candidate_id, candidate_id from interview where interview_no = 2 """, connection)
second_interview_date = second_interview_date.merge(second_interview, on=['position_description_id','position_candidate_id','candidate_id'])
# first_interview_date['interview_id'] = first_interview_date['id']
second_interview_date['interview_date_time_from'] = second_interview_date['associated_date']
second_interview_date['interview_date_time_to'] = second_interview_date['interview_date_time_from']
second_interview_date['creator_id'] = -10
second_interview_date['modifier_id'] = -10
second_interview_date['rescheduled'] = 0
second_interview_date['insert_timestamp'] = datetime.datetime.now()
cols_int_time = ['interview_id','interview_date_time_from','insert_timestamp','interview_date_time_to','creator_id','modifier_id','rescheduled','interview_no']
vincere_custom_migration.psycopg2_bulk_insert(second_interview_date, connection, cols_int_time, 'interview_history')