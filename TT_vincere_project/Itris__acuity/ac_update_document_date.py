# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
import datetime
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ac_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

created_date = pd.read_sql("""
select id, insert_timestamp, created, uploaded_filename
from candidate_document
""", engine_postgre)

temp_msg_metadata_comp = pd.read_csv('temp_msg_metadata_comp.csv')
temp_msg_metadata_cts = pd.read_csv('temp_msg_metadata_contact.csv')
temp_msg_metadata_job = pd.read_csv('temp_msg_metadata_job.csv')
temp_msg_metadata_cand = pd.read_csv('temp_msg_metadata_cand.csv')
temp_msg_metadata = pd.concat([temp_msg_metadata_comp, temp_msg_metadata_cts, temp_msg_metadata_job, temp_msg_metadata_cand])
assert False
created_date = created_date.merge(temp_msg_metadata[['file', 'CREATED_ON']], left_on='uploaded_filename', right_on='file')
created_date = created_date.drop_duplicates()
created_date['created'] = created_date['CREATED_ON']
created_date_2 = created_date[['id','created']]
created_date_2 = created_date_2.drop_duplicates().dropna()
created_date_2['created'] = pd.to_datetime(created_date_2['created'])
vincere_custom_migration.psycopg2_bulk_update_tracking(created_date_2, engine_postgre.raw_connection(), ['created', ], ['id', ], 'candidate_document', mylog)
assert False
# %% update job title
# created_date['created'] = created_date['insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_update_tracking(created_date, engine_postgre.raw_connection(), ['created', ], ['id', ], 'candidate_document', mylog)

