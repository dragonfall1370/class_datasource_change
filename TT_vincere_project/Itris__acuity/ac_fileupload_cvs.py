# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy

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

# %% extract data
file1 = pd.read_sql("""
select APP_ID as external_id, ORIGINAL_PATH as FILE_PATH, ORIGINAL_DATE as CREATED_ON from ApplicantCV;
""", engine_mssql)
file2 = pd.read_sql("""
select APP_ID as external_id, FILE_PATH, CREATED_ON from ApplicantFile;
""", engine_mssql)
file = pd.concat([file1, file2])
file = file.dropna()
file['FILE_PATH'] = file['FILE_PATH'].apply(lambda x: x[3:])

document_path = r'D:\Tony\File\Acuity Consultant\ITRISFILES-2019-08-19\ITRISFILES'
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
temp_msg_metadata['file_id'] = temp_msg_metadata['file']
temp_msg_metadata = temp_msg_metadata.merge(file, left_on='file_id', right_on='FILE_PATH')

# %% transform data
temp_msg_metadata['file_name'] = temp_msg_metadata['alter_file2']
temp_msg_metadata.loc[temp_msg_metadata['external_id'] == 'HQ00018608']
# temp_msg_metadata.to_csv('temp_msg_metadata.csv')
# # assert False
# # %% candidate
# candidate_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(temp_msg_metadata, engine_postgre.raw_connection())
# candidate_file.to_csv('candidate_file.csv')
# candidate_file.dropna()
# # assert False
#
# # %% upload files to s3
# s3_bucket = cf['default'].get('s3_bucket')
# s3_key = cf['default'].get('s3_key')
# REGION_HOST = 's3.eu-central-1.amazonaws.com'
#
# from common import s3_add_thread_pool
# s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'alter_file2', 'root'
#                                                       , bucket=s3_bucket
#                                                       , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host
#
# assert False
# # %% rename uploaded file
# temp_msg_metadata = pd.read_csv('temp_msg_metadata.csv')
# vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",engine_postgre.raw_connection())
# vindoc = vindoc[['id', 'uploaded_filename']].merge(temp_msg_metadata[['alter_file2', 'FILE_PATH']], left_on='uploaded_filename', right_on='alter_file2', how='left')
# vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'FILE_PATH']
# vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, engine_postgre.raw_connection(), ['uploaded_filename'], ['id'], 'candidate_document', mylog)
#
# # %% set primary docs
# vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, engine_postgre.raw_connection(), ['uploaded_filename'], ['id'], 'candidate_document', mylog)

assert False
vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",engine_postgre.raw_connection())
vindoc = vindoc[['id', 'uploaded_filename']].merge(temp_msg_metadata[['file']], left_on='uploaded_filename', right_on='file')
vindoc = vindoc.drop_duplicates()
vindoc = vindoc.dropna()
vindoc['primary_document'] = 1
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, engine_postgre.raw_connection(), ['primary_document'], ['id'], 'candidate_document', mylog)

# # %% insert timestamp

# vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",engine_postgre.raw_connection())
# vindoc = vindoc[['id', 'uploaded_filename']].merge(temp_msg_metadata[['external_id', 'FILE_PATH', 'CREATED_ON']], left_on='uploaded_filename', right_on='FILE_PATH')
# vindoc = vindoc.drop_duplicates()
# vindoc = vindoc.dropna()
# vindoc.loc[vindoc['uploaded_filename'].notnull(), 'insert_timestamp'] = vindoc.loc[vindoc['uploaded_filename'].notnull(), 'CREATED_ON']
# vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, engine_postgre.raw_connection(), ['insert_timestamp'], ['id'], 'candidate_document', mylog)
