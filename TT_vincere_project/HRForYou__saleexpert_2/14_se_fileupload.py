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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('review_db')]
mylog = log.get_info_logger(log_file)
document_path = r'D:\Tony\SalesExpert_PROD\data'
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %% extract data
file_cand = pd.read_sql("""
select user_id, dateiname, dateiname_original, zeitpunkt from user_upload_files
""", engine)

file_job = pd.read_sql("""
select projekt_id, dateiname, real_dateiname, dateizeit from projekte_dokumente
""", engine)

file_comp = pd.read_sql("""
select referenz_id, dateiname, dateiname_original, zeitpunkt from upload_files
""", engine)

temp_msg_metadata = vincere_common.get_folder_structure(document_path)
# assert False
# %% extract data
temp_msg_metadata['matcher'] = temp_msg_metadata['file'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
file_cand['matcher'] = file_cand['dateiname'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
file_job['matcher'] = file_job['dateiname'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
file_comp['matcher'] = file_comp['dateiname'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

temp_msg_metadata_cand = temp_msg_metadata.merge(file_cand, on='matcher')
temp_msg_metadata_job = temp_msg_metadata.merge(file_job, on='matcher')
temp_msg_metadata_comp = temp_msg_metadata.merge(file_comp, on='matcher')

temp_msg_metadata_comp['file_name'] = temp_msg_metadata_comp['alter_file2']
temp_msg_metadata_comp['external_id'] = temp_msg_metadata_comp['referenz_id']
temp_msg_metadata_comp['external_id'] = temp_msg_metadata_comp['external_id'].astype(str)
temp_msg_metadata_comp['external_id'] = 'SE'+temp_msg_metadata_comp['external_id']

temp_msg_metadata_cand['file_name'] = temp_msg_metadata_cand['alter_file2']
temp_msg_metadata_cand['external_id'] = temp_msg_metadata_cand['user_id']
temp_msg_metadata_cand['external_id'] = temp_msg_metadata_cand['external_id'].astype(str)
temp_msg_metadata_cand['external_id'] = 'SE'+temp_msg_metadata_cand['external_id']

temp_msg_metadata_job['file_name'] = temp_msg_metadata_job['alter_file2']
temp_msg_metadata_job['external_id'] = temp_msg_metadata_job['projekt_id']
temp_msg_metadata_job['external_id'] = temp_msg_metadata_job['external_id'].astype(str)
temp_msg_metadata_job['external_id'] = 'SE'+temp_msg_metadata_job['external_id']

temp_msg_metadata_cand = temp_msg_metadata_cand.drop_duplicates()
temp_msg_metadata_job = temp_msg_metadata_job.drop_duplicates()
temp_msg_metadata_comp = temp_msg_metadata_comp.drop_duplicates()
assert False
# %% company
company_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(temp_msg_metadata_comp, connection)

# %% contact
# contact_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(temp_msg_metadata, connection)

# %% job
job_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(temp_msg_metadata_job, connection)

# %% candidate
candidate_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate_v2(temp_msg_metadata_cand, dest_db, connection, mylog)
#
# df = temp_msg_metadata_cand
# db_conn = connection
# logger = mylog
# conn_param = dest_db
#
# df_candidate = pd.read_sql("""select id as entity_id, external_id from candidate""", db_conn)
# df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
# df = df.merge(df_candidate, left_on='external_id', right_on='external_id')
# tem = df[['file_name','entity_id']]
# tem['document_type'] = 'resume'
# tem['entity_type'] = 'CANDIDATE'
# tem.info()
# vincere_custom_migration.load_data_to_vincere(tem, conn_param, 'insert', 'bulk_upload_document_mapping', ['file_name', 'entity_type', 'document_type', 'entity_id'], [], logger)
assert False

# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.eu-central-1.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(company_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host
#
# s3_add_thread_pool.upload_multi_files_parallelism_1_2(contact_file, 'file', 'alter_file2', 'root'
#                                                       , bucket=s3_bucket
#                                                       , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(job_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host
company_file.to_csv('company_file')
job_file.to_csv('job_file')
candidate_file.to_csv('candidate_file')

temp_msg_metadata_job.to_csv('temp_msg_metadata_job')
temp_msg_metadata_cand.to_csv('temp_msg_metadata_cand')
temp_msg_metadata_comp.to_csv('temp_msg_metadata_comp')
assert False
# %% rename uploaded file
tem1 = temp_msg_metadata_job[['alter_file2','real_dateiname','dateizeit']].rename(columns={'real_dateiname':'real_name'})
tem2 = temp_msg_metadata_cand[['alter_file2','dateiname_original','zeitpunkt']].rename(columns={'dateiname_original':'real_name','zeitpunkt':'dateizeit'})
tem3 = temp_msg_metadata_comp[['alter_file2','dateiname_original','zeitpunkt']].rename(columns={'dateiname_original':'real_name','zeitpunkt':'dateizeit'})
temp_msg_metadata = pd.concat([tem1, tem2, tem3])
# temp_msg_metadata = tem3
vindoc = pd.read_sql("select id, uploaded_filename, created from candidate_document",connection)
vindoc = vindoc[['id', 'uploaded_filename', 'created']].merge(temp_msg_metadata, left_on='uploaded_filename', right_on='alter_file2', how='left')
vindoc = vindoc.drop_duplicates()
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'real_name']
vindoc.loc[vindoc['alter_file2'].notnull(), 'created'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'dateizeit']
vindoc['created'] = pd.to_datetime(vindoc['created'])
# vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'created'], ['id'], 'candidate_document', mylog)
vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['uploaded_filename','created'], ['id'], mylog)