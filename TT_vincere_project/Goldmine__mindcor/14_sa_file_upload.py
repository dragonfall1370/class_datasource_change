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
cf.read('mc_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()
document_path = r'D:\Tony\File\StepAhead_Prod'

# %% extract data
doc = pd.read_sql("""
select concat(Document_name,'.',d.Document_Type) as readable_filename, concat(d.Reference,'.',d.Document_Type) as file_name, Date_Added, dl.Reference
from Documents d
join Document_Lookup dl on dl.Document_Reference = d.Reference
""", engine_mssql)
doc['Reference'] = doc['Reference'].astype(str)
temp_msg_metadata = vincere_common.get_folder_structure(document_path)

assert False
doc['matcher'] = doc['file_name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
temp_msg_metadata['matcher'] = temp_msg_metadata['file'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
temp_msg_metadata = temp_msg_metadata.merge(doc, on='matcher')
temp_msg_metadata = temp_msg_metadata.drop_duplicates()
temp_msg_metadata['file_name'] = temp_msg_metadata['alter_file2']
temp_msg_metadata['external_id'] = temp_msg_metadata['Reference']

assert False
# %% company
company_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(temp_msg_metadata, ddbconn)
company_file = company_file.drop_duplicates()

# %% contact
external_id = pd.read_sql("""
select external_id from contact where deleted_timestamp is null and external_id is not null and external_id  not like '%DEFAULT%'
""", ddbconn)
external_id['contact_externalid'] = external_id['external_id'].apply(lambda x: x.split('_')[0])
tem = temp_msg_metadata.merge(external_id, left_on='external_id', right_on='contact_externalid')
tem['external_id'] = tem['external_id_y']
contact_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(tem, ddbconn)
contact_file = contact_file.drop_duplicates()

# %% job
job_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(temp_msg_metadata, ddbconn)
job_file = job_file.drop_duplicates()

# %% candidate
candidate_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(temp_msg_metadata, ddbconn)
candidate_file = candidate_file.drop_duplicates()
# assert False
# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.eu-central-1.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(company_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(contact_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(job_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

assert False
# %% rename uploaded file
# tem1 = temp_msg_metadata_cand[['alter_file2','file','CandDocTypeName','CandDocCreateDate']].rename(columns={'CandDocCreateDate':'date'})
# tem2 = temp_msg_metadata_ccj[['alter_file2','file','DCreated']].rename(columns={'DCreated':'date'})
# tem = pd.concat([tem1, tem2])
# tem = tem.where(tem.notnull(),None)
vindoc = pd.read_sql("select id, uploaded_filename, created from candidate_document",ddbconn)
vindoc = vindoc.merge(temp_msg_metadata, left_on='uploaded_filename', right_on='alter_file2', how='left')
vindoc = vindoc.drop_duplicates()
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'readable_filename']
vindoc.loc[vindoc['alter_file2'].notnull(), 'created'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'Date_Added']
vindoc['created'] = pd.to_datetime(vindoc['created'])
vindoc.to_csv('vindoc.csv')
# vindoc.loc[vindoc['CandDocTypeName']!='Original Resumes','primary_document'] = 0
# vindoc.loc[vindoc['CandDocTypeName']=='Original Resumes','primary_document'] = 1
# vindoc['CandDocTypeName'].unique()
# vindoc.loc[vindoc['CandDocTypeName']!='Original Resumes']
# vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'created'], ['id'], 'candidate_document', mylog)
vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['uploaded_filename','created'], ['id'], mylog)

# %% rename uploaded file
vindoc = pd.read_csv('vindoc.csv')
vindoc['created'] = pd.to_datetime(vindoc['created'])
vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['uploaded_filename','created'], ['id'], mylog)
