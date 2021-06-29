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
document_path = r'D:\Tony\project\youconnect_TungDownload'
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre.raw_connection()

# %% extract data
file = pd.read_sql("""
select Id, ParentId as entityid, Name, CreatedDate from Attachment where IsDeleted=0;
""", engine_sqlite)
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
# assert False

temp_msg_metadata = temp_msg_metadata.merge(file, left_on='file', right_on='Id')
temp_msg_metadata['extension_file'] = temp_msg_metadata['Name'].map(lambda x: re.search(r"\.[a-zA-Z0-9_ ]*(?!.*\.[a-zA-Z0-9_ ]*)", x))
temp_msg_metadata['extension_file'] = temp_msg_metadata['extension_file'].apply(lambda x: x.group() if x else x)

# %% transform data
temp_msg_metadata['alter_file2'] = temp_msg_metadata[['alter_file2', 'extension_file']].apply(lambda x: ''.join([e for e in x if e]), axis=1)
temp_msg_metadata['file_name'] = temp_msg_metadata['alter_file2']
temp_msg_metadata['external_id'] = temp_msg_metadata['entityid'].map(lambda x: str(x))

# %% company
company_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(temp_msg_metadata, ddbconn)

# %% contact
contact_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(temp_msg_metadata, ddbconn)

# %% job
job_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(temp_msg_metadata, ddbconn)

# %% candidate
candidate_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(temp_msg_metadata, ddbconn)

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

# assert False
# %% rename uploaded file
vindoc = pd.read_sql("select id, uploaded_filename, created from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename', 'created']].merge(temp_msg_metadata[['alter_file2', 'Name','CreatedDate']], left_on='uploaded_filename', right_on='alter_file2', how='left')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'Name']
vindoc.loc[vindoc['alter_file2'].notnull(), 'created'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'CreatedDate']
vindoc['created'] = pd.to_datetime(vindoc['created'])
# vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'created'], ['id'], 'candidate_document', mylog)
vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['uploaded_filename', 'created'], ['id'], mylog)
