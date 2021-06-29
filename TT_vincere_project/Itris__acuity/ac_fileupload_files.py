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
ddbconn = engine_postgre.raw_connection()

engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %% extract data
file1 = pd.read_sql("""
select APP_ID, COMPANY_ID, CONTACT_ID, JOB_ID, DOC_PATH, ATTACHED as CREATED_ON from Documents;
""", engine_mssql)
file2 = pd.read_sql("""
select ApplicantId as APP_ID, DocumentPath as DOC_PATH, AttachedDateTime as CREATED_ON from Document;
""", engine_mssql)
file3 = pd.read_sql("""
select APP_ID, ORIGINAL_PATH as DOC_PATH, ORIGINAL_DATE as CREATED_ON from ApplicantCV;
""", engine_mssql)
file4 = pd.read_sql("""
select APP_ID, FILE_PATH as DOC_PATH, CREATED_ON from ApplicantFile;
""", engine_mssql)
file = pd.concat([file1, file2, file3, file4])
file = file.drop_duplicates()
# file = file.dropna()

document_path = r'D:\Tony\File\Acuity Consultant\ITRISFILES-2019-08-19\ITRISFILES'
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
temp_msg_metadata['folder'] = temp_msg_metadata['root'].map(lambda x: x.split('\\')[-1])
temp_msg_metadata['file_id'] = temp_msg_metadata[['folder', 'file']].apply(lambda x: '\\' + '\\'.join(x), axis=1)
temp_msg_metadata = temp_msg_metadata.merge(file, left_on='file_id', right_on='DOC_PATH')
temp_msg_metadata = temp_msg_metadata.drop_duplicates()
temp_msg_metadata['file_name'] = temp_msg_metadata['alter_file2']
# assert False
company = temp_msg_metadata.drop(['APP_ID', 'CONTACT_ID', 'JOB_ID'], axis=1)
company = company.dropna()
company['external_id'] = company['COMPANY_ID']
company.to_csv('temp_msg_metadata_comp.csv')

contact = temp_msg_metadata.drop(['APP_ID', 'COMPANY_ID', 'JOB_ID'], axis=1)
contact = contact.dropna()
contact['external_id'] = contact['CONTACT_ID']
contact.to_csv('temp_msg_metadata_contact.csv')

job = temp_msg_metadata.drop(['APP_ID', 'CONTACT_ID', 'COMPANY_ID'], axis=1)
job = job.dropna()
job['external_id'] = job['JOB_ID']
job.to_csv('temp_msg_metadata_job.csv')

candidate = temp_msg_metadata.drop(['COMPANY_ID', 'CONTACT_ID', 'JOB_ID'], axis=1)
candidate = candidate.dropna()
candidate['external_id'] = candidate['APP_ID']
candidate.to_csv('temp_msg_metadata_cand.csv')

#%% company
company_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(company, ddbconn)

# %% contact
contact_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(contact, ddbconn)

# %% job
job_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(job, ddbconn)

# %% candidate
candidate_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(candidate, ddbconn)

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
#
assert False
# %% rename uploaded file
temp_msg_metadata_comp = pd.read_csv('temp_msg_metadata_comp.csv')
temp_msg_metadata_cts = pd.read_csv('temp_msg_metadata_contact.csv')
temp_msg_metadata_job = pd.read_csv('temp_msg_metadata_job.csv')
temp_msg_metadata_cand = pd.read_csv('temp_msg_metadata_cand.csv')
temp_msg_metadata = pd.concat([temp_msg_metadata_comp, temp_msg_metadata_cts, temp_msg_metadata_job, temp_msg_metadata_cand])
vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
# assert False
vindoc = vindoc[['id', 'uploaded_filename']].merge(temp_msg_metadata[['alter_file2', 'DOC_PATH', 'CREATED_ON']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'DOC_PATH']
vindoc.loc[vindoc['alter_file2'].notnull(), 'insert_timestamp'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'CREATED_ON']
vindoc = vindoc.drop_duplicates()
vindoc['uploaded_filename'] = vindoc['uploaded_filename'].map(lambda x: x[3:])
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename','insert_timestamp'], ['id'], 'candidate_document', mylog)


