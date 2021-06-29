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
cf.read('pj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# csv_path = r'd:\vincere\data_output\firstcall\data_input\raw_client_file\document\csv'
document_path = r'D:\Tony\File\PJ Consulting'

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect data bases
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre.raw_connection()

engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

# %% extract data
id_mapping = pd.read_csv(os.path.join(standard_file_upload, 'id_mapping.csv'))

company_file = pd.read_csv(os.path.join(standard_file_upload, 'company.csv'))

contact_file = pd.read_csv(os.path.join(standard_file_upload, 'contact_2.csv'))

candidate_file = pd.read_csv(os.path.join(standard_file_upload, 'candidate_2.csv'))


job_file = pd.read_csv(os.path.join(standard_file_upload, 'job_2.csv'))


################################################
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
id_mapping['matcher'] = id_mapping['ID']
temp_msg_metadata['matcher'] = temp_msg_metadata['root'].apply(lambda x: x.split('\\')[4].split('-')[0])
temp_msg_metadata = temp_msg_metadata.loc[~temp_msg_metadata['file'].str.contains('image00')]

# %% company
company_file['matcher'] = company_file['company-externalId']
company = company_file[['matcher']].merge(id_mapping, on='matcher')
company['DOC_ID'] = company['DOC_ID'].astype(str)
company = company.merge(temp_msg_metadata, left_on='DOC_ID', right_on='matcher')
company = company.drop_duplicates()

company['file_name'] = company['alter_file2']
company['external_id'] = company['ID'].map(lambda x: str(x))
company = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(company, ddbconn)

# %% contact
contact_file['matcher'] = contact_file['contact-externalId']
contact = contact_file[['matcher']].merge(id_mapping, on='matcher')
contact['DOC_ID'] = contact['DOC_ID'].astype(str)
contact = contact.merge(temp_msg_metadata, left_on='DOC_ID', right_on='matcher')
contact = contact.drop_duplicates()

contact['file_name'] = contact['alter_file2']
contact['external_id'] = contact['ID'].map(lambda x: str(x))
contact = vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(contact, ddbconn)

# %% job
job_file['matcher'] = job_file['position-externalId']
job = job_file[['matcher']].merge(id_mapping, on='matcher')
job['DOC_ID'] = job['DOC_ID'].astype(str)
job = job.merge(temp_msg_metadata, left_on='DOC_ID', right_on='matcher')
job = job.drop_duplicates()

job['file_name'] = job['alter_file2']
job['external_id'] = job['ID'].map(lambda x: str(x))
job = vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(job, ddbconn)

# %% candidate
candidate_file['matcher'] = candidate_file['candidate-externalId']
candidate = candidate_file[['matcher']].merge(id_mapping, on='matcher')
candidate['DOC_ID'] = candidate['DOC_ID'].astype(str)
candidate = candidate.merge(temp_msg_metadata, left_on='DOC_ID', right_on='matcher')
candidate = candidate.drop_duplicates()

candidate['file_name'] = candidate['alter_file2']
candidate['external_id'] = candidate['ID'].map(lambda x: str(x))
candidate = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(candidate, ddbconn)

# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.ap-southeast-2.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(company, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(contact, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(job, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

assert False
# %% rename uploaded file
vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(contact[['alter_file2', 'file']], left_on='uploaded_filename', right_on='alter_file2', how='left')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'file']

vindoc = vindoc[['id', 'uploaded_filename']].merge(candidate[['alter_file2', 'file']], left_on='uploaded_filename', right_on='alter_file2', how='left')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'file']

vindoc = vindoc[['id', 'uploaded_filename']].merge(company[['alter_file2', 'file']], left_on='uploaded_filename', right_on='alter_file2', how='left')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'file']

vindoc = vindoc[['id', 'uploaded_filename']].merge(job[['alter_file2', 'file']], left_on='uploaded_filename', right_on='alter_file2', how='left')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'file']

vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename'], ['id'], 'candidate_document', mylog)

job.loc[job['file'].str.contains('Resume')]
