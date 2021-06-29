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
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()

document_path_company = r'D:\Tony\File\Loxo\prod\captivate-talent\companies\documents'
document_path_job = r'D:\Tony\File\Loxo\prod\captivate-talent\jobs\documents'
document_path_people = r'D:\Tony\File\Loxo\prod\captivate-talent\people\documents'
resume_path_people = r'D:\Tony\File\Loxo\prod\captivate-talent\people\resumes'
# %% extract data
company = pd.read_sql("""select * from companies_documents""", engine_sqlite)
company['id'] = company['id'].astype(str)

people_doc = pd.read_sql("""select * from people_documents""", engine_sqlite)
people_doc['id'] = people_doc['id'].astype(str)

people_resumes = pd.read_sql("""select people_id as id,* from people_resumes""", engine_sqlite)
people_resumes['id'] = people_resumes['id'].astype(str)

job_doc = pd.read_sql("""select * from job_documents""", engine_sqlite)
job_doc['id'] = job_doc['id'].astype(str)

temp_msg_metadata_company = vincere_common.get_folder_structure(document_path_company)
temp_msg_metadata_pdoc = vincere_common.get_folder_structure(document_path_people)
temp_msg_metadata_job = vincere_common.get_folder_structure(document_path_job)
temp_msg_metadata_resume = vincere_common.get_folder_structure(resume_path_people)

assert False
temp_msg_metadata_company['matcher'] = temp_msg_metadata_company['file_fullpath'].apply(lambda x: x.split('captivate-talent')[1])
temp_msg_metadata_company['matcher'] = temp_msg_metadata_company['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

temp_msg_metadata_pdoc['matcher'] = temp_msg_metadata_pdoc['file_fullpath'].apply(lambda x: x.split('captivate-talent')[1])
temp_msg_metadata_pdoc['matcher'] = temp_msg_metadata_pdoc['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

temp_msg_metadata_job['matcher'] = temp_msg_metadata_job['file_fullpath'].apply(lambda x: x.split('captivate-talent')[1])
temp_msg_metadata_job['matcher'] = temp_msg_metadata_job['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

temp_msg_metadata_resume['matcher'] = temp_msg_metadata_resume['file_fullpath'].apply(lambda x: x.split('captivate-talent')[1])
temp_msg_metadata_resume['matcher'] = temp_msg_metadata_resume['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

company['matcher'] = company['path'].apply(lambda x: x.split('captivate-talent')[1])
company['matcher'] = company['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

people_doc['matcher'] = people_doc['path'].apply(lambda x: x.split('captivate-talent')[1])
people_doc['matcher'] = people_doc['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

people_resumes['matcher'] = people_resumes['path'].apply(lambda x: x.split('captivate-talent')[1])
people_resumes['matcher'] = people_resumes['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

job_doc['matcher'] = job_doc['path'].apply(lambda x: x.split('captivate-talent')[1])
job_doc['matcher'] = job_doc['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

# assert False
temp_msg_metadata_client = temp_msg_metadata_company.merge(company, on='matcher')
temp_msg_metadata_client['ext'] = temp_msg_metadata_client['filename'].apply(lambda x: x.split('.')[-1])
temp_msg_metadata_client['alter_file2'] = temp_msg_metadata_client['alter_file2']+'.'+temp_msg_metadata_client['ext']
temp_msg_metadata_client['file_name'] = temp_msg_metadata_client['alter_file2']
temp_msg_metadata_client['external_id'] = temp_msg_metadata_client['id']
temp_msg_metadata_client['uploaded_filename'] = temp_msg_metadata_client['filename']
temp_msg_metadata_client['created'] = pd.to_datetime(temp_msg_metadata_client['upload_date'])

temp_msg_metadata_person_doc = temp_msg_metadata_pdoc.merge(people_doc, on='matcher')
temp_msg_metadata_person_doc['ext'] = temp_msg_metadata_person_doc['filename'].apply(lambda x: x.split('.')[-1])
temp_msg_metadata_person_doc['alter_file2'] = temp_msg_metadata_person_doc['alter_file2']+'.'+temp_msg_metadata_person_doc['ext']
temp_msg_metadata_person_doc['file_name'] = temp_msg_metadata_person_doc['alter_file2']
temp_msg_metadata_person_doc['external_id'] = temp_msg_metadata_person_doc['id']
temp_msg_metadata_person_doc['uploaded_filename'] = temp_msg_metadata_person_doc['filename']
temp_msg_metadata_person_doc['created'] = pd.to_datetime(temp_msg_metadata_person_doc['upload_date'])

temp_msg_metadata_person_re = temp_msg_metadata_resume.merge(people_resumes, on='matcher')
temp_msg_metadata_person_re['ext'] = temp_msg_metadata_person_re['filename'].apply(lambda x: x.split('.')[-1])
temp_msg_metadata_person_re['alter_file2'] = temp_msg_metadata_person_re['alter_file2']+'.'+temp_msg_metadata_person_re['ext']
temp_msg_metadata_person_re['file_name'] = temp_msg_metadata_person_re['alter_file2']
temp_msg_metadata_person_re['external_id'] = temp_msg_metadata_person_re['id']
temp_msg_metadata_person_re['uploaded_filename'] = temp_msg_metadata_person_re['filename']
temp_msg_metadata_person_re['created'] = pd.to_datetime(temp_msg_metadata_person_re['upload_date'])
temp_msg_metadata_person_re['primary_document'] = 1
temp_msg_metadata_person = pd.concat([temp_msg_metadata_person_doc,temp_msg_metadata_person_re])
temp_msg_metadata_person = temp_msg_metadata_person.drop_duplicates()
temp_msg_metadata_person['primary_document'] = temp_msg_metadata_person['primary_document'].fillna(0)
temp_msg_metadata_person['primary_document'] = temp_msg_metadata_person['primary_document'].astype(int)

temp_msg_metadata_job_doc = temp_msg_metadata_job.merge(job_doc, on='matcher')
temp_msg_metadata_job_doc['ext'] = temp_msg_metadata_job_doc['filename'].apply(lambda x: x.split('.')[-1])
temp_msg_metadata_job_doc['alter_file2'] = temp_msg_metadata_job_doc['alter_file2']+'.'+temp_msg_metadata_job_doc['ext']
temp_msg_metadata_job_doc['file_name'] = temp_msg_metadata_job_doc['alter_file2']
temp_msg_metadata_job_doc['external_id'] = temp_msg_metadata_job_doc['id']
temp_msg_metadata_job_doc['uploaded_filename'] = temp_msg_metadata_job_doc['filename']
temp_msg_metadata_job_doc['created'] = pd.to_datetime(temp_msg_metadata_job_doc['upload_date'])

assert False
# %% company
company_file = vincere_custom_migration.insert_candidate_documents_company(temp_msg_metadata_client, ddbconn, dest_db, mylog)
company_file = company_file.drop_duplicates()
# %% contact
df = temp_msg_metadata_person
db_conn = ddbconn
conn_param = dest_db
logger = mylog
df_contact = pd.read_sql("""select id as contact_id, external_id from contact""", db_conn)
df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
df_temp = df.merge(df_contact, left_on='external_id', right_on='external_id')
if not 'document_types_id' in df_temp:
    df_temp['document_types_id'] = 19
df_temp['saved_filename'] = df_temp['file_name']
df_temp['version_no'] = 1
df_temp['successful_parsing_percent'] = 0.0
df_temp['primary_document'] = 0
df_temp['google_viewer'] = -1
df_temp['temporary'] = 0
df_temp['customer_portal'] = 0
df_temp['visible'] = 1
df_temp['entity_type'] = 'CONTACT'
df_temp['insert_timestamp'] = datetime.datetime.now()
df_temp['trigger_index_update_timestamp'] = df_temp['insert_timestamp']
df_temp['document_type'] = 'legacy_contact_document'
if not 'created' in df_temp:
    df_temp['created'] = df_temp['insert_timestamp']
df_temp.loc[(df_temp['created'].isnull()), 'created'] = df_temp['insert_timestamp']
df_temp['created'] = pd.to_datetime(df_temp['created'],utc=True)
col = ['contact_id'
    , 'document_types_id'
    , 'uploaded_filename'
    ,'saved_filename'
    ,'insert_timestamp'
    ,'document_type'
    ,'created'
    ,'trigger_index_update_timestamp'
    ,'version_no'
    ,'successful_parsing_percent'
    ,'primary_document'
    ,'google_viewer'
    ,'temporary'
    ,'customer_portal'
    ,'visible'
    ,'entity_type']
tem = df_temp[col]
vincere_custom_migration.load_data_to_vincere(tem, conn_param, 'insert', 'candidate_document', col, '', logger)
contact_file = df_temp

# contact_file = vincere_custom_migration.insert_candidate_documents_contact(temp_msg_metadata_person, ddbconn, dest_db, mylog)
# contact_file = contact_file.drop_duplicates()
# %% job
job_file = vincere_custom_migration.insert_candidate_documents_job(temp_msg_metadata_job_doc, ddbconn, dest_db, mylog)
job_file = job_file.drop_duplicates()
# %% candidate
df = temp_msg_metadata_person
db_conn = ddbconn
conn_param = dest_db
logger = mylog
df_candidate = pd.read_sql("""select id as candidate_id, external_id from candidate""", db_conn)
df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
df_temp = df.merge(df_candidate, left_on='external_id', right_on='external_id')
if not 'document_types_id' in df_temp:
    df_temp['document_types_id'] = 1
df_temp['saved_filename'] = df_temp['file_name']
df_temp['version_no'] = 1
df_temp['successful_parsing_percent'] = 0.0
if not 'primary_document' in df_temp:
    df_temp['primary_document'] = 0
df_temp['google_viewer'] = -1
df_temp['temporary'] = 0
df_temp['customer_portal'] = 0
df_temp['visible'] = 1
df_temp['entity_type'] = 'CANDIDATE'
df_temp['insert_timestamp'] = datetime.datetime.now()
df_temp['trigger_index_update_timestamp'] = df_temp['insert_timestamp']
df_temp['document_type'] = 'resume'
if not 'created' in df_temp:
    df_temp['created'] = df_temp['insert_timestamp']
df_temp.loc[(df_temp['created'].isnull()), 'created'] = df_temp['insert_timestamp']
df_temp['created'] = pd.to_datetime(df_temp['created'],utc=True)
col = ['candidate_id'
    , 'document_types_id'
    , 'uploaded_filename'
    , 'saved_filename'
    , 'insert_timestamp'
    , 'document_type'
    , 'created'
    , 'trigger_index_update_timestamp'
    , 'version_no'
    , 'successful_parsing_percent'
    , 'primary_document'
    , 'google_viewer'
    , 'temporary'
    , 'customer_portal'
    , 'visible'
    , 'entity_type']
tem = df_temp[col]
vincere_custom_migration.load_data_to_vincere(tem, conn_param, 'insert', 'candidate_document', col, '', logger)
candidate_file = df_temp

# candidate_file = vincere_custom_migration.insert_candidate_documents_candidate(temp_msg_metadata_person, ddbconn, dest_db, mylog)
# candidate_file = candidate_file.drop_duplicates()


# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.us-east-1.amazonaws.com'

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

