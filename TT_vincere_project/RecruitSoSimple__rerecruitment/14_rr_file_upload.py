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
cf.read('rr_config.ini')
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
document_path = r'D:\Tony\File\RSSFile\FINAL RE RSS Dump 15.06.21\Documents'
assert False
# %% extract data
cand = pd.read_sql("""
select * from Documents
""", engine_mssql)

cand['matcher'] = cand['Filename'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', str(x))).lower())
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
temp_msg_metadata['matcher'] = temp_msg_metadata['file'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', str(x))).lower())
temp_msg_metadata = temp_msg_metadata.merge(cand, on='matcher')
assert False
# %% transform
temp_msg_metadata_client = temp_msg_metadata.loc[temp_msg_metadata['Client'].notnull()]
temp_msg_metadata_client['file_name'] = temp_msg_metadata_client['alter_file2']
temp_msg_metadata_client['external_id'] = 'RSS'+temp_msg_metadata_client['Client'].astype(str)
temp_msg_metadata_client['uploaded_filename'] = temp_msg_metadata_client['Original Filename']

temp_msg_metadata_contact = temp_msg_metadata.loc[temp_msg_metadata['Contact'].notnull()]
temp_msg_metadata_contact['file_name'] = temp_msg_metadata_contact['alter_file2']
temp_msg_metadata_contact['external_id'] = 'RSS'+temp_msg_metadata_contact['Contact'].astype(str)
temp_msg_metadata_contact['uploaded_filename'] = temp_msg_metadata_contact['Original Filename']

temp_msg_metadata_job = temp_msg_metadata.loc[temp_msg_metadata['Vacancy'].notnull()]
temp_msg_metadata_job['file_name'] = temp_msg_metadata_job['alter_file2']
temp_msg_metadata_job['external_id'] = 'RSS'+temp_msg_metadata_job['Vacancy'].astype(str)
temp_msg_metadata_job['uploaded_filename'] = temp_msg_metadata_job['Original Filename']

temp_msg_metadata_cand = temp_msg_metadata.loc[temp_msg_metadata['Candidate'].notnull()]
temp_msg_metadata_cand['file_name'] = temp_msg_metadata_cand['alter_file2']
temp_msg_metadata_cand['external_id'] = 'RSS'+temp_msg_metadata_cand['Candidate'].astype(str)
temp_msg_metadata_cand['uploaded_filename'] = temp_msg_metadata_cand['Original Filename']
temp_msg_metadata_cand['Type'].unique()
temp_msg_metadata_cand.loc[temp_msg_metadata_cand['Type']=='Agency CV', 'primary_document'] =1
temp_msg_metadata_cand.loc[temp_msg_metadata_cand['Type']=='Original CV', 'primary_document'] =1
temp_msg_metadata_cand['primary_document'] = temp_msg_metadata_cand['primary_document'].fillna(0)
temp_msg_metadata_cand['primary_document'] = temp_msg_metadata_cand['primary_document'].astype(int)

# assert False
# %% company
company_file = vincere_custom_migration.insert_candidate_documents_company(temp_msg_metadata_client, ddbconn, dest_db, mylog)
company_file = company_file.drop_duplicates()
company_file.to_csv('company_file_uploaded_prod.csv',index=False)
# %% contact
contact_file = vincere_custom_migration.insert_candidate_documents_contact(temp_msg_metadata_contact, ddbconn, dest_db, mylog)
contact_file = contact_file.drop_duplicates()
contact_file.to_csv('contact_file_uploaded_prod.csv',index=False)
# %% job
job_file = vincere_custom_migration.insert_candidate_documents_job(temp_msg_metadata_job, ddbconn, dest_db, mylog)
job_file = job_file.drop_duplicates()
job_file.to_csv('job_file_uploaded_prod.csv',index=False)
# %% candidate
candidate_file = vincere_custom_migration.insert_candidate_documents_candidate(temp_msg_metadata_cand, ddbconn, dest_db, mylog)
candidate_file = candidate_file.drop_duplicates()
candidate_file.to_csv('candidate_file_uploaded_prod.csv',index=False)
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
# %% note
tem = pd.concat([company_file[['saved_filename','Notes']], contact_file[['saved_filename','Notes']], job_file[['saved_filename','Notes']], candidate_file[['saved_filename','Notes']]])
tem = tem.where(tem.notnull(),None)
tem = tem.drop_duplicates()
tem = tem.loc[tem['Notes'].notnull()]

vindoc = pd.read_sql("select id, saved_filename from candidate_document",ddbconn)
vindoc = vindoc.merge(tem, on='saved_filename')
vindoc = vindoc.drop_duplicates()
vindoc['note'] = vindoc['Notes']
vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['note'], ['id'], mylog)

# %% update created date
com = pd.read_csv('company_file_uploaded_prod.csv')
cont = pd.read_csv('contact_file_uploaded_prod.csv')
job = pd.read_csv('job_file_uploaded_prod.csv')
cand = pd.read_csv('candidate_file_uploaded_prod.csv')
tem = pd.concat([com[['saved_filename','Filename']], cont[['saved_filename','Filename']], job[['saved_filename','Filename']], cand[['saved_filename','Filename']]])
tem['date'] = tem['Filename'].apply(lambda x: x[0:10])
tem['created'] = pd.to_datetime(tem['date'], format='%d-%m-%Y')

vindoc = pd.read_sql("select id, saved_filename from candidate_document",ddbconn)
vindoc = vindoc.merge(tem, on='saved_filename')
vindoc = vindoc.drop_duplicates()
vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['created'], ['id'], mylog)

# %% primary CV
com = pd.read_csv('company_file_uploaded_prod.csv')
cont = pd.read_csv('contact_file_uploaded_prod.csv')
job = pd.read_csv('job_file_uploaded_prod.csv')
cand = pd.read_csv('candidate_file_uploaded_prod.csv')
tem = pd.concat([com[['saved_filename','Type']], cont[['saved_filename','Type']], job[['saved_filename','Type']], cand[['saved_filename','Type']]])
tem.loc[tem['Type']=='Original CV', 'primary_document'] =1


vindoc = pd.read_sql("select id, saved_filename from candidate_document",ddbconn)
vindoc = vindoc.merge(tem, on='saved_filename')
vindoc = vindoc.drop_duplicates()
tem2 = vindoc[['id','primary_document']].dropna()
tem2['primary_document'] = tem2['primary_document'].astype(int)
vincere_custom_migration.load_data_to_vincere(tem2, dest_db, 'update', 'candidate_document', ['primary_document'], ['id'], mylog)



doc = pd.read_sql("select id, candidate_id, primary_document from candidate_document where candidate_id is not null and candidate_id != 0",ddbconn)
doc['rn'] = doc.groupby('candidate_id').cumcount()
a = doc.loc[doc['rn']==0]
b = a.loc[a['primary_document']>0]
c = a.loc[~a['candidate_id'].isin(b['candidate_id'])]
