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
cf.read('ca_config.ini')
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
document_path = r'D:\Tony\File\CastleDoc\20210322'

# %% extract data
client = pd.read_sql("""
select Client_Id, File_Name, Code, Default_Remote_Folder
from Client_Linked_Files clf
left join Linked_File_Type lft on clf.Type_Id = lft.Type_Id
where File_Name is not null
""", engine_mssql)
client['Client_Id'] = client['Client_Id'].astype(str)

vr = pd.read_sql("""
select Reference_Id, Reference_Type, File_Name, Default_Remote_Folder
from Linked_File lf
left join Linked_File_Type lft on lf.Type_Id = lft.Type_Id
where Reference_Type = 'VCRL'
and File_Name is not null
""", engine_mssql)
vr['Reference_Id'] = 'VC'+vr['Reference_Id'].astype(str)

br = pd.read_sql("""
select Reference_Id, Reference_Type, File_Name, Default_Remote_Folder
from Linked_File lf
left join Linked_File_Type lft on lf.Type_Id = lft.Type_Id
where Reference_Type = 'BKRL'
and File_Name is not null
""", engine_mssql)
br['Reference_Id'] = 'BK'+br['Reference_Id'].astype(str)

cand = pd.read_sql("""
select Candidate_Id, File_Name, Code, Default_Remote_Folder
from Candidate_Linked_Files clf
left join Linked_File_Type lft on clf.Type_Id = lft.Type_Id
where File_Name is not null
""", engine_mssql)
cand['Candidate_Id'] = cand['Candidate_Id'].astype(str)
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
doc_type = pd.read_sql("""select id as document_types_id, name from document_types""",ddbconn)

temp_msg_metadata['matcher'] = temp_msg_metadata['file_fullpath'].apply(lambda x: x.split('20210322')[1])
temp_msg_metadata['matcher'] = temp_msg_metadata['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

client['matcher'] = client['Default_Remote_Folder'].apply(lambda x: x.replace('d:\GEL_Docs','').replace('d:\Gel_Docs',''))
client['matcher'] = client['matcher'] + client['File_Name']
client['matcher'] = client['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

vr['matcher'] = vr['Default_Remote_Folder'].apply(lambda x: x.replace('d:\GEL_Docs','').replace('d:\Gel_Docs',''))
vr['matcher'] = vr['matcher'] + vr['File_Name']
vr['matcher'] = vr['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

br['matcher'] = br['Default_Remote_Folder'].apply(lambda x: x.replace('d:\GEL_Docs','').replace('d:\Gel_Docs',''))
br['matcher'] = br['matcher'] + br['File_Name']
br['matcher'] = br['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

job = pd.concat([vr,br])

cand['matcher'] = cand['Default_Remote_Folder'].apply(lambda x: x.replace('d:\GEL_Docs','').replace('d:\Gel_Docs',''))
cand['matcher'] = cand['matcher'] + cand['File_Name']
cand['matcher'] = cand['matcher'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

temp_msg_metadata_client = temp_msg_metadata.merge(client, on='matcher')
temp_msg_metadata_client = temp_msg_metadata_client.drop_duplicates()
temp_msg_metadata_client.loc[temp_msg_metadata_client['Client_Id']=='1925']
temp_msg_metadata_client['file_name'] = temp_msg_metadata_client['alter_file2']
temp_msg_metadata_client['external_id'] = temp_msg_metadata_client['Client_Id']
temp_msg_metadata_client['uploaded_filename'] = temp_msg_metadata_client['File_Name']

temp_msg_metadata_job = temp_msg_metadata.merge(job, on='matcher')
temp_msg_metadata_job = temp_msg_metadata_job.drop_duplicates()
temp_msg_metadata_job.loc[temp_msg_metadata_job['Reference_Id']=='VC2744']
temp_msg_metadata_job['file_name'] = temp_msg_metadata_job['alter_file2']
temp_msg_metadata_job['external_id'] = temp_msg_metadata_job['Reference_Id']
temp_msg_metadata_job['uploaded_filename'] = temp_msg_metadata_job['File_Name']

temp_msg_metadata_cand = temp_msg_metadata.merge(cand, on='matcher')
temp_msg_metadata_cand = temp_msg_metadata_cand.drop_duplicates()
temp_msg_metadata_cand.loc[temp_msg_metadata_cand['Candidate_Id']=='12962']
temp_msg_metadata_cand['file_name'] = temp_msg_metadata_cand['alter_file2']
temp_msg_metadata_cand['external_id'] = temp_msg_metadata_cand['Candidate_Id']
temp_msg_metadata_cand['uploaded_filename'] = temp_msg_metadata_cand['File_Name']
temp_msg_metadata_cand = temp_msg_metadata_cand.merge(doc_type, left_on='Code', right_on='name', how='left')
temp_msg_metadata_cand.loc[temp_msg_metadata_cand['Code']=='CV', 'primary_document'] =1
temp_msg_metadata_cand.loc[temp_msg_metadata_cand['Code']=='CV folder', 'primary_document'] =1
temp_msg_metadata_cand['primary_document'] = temp_msg_metadata_cand['primary_document'].fillna(0)

# assert False
# %% company
company_file = vincere_custom_migration.insert_candidate_documents_company(temp_msg_metadata_client, ddbconn, dest_db, mylog)
company_file = company_file.drop_duplicates()
company_file.to_csv('company_file_uploaded_prod.csv',index=False)
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

s3_add_thread_pool.upload_multi_files_parallelism_1_2(job_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(candidate_file, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

assert False
# %% rename uploaded file
# temp_msg_metadata_job['Code'] = None
# tem = pd.concat([temp_msg_metadata_client[['alter_file2','File_Name','Code']], temp_msg_metadata_job[['alter_file2','File_Name','Code']], temp_msg_metadata_cand[['alter_file2','File_Name','Code']]])
# tem = tem.where(tem.notnull(),None)
# tem = tem.drop_duplicates()
#
# vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
# vindoc = vindoc.merge(tem, left_on='uploaded_filename', right_on='alter_file2', how='left')
# vindoc = vindoc.drop_duplicates()
# vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'File_Name']
# vindoc['primary_document'] = 0
# vindoc.loc[vindoc['Code']=='CV','primary_document'] = 1
# # vindoc.loc[vindoc['Code']=='CV']
# vindoc.to_csv('vindoc.csv')
# vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['uploaded_filename','primary_document'], ['id'], mylog)

# %% rename uploaded file
# vindoc = pd.read_csv('vindoc.csv')
# vindoc['created'] = pd.to_datetime(vindoc['created'])
# vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['uploaded_filename','created'], ['id'], mylog)
