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
cf.read('theo_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# csv_path = r'd:\vincere\data_output\firstcall\data_input\raw_client_file\document\csv'
document_path = r'D:\Tony\File\Aug_TheoJamesRecruitmentLimited_12098_BULLHORN11235'

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect data bases
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre.raw_connection()
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

# %% extract data
company_file = pd.read_sql("""
SELECT 'company' as category
, concat('',clientCorporationID) as company_externalid
, concat(F.clientCorporationFileID,F.fileExtension) as UploadedName
, case when right(F.name, charindex('.', reverse(name))) = F.fileExtension then F.Name
	else concat(F.name, F.fileExtension) end as RealName
, F.dateadded as created
from bullhorn1.BH_ClientCorporationFile F 
where F.fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') 
""", engine_mssql)

contact_file = pd.read_sql("""
SELECT 'contact' as Category
, concat('',Cl.clientid) as contact_externalid
, concat(F.clientContactFileID, F.fileExtension) as UploadedName
, case when right(F.name, charindex('.', reverse(name))) = F.fileExtension then F.Name
	else concat(F.name, F.fileExtension) end as RealName
, F.dateadded as created
, 'document' as document_type
from bullhorn1.View_ClientContactFile F 
left join bullhorn1.BH_Client Cl on Cl.userid = F.clientcontactuserid 
where F.fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html')
""", engine_mssql)

candidate_file = pd.read_sql("""
SELECT 'candidateUserID' as Category
		, concat('', c.candidateid) as candidate_externalid
		, concat(F.candidateFileID, F.fileExtension) as UploadedName
		, case when right(F.name, charindex('.', reverse(F.name))) = F.fileExtension then F.Name
		else concat(F.name, F.fileExtension) end as RealName
		, F.dateadded as created
		from bullhorn1.View_CandidateFile F 
		left join bullhorn1.Candidate C on C.userid = F.candidateuserid
		WHERE F.fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html')
union
SELECT 'placementUserID'
	, concat('', c.candidateid) as candidate_externalid
	, concat(placementFileID, fileExtension) as UploadedName
	, case when right(F.name, charindex('.', reverse(F.name))) = F.fileExtension then F.Name
		else concat(F.name, F.fileExtension) end as RealName
		, F.dateadded as created
	from bullhorn1.View_PlacementFile F
	left join bullhorn1.Candidate C on C.userid = F.userid
	WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html')
""", engine_mssql)


job_file = pd.read_sql("""
SELECT 'job' as Category
, concat('',jobPostingID) as job_externalid
, concat(jobPostingFileID, fileExtension) as UploadedName
, case when right(F.name, charindex('.', reverse(name))) = F.fileExtension then F.Name
	else concat(F.name, F.fileExtension) end as RealName
, F.dateadded as created
, 'job_description' as document_type 
from bullhorn1.View_JobPostingFile F
where fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html')
""", engine_mssql)

assert False
################################################
temp_msg_metadata = vincere_common.get_folder_structure(document_path)
temp_msg_metadata['extension_file'] = temp_msg_metadata['extension_file'].apply(lambda x: x.group() if x else x)
temp_msg_metadata['matcher'] = temp_msg_metadata.file.str.lower()

# %% company
company_file['matcher'] = company_file.UploadedName.str.lower()
company_file = company_file.merge(temp_msg_metadata, on='matcher')
company_file['file_name'] = company_file['alter_file2']
company_file['external_id'] = company_file['company_externalid'].map(lambda x: str(x))
company_file = company_file.drop_duplicates()
company_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(company_file, ddbconn)

# %% contact
contact_file['matcher'] = contact_file.UploadedName.str.lower()
contact_file = contact_file.merge(temp_msg_metadata, on='matcher')
contact_file['file_name'] = contact_file['alter_file2']
contact_file['external_id'] = contact_file['contact_externalid'].map(lambda x: str(x))
contact_file = contact_file.drop_duplicates()
contact_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(contact_file, ddbconn)

# %% job
job_file['matcher'] = job_file.UploadedName.str.lower()
job_file = job_file.merge(temp_msg_metadata, on='matcher')
job_file['file_name'] = job_file['alter_file2']
job_file['external_id'] = job_file['job_externalid'].map(lambda x: str(x))
job_file = job_file.drop_duplicates()
job_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(job_file, ddbconn)

# %% candidate
candidate_file['matcher'] = candidate_file.UploadedName.str.lower()
candidate_file = candidate_file.merge(temp_msg_metadata, on='matcher')
candidate_file['file_name'] = candidate_file['alter_file2']
candidate_file['external_id'] = candidate_file['candidate_externalid'].map(lambda x: str(x))
candidate_file = candidate_file.drop_duplicates()
candidate_file = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(candidate_file, ddbconn)

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
vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(contact_file[['alter_file2', 'RealName', 'created']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'RealName']
vindoc.loc[vindoc['alter_file2'].notnull(), 'insert_timestamp'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'created']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)

vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(candidate_file[['alter_file2', 'RealName', 'created']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'RealName']
vindoc.loc[vindoc['alter_file2'].notnull(), 'insert_timestamp'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'created']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)

vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(company_file[['alter_file2', 'RealName', 'created']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'RealName']
vindoc.loc[vindoc['alter_file2'].notnull(), 'insert_timestamp'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'created']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)

vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(job_file[['alter_file2', 'RealName', 'created']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'RealName']
vindoc.loc[vindoc['alter_file2'].notnull(), 'insert_timestamp'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'created']

vindoc.loc[vindoc.RealName.notnull()]

vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)
