
# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import os
import re
import common.s3 as s3
import psycopg2
import numpy as np
import pymssql
import datetime
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_common as vincere_common
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %%
# loading configuration
cf = configparser.RawConfigParser()
cf.read('exede_config.ini')
data_folder = cf['default'].get('data_folder')
upload_folder = cf['default'].get('upload_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(cf['default'].get('log_file'))

# %%
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
pathlib.Path(upload_folder).mkdir(parents=True, exist_ok=True)

# %% connect databases
sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
ddbconn.set_client_encoding('UTF8')

# %% load doc for candidate
df_1 = pd.read_sql("""
    select 'candidate' as entity, d.DOC_ID, APP_ID, DOC_PATH, dc.NAME as Category
    from Documents d
    left join DocCategories dc on dc.ID = d.CATEGORY_ID
    where APP_ID is not null 
    and APP_ID > 'HQ00004169'
    --and DOC_PATH not like '%.msg'; -- candidate doc
    """, sdbconn)
df_2 = pd.read_sql("""
    select 'candidate' as entity, APP_ID as DOC_ID, APP_ID, ORIGINAL_PATH, 'Candidate CV' as Category
    from ApplicantCV 
    where ORIGINAL_PATH is not null
    and APP_ID > 'HQ00004169'
    """, sdbconn)

# %% company doc
df_3 = pd.read_sql("""
        select 'company' as entity, d.DOC_ID, COMPANY_ID, DOC_PATH, dc.NAME as Category
        from Documents d
        left join DocCategories dc on dc.ID = d.CATEGORY_ID
        where COMPANY_ID is not null 
        and COMPANY_ID > 'HQ00014630'
        --and DOC_PATH not like '%.msg'; -- company doc
        """, sdbconn)

df_4 = pd.read_sql("""
        select 'company' as entity, tch.ID as DOC_ID, c.COMPANY_ID
        , tch.DOC_NAME
        , 'Terms and Conditions History' as Category --to be injected
        from TCHistory tch
        left join Contacts c on c.CONTACT_ID = tch.RECORD_ID
        where tch.FORM_ID = 3 
        and DOC_NAME is not NULL
        and COMPANY_ID > 'HQ00014630'
        """, sdbconn)

# %% load doc for job
df_5 = pd.read_sql("""
        select 'job' as entity, d.DOC_ID, JOB_ID, DOC_PATH, dc.NAME as Category
        from Documents d
        left join DocCategories dc on dc.ID = d.CATEGORY_ID
        where JOB_ID is not null 
        and JOB_ID > 'HQ00000477'
        --and DOC_PATH not like '%.msg'; -- job doc
        """, sdbconn)

# %% load doc for contact
df_6 = pd.read_sql("""
       select 'contact' as entity, d.DOC_ID, CONTACT_ID, DOC_PATH, dc.NAME as Category
        from Documents d
        left join DocCategories dc on dc.ID = d.CATEGORY_ID
        where CONTACT_ID is not null  
        and CONTACT_ID > 'HQ00078829'
        --and DOC_PATH not like '%.msg'; -- contact doc
        """, sdbconn)

# %% prepare meta data file upload for candidate
files_metadata = vincere_common.get_folder_structure(r'E:\vc_exede_production2')
files_metadata['matcher'] = files_metadata['file_fullpath'].map(lambda x: '\\'.join(x.split('\\')[-2:]))
files_metadata['matcher'] = files_metadata['matcher'].map(lambda x: str.lower(''.join(re.findall("[[a-zA-Z0-9|(|)|\\]*]", x))))
files_metadata['matcher'] = files_metadata['matcher'].map(lambda x: x.replace('(u)', ''))
assert False
# %%

# df_1['matcher'] = df_1['DOC_PATH'].map(lambda x: '\\'.join('_'.join(re.findall(r"[\w\\]+", x)).split('\\')[-2:]))
df_1['matcher'] = df_1['DOC_PATH'].map(lambda x: '\\'.join(x.split('\\')[-2:]))
df_1['matcher'] = df_1['matcher'].map(lambda x: str.lower(''.join(re.findall("[[a-zA-Z0-9|(|)|\\]*]", x))))
df_1['matcher'] = df_1['matcher'].map(lambda x: x.replace('(u)', ''))
fmeta = files_metadata.loc[
    files_metadata['root'].str.contains('APPLICANT', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('ORIGINAL_CVS', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('TEMPS', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('PLACEMENT', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('FORMATTED_CVS', flags=re.IGNORECASE)
,]
df_1 = df_1.merge(fmeta, on='matcher')

# df_2['matcher'] = df_2['ORIGINAL_PATH'].map(lambda x: '\\'.join('_'.join(re.findall(r"[\w\\]+", x)).split('\\')[-2:]))
df_2['matcher'] = df_2['ORIGINAL_PATH'].map(lambda x: '\\'.join(x.split('\\')[-2:]))
df_2['matcher'] = df_2['matcher'].map(lambda x: str.lower(''.join(re.findall("[[a-zA-Z0-9|(|)|\\]*]", x))))
df_2['matcher'] = df_2['matcher'].map(lambda x: x.replace('(u)', ''))
df_2 = df_2.merge(fmeta, on='matcher')

# %% prepare meta data file upload for COMPANY
fmeta = files_metadata.loc[
    files_metadata['root'].str.contains('COMPANY', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('TEMPS', flags=re.IGNORECASE)
    ,]

# df_3['matcher'] = df_3['DOC_PATH'].map(lambda x: '\\'.join('_'.join(re.findall(r"[\w\\]+", x)).split('\\')[-2:]))
df_3['matcher'] = df_3['DOC_PATH'].map(lambda x: '\\'.join(x.split('\\')[-2:]))
df_3['matcher'] = df_3['matcher'].map(lambda x: str.lower(''.join(re.findall("[[a-zA-Z0-9|(|)|\\]*]", x))))
df_3['matcher'] = df_3['matcher'].map(lambda x: x.replace('(u)', ''))
df_3 = df_3.merge(fmeta, on='matcher')

# df_4['matcher'] = df_4['DOC_NAME'].map(lambda x: '\\'.join('_'.join(re.findall(r"[\w\\]+", x)).split('\\')[-2:]))
df_4['matcher'] = df_4['DOC_NAME'].map(lambda x: '\\'.join(x.split('\\')[-2:]))
df_4['matcher'] = df_4['matcher'].map(lambda x: str.lower(''.join(re.findall("[[a-zA-Z0-9|(|)|\\]*]", x))))
df_4['matcher'] = df_4['matcher'].map(lambda x: x.replace('(u)', ''))
df_4 = df_4.merge(fmeta, on='matcher')

# %% prepare meta data file upload for job
fmeta = files_metadata.loc[
    files_metadata['root'].str.contains('REQUIREMENT', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('TEMPS', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('GENERAL', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('PLACEMENT', flags=re.IGNORECASE)
    ,]

# df_5['matcher'] = df_5['DOC_PATH'].map(lambda x: '\\'.join('_'.join(re.findall(r"[\w\\]+", x)).split('\\')[-2:]))
df_5['matcher'] = df_5['DOC_PATH'].map(lambda x: '\\'.join(x.split('\\')[-2:]))
df_5['matcher'] = df_5['matcher'].map(lambda x: str.lower(''.join(re.findall("[[a-zA-Z0-9|(|)|\\]*]", x))))
df_5['matcher'] = df_5['matcher'].map(lambda x: x.replace('(u)', ''))
df_5 = df_5.merge(fmeta, on='matcher')

# %% prepare meta data file for contact
fmeta = files_metadata.loc[
    files_metadata['root'].str.contains('CLIENT', flags=re.IGNORECASE) |
    files_metadata['root'].str.contains('TEMPS', flags=re.IGNORECASE)
    ,]

# df_6['matcher'] = df_6['DOC_PATH'].map(lambda x: '\\'.join('_'.join(re.findall(r"[\w\\]+", x)).split('\\')[-2:]))
df_6['matcher'] = df_6['DOC_PATH'].map(lambda x: '\\'.join(x.split('\\')[-2:]))
df_6['matcher'] = df_6['matcher'].map(lambda x: str.lower(''.join(re.findall("[[a-zA-Z0-9|(|)|\\]*]", x))))
df_6['matcher'] = df_6['matcher'].map(lambda x: x.replace('(u)', ''))
df_6 = df_6.merge(fmeta, on='matcher')

# %% create file name col and external id col
df_1['file_name'] = df_1['alter_file2']
df_1['external_id'] = df_1['APP_ID']

df_2['file_name'] = df_2['alter_file2']
df_2['external_id'] = df_2['APP_ID']

df_3['file_name'] = df_3['alter_file2']
df_3['external_id'] = df_3['COMPANY_ID']

df_4['file_name'] = df_4['alter_file2']
df_4['external_id'] = df_4['COMPANY_ID']

df_5['file_name'] = df_5['alter_file2']
df_5['external_id'] = df_5['JOB_ID']

df_6['file_name'] = df_6['alter_file2']
df_6['external_id'] = df_6['CONTACT_ID']

# file_name and external_id
if len(df_1):
    df_1 = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(df_1, ddbconn)
if len(df_2):
    df_2 = vincere_custom_migration.insert_bulk_upload_document_mapping_4_candidate(df_2, ddbconn)
if len(df_3):
    df_3 = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(df_3, ddbconn)
if len(df_4):
    df_4 = vincere_custom_migration.insert_bulk_upload_document_mapping_4_company(df_4, ddbconn)
if len(df_5):
    df_5 = vincere_custom_migration.insert_bulk_upload_document_mapping_4_position_description(df_5, ddbconn)
if len(df_6):
    df_6 = vincere_custom_migration.insert_bulk_upload_document_mapping_4_contact(df_6, ddbconn)
assert False
# %% upload files to s3
s3_bucket = cf['default'].get('s3_bucket')
s3_key = cf['default'].get('s3_key')
REGION_HOST = 's3.eu-central-1.amazonaws.com'

from common import s3_add_thread_pool

s3_add_thread_pool.upload_multi_files_parallelism_1_2(df_1, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host
s3_add_thread_pool.upload_multi_files_parallelism_1_2(df_2, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(df_3, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host
s3_add_thread_pool.upload_multi_files_parallelism_1_2(df_4, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(df_5, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(df_6, 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host

s3_add_thread_pool.upload_multi_files_parallelism_1_2(df_6[:1], 'file', 'alter_file2', 'root'
                                                      , bucket=s3_bucket
                                                      , key=s3_key, log=mylog, region_host=REGION_HOST)  # singapore server does not need region host



df_1['fileName'] = df_1.file
df_2['fileName'] = df_2.file
df_3['fileName'] = df_3.file
# df_4['fileName'] = df_4.file
df_5['fileName'] = df_5.file
df_6['fileName'] = df_6.file
# df_1['fileName'] = df_1.matcher.map(lambda x: x.split('\\')[-1])
# df_2['fileName'] = df_2.matcher.map(lambda x: x.split('\\')[-1])
# df_3['fileName'] = df_3.matcher.map(lambda x: x.split('\\')[-1])
# # df_4['fileName'] = df_4.matcher.map(lambda x: x.split('\\')[-1])
# df_5['fileName'] = df_5.matcher.map(lambda x: x.split('\\')[-1])
# df_6['fileName'] = df_6.matcher.map(lambda x: x.split('\\')[-1])

df_1['insert_timestamp'] = df_1['last_modification_date']
df_2['insert_timestamp'] = df_2['last_modification_date']
df_3['insert_timestamp'] = df_3['last_modification_date']
df_5['insert_timestamp'] = df_5['last_modification_date']
df_6['insert_timestamp'] = df_6['last_modification_date']

import sqlalchemy
sqlite_path = cf['default'].get('sqlite_path')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
df_1.extension_file = df_1.extension_file.map(lambda x: x.group() if x else x)
df_1.to_sql(con=engine_sqlite, name='upload_df_1', if_exists='replace', index=False)

df_2.extension_file = df_2.extension_file.map(lambda x: x.group() if x else x)
df_2.to_sql(con=engine_sqlite, name='upload_df_2', if_exists='replace', index=False)

df_3.extension_file = df_3.extension_file.map(lambda x: x.group() if x else x)
df_3.to_sql(con=engine_sqlite, name='upload_df_3', if_exists='replace', index=False)

df_5.extension_file = df_5.extension_file.map(lambda x: x.group() if x else x)
df_5.to_sql(con=engine_sqlite, name='upload_df_5', if_exists='replace', index=False)

df_6.extension_file = df_6.extension_file.map(lambda x: x.group() if x else x)
df_6.to_sql(con=engine_sqlite, name='upload_df_6', if_exists='replace', index=False)

assert False
# %% rename uploaded file and update created date
vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(df_1[['alter_file2', 'fileName', 'insert_timestamp']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)

vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(df_2[['alter_file2', 'fileName', 'insert_timestamp']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)

vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(df_3[['alter_file2', 'fileName', 'insert_timestamp']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)

vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(df_5[['alter_file2', 'fileName', 'insert_timestamp']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)

vindoc = pd.read_sql("select id, uploaded_filename from candidate_document",ddbconn)
vindoc = vindoc[['id', 'uploaded_filename']].merge(df_6[['alter_file2', 'fileName', 'insert_timestamp']], left_on='uploaded_filename', right_on='alter_file2')
vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(vindoc, ddbconn, ['uploaded_filename', 'insert_timestamp'], ['id'], 'candidate_document', mylog)



# %%  set original cv
vincere_custom_migration.execute_sql_update("update candidate_document set primary_document = 0 where candidate_id>(select id from candidate where external_id='HQ00004169')", ddbconn)

vin_doc = pd.read_sql("select * from candidate_document where candidate_id>(select id from candidate where external_id='HQ00004169')", ddbconn)
vin_doc = vin_doc.merge(df_2, left_on='uploaded_filename', right_on='file')
vin_doc['primary_document'] = 1
vincere_custom_migration.psycopg2_bulk_update_tracking(vin_doc, ddbconn, ['primary_document'], ['id'], 'candidate_document', mylog)


