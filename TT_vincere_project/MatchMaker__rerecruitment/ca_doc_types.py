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
cf.read('ca_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

# %% source
cand = pd.read_sql("""
select Candidate_Id as candidate_externalid, File_Name, Code
from Candidate_Linked_Files clf
left join Linked_File_Type lft on clf.Type_Id = lft.Type_Id
where File_Name is not null
""", engine_mssql)
cand['candidate_externalid'] = cand['candidate_externalid'].astype(str)
assert False
# %% insert doctype
df_doc = cand[['Code']].drop_duplicates().dropna()
df_doc['name'] = df_doc['Code']
df_doc['insert_timestamp'] = datetime.datetime.now()
df_doc['kind'] = 0
cols = ['name', 'insert_timestamp', 'kind']
doc_t = pd.read_sql("select * from document_types",connection)
doc_t['matcher'] = doc_t['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
df_doc['matcher'] = df_doc['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
df_doc = df_doc.loc[~df_doc['matcher'].isin(doc_t['matcher'])]
vincere_custom_migration.psycopg2_bulk_insert(df_doc, connection, cols, 'document_types')

# %% mapping doc type
cand = cand.loc[cand['Code'].notnull()]
cand['matcher'] = cand['Code'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
doc_t = pd.read_sql("select id as document_types_id, name from document_types",connection)
doc_t['matcher'] = doc_t['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand = cand.merge(doc_t,on='matcher')
cand_tem = cand[['candidate_externalid','document_types_id','File_Name']]
cand_tem = cand_tem.merge(pd.read_sql("""select id as candidate_id, external_id as candidate_externalid from candidate""",connection),on='candidate_externalid')
vindoc = pd.read_sql("select id, uploaded_filename, candidate_id from candidate_document",connection)
doc_tem = vindoc.merge(cand_tem, left_on=['candidate_id','uploaded_filename'], right_on=['candidate_id','File_Name'])
doc_tem = doc_tem.drop_duplicates()
doc_tem['rn'] = doc_tem.groupby('id').cumcount()
doc_tem = doc_tem.loc[doc_tem['rn']==0]
vincere_custom_migration.psycopg2_bulk_update_tracking(doc_tem, connection, ['document_types_id'], ['id'], 'candidate_document', mylog)