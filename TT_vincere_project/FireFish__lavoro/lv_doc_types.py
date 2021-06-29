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
cf.read('lv_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

# %% source
doc_type = pd.read_csv('doc_type.csv')
file = pd.read_sql("""
select CoreObjectID, FileName, CreatedDate, type.Description
from Document d
left join (select DocumentTypeID, Description, Name from DocumentType dt
left join DocumentCategory dc on dc.ID = dt.DocumentCategoryID) type on type.DocumentTypeID = d.DocumentTypeID
where Name = 'Candidate'
""", engine_sqlite)
assert False
# %% insert doctype
df_doc = doc_type[['Vincere value']].drop_duplicates()
df_doc['name'] = df_doc['Vincere value']
df_doc['insert_timestamp'] = datetime.datetime.now()
df_doc['kind'] = 0
cols = ['name', 'insert_timestamp', 'kind']
doc_t = pd.read_sql("select * from document_types",connection)
doc_t['matcher'] = doc_t['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
df_doc['matcher'] = df_doc['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
df_doc = df_doc.loc[~df_doc['matcher'].isin(doc_t['matcher'])]
vincere_custom_migration.psycopg2_bulk_insert(df_doc, connection, cols, 'document_types')

# %% mapping doc type
vindoc = pd.read_sql("select id, uploaded_filename from candidate_document where entity_type='CANDIDATE'",connection)
file['matcher'] = file['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
doc_type['matcher'] = doc_type['FF'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

file = file.merge(doc_type, on='matcher', how='left')
file_type = file[['FileName','Vincere value']]
file_type = file_type.fillna('Other docs')

vin_doctype = pd.read_sql("select * from document_types",connection)
vin_doctype['matcher'] = vin_doctype['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

file_type['matcher'] = file_type['Vincere value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
file_type = file_type.merge(vin_doctype, on='matcher', how='left')
# file_type.loc[file_type['Vincere Value'].str.contains('Invoice')]

file_type_map = file_type[['FileName', 'id']]
# file_type_map = file_type_map.fillna('3')
file_type_map['document_types_id'] = file_type_map['id'].apply(lambda x: int(x))
vindoc = vindoc.merge(file_type_map[['FileName','document_types_id']], left_on='uploaded_filename', right_on='FileName')
vindoc = vindoc.drop_duplicates()
tem =vindoc[['id','document_types_id']]
tem['rn'] = tem.groupby('id')['document_types_id'].cumcount()
tem.to_csv('doc_type_file.csv')
tem1 = tem.loc[tem['rn']==0]
tem2 = tem.loc[tem['rn']==1]
tem.loc[tem['id'] == 40842]
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, connection, ['document_types_id'], ['id'], 'candidate_document', mylog)