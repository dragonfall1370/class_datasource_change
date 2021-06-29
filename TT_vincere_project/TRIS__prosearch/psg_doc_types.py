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
cf.read('psg_config.ini')
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
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

# %% source
file_type1 = pd.read_sql("""
select distinct CandDocTypeDisplayName as name from CandidateDocType
""", engine_mssql)
file_type2 = pd.read_sql("""
select distinct DTDisplayName as name  from tblDocumentType
""", engine_mssql)
file_type = pd.concat([file_type1,file_type2])
file_type = file_type.drop_duplicates()
assert False
# %% insert doctype
file_type['insert_timestamp'] = datetime.datetime.now()
file_type['kind'] = 0
cols = ['name', 'insert_timestamp', 'kind']
doc_t = pd.read_sql("select * from document_types",connection)
doc_t['matcher'] = doc_t['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
file_type['matcher'] = file_type['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
file_type = file_type.loc[~file_type['matcher'].isin(doc_t['matcher'])]
vincere_custom_migration.psycopg2_bulk_insert(file_type, connection, cols, 'document_types')