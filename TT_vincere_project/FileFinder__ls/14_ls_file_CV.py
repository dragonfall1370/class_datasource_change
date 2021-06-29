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
cf.read('ls_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
# src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
document_path = r'D:\Tony\LS\FFDocs-CU1002828\FFDocs-CU1002828'
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

# %% extract data
file = pd.read_sql("""
select OriginalDocumentName from document where Description like '%CV%'
""", engine_sqlite)
assert False
# %% rename uploaded file
vindoc = pd.read_sql("select id, uploaded_filename, primary_document from candidate_document",connection)
vindoc = vindoc[['id', 'uploaded_filename', 'primary_document']].merge(file, left_on='uploaded_filename', right_on='OriginalDocumentName')
vindoc = vindoc.drop_duplicates()
vindoc['primary_document'] = 1
# vindoc.loc[vindoc['alter_file2'].notnull(), 'uploaded_filename'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'originaldocumentname']
# vindoc.loc[vindoc['alter_file2'].notnull(), 'created'] = vindoc.loc[vindoc['alter_file2'].notnull(), 'createdon']
# vindoc['created'] = pd.to_datetime(vindoc['created'])

vincere_custom_migration.load_data_to_vincere(vindoc, dest_db, 'update', 'candidate_document', ['primary_document'], ['id'], mylog)
