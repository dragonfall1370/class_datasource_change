
# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import os
import re
from common import s3_add_thread_pool as s3
import psycopg2
import sqlalchemy
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
mylog = log.get_info_logger("_apis.log")
cf = configparser.RawConfigParser()
cf.read('exede_config.ini')
data_folder = cf['default'].get('data_folder')
upload_folder = cf['default'].get('upload_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]

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
sqlite_path = cf['default'].get('sqlite_path')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# assert False
# %% load mapping file from csv files
df_1 = pd.read_sql('select * from upload_df_1', engine_sqlite)
df_2 = pd.read_sql('select * from upload_df_2', engine_sqlite)
df_3 = pd.read_sql('select * from upload_df_3', engine_sqlite)
df_5 = pd.read_sql('select * from upload_df_5', engine_sqlite)
df_6 = pd.read_sql('select * from upload_df_6', engine_sqlite)

# %% assign column value for subset
df_1.loc[df_1['Category'].isnull() & df_1['file'].str.endswith('.msg'), 'Category'] = 'Email Sent'
df_1.loc[df_1['Category'].isnull(), 'Category'] = 'Other docs'

df_2.loc[df_2['Category'].isnull() & df_2['file'].str.endswith('.msg'), 'Category'] = 'Email Sent'
df_2.loc[df_2['Category'].isnull(), 'Category'] = 'Other docs'

df_3.loc[df_3['Category'].isnull() & df_3['file'].str.endswith('.msg'), 'Category'] = 'Email Sent'
df_3.loc[df_3['Category'].isnull(), 'Category'] = 'Other docs'

df_5.loc[df_5['Category'].isnull() & df_5['file'].str.endswith('.msg'), 'Category'] = 'Email Sent'
df_5.loc[df_5['Category'].isnull(), 'Category'] = 'Other docs'

df_6.loc[df_6['Category'].isnull() & df_6['file'].str.endswith('.msg'), 'Category'] = 'Email Sent'
df_6.loc[df_6['Category'].isnull(), 'Category'] = 'Other docs'

# assert False
df_1.Category = df_1.Category.map(lambda x: x.replace("'", '')) # not allow this character
df_1.Category = df_1.Category.map(lambda x: x.replace("%", '')) # not allow this character
df_1.Category = df_1.Category.map(lambda x: x.replace("#", '')) # not allow this character
df_1.Category = df_1.Category.map(lambda x: x.replace("$", '')) # not allow this character
df_1.Category = df_1.Category.map(lambda x: x.replace("/", '')) # not allow this character

df_2.Category = df_2.Category.map(lambda x: x.replace("'", '')) # not allow this character
df_2.Category = df_2.Category.map(lambda x: x.replace("%", '')) # not allow this character
df_2.Category = df_2.Category.map(lambda x: x.replace("#", '')) # not allow this character
df_2.Category = df_2.Category.map(lambda x: x.replace("$", '')) # not allow this character
df_2.Category = df_2.Category.map(lambda x: x.replace("/", '')) # not allow this character

df_3.Category = df_3.Category.map(lambda x: x.replace("'", '')) # not allow this character
df_3.Category = df_3.Category.map(lambda x: x.replace("%", '')) # not allow this character
df_3.Category = df_3.Category.map(lambda x: x.replace("#", '')) # not allow this character
df_3.Category = df_3.Category.map(lambda x: x.replace("$", '')) # not allow this character
df_3.Category = df_3.Category.map(lambda x: x.replace("/", '')) # not allow this character

df_5.Category = df_5.Category.map(lambda x: x.replace("'", '')) # not allow this character
df_5.Category = df_5.Category.map(lambda x: x.replace("%", '')) # not allow this character
df_5.Category = df_5.Category.map(lambda x: x.replace("#", '')) # not allow this character
df_5.Category = df_5.Category.map(lambda x: x.replace("$", '')) # not allow this character
df_5.Category = df_5.Category.map(lambda x: x.replace("/", '')) # not allow this character

df_6.Category = df_6.Category.map(lambda x: x.replace("'", '')) # not allow this character
df_6.Category = df_6.Category.map(lambda x: x.replace("%", '')) # not allow this character
df_6.Category = df_6.Category.map(lambda x: x.replace("#", '')) # not allow this character
df_6.Category = df_6.Category.map(lambda x: x.replace("$", '')) # not allow this character
df_6.Category = df_6.Category.map(lambda x: x.replace("/", '')) # not allow this character

# %%
df_1.loc[df_1.file == 'SentEmail_20181220123508880.msg']

document_types = pd.concat([
        df_1[['Category']],
        df_2[['Category']],
        df_3[['Category']],
        df_5[['Category']],
        df_6[['Category']],
    ]).dropna().drop_duplicates()
document_types['name'] = document_types['Category']
document_types['kind'] = 0 # allow delete
document_types['insert_timestamp'] = datetime.datetime.now()
document_types['code'] = document_types['name'].map(lambda x: '_'.join(re.findall(r'[\w]+', x)))
document_types = document_types.merge(pd.read_sql("select id, name from document_types", ddbconn), on='name', how='left')
document_types = document_types[document_types['id'].isnull()]
vincere_custom_migration.psycopg2_bulk_insert_tracking(document_types, ddbconn, ['name','kind', 'insert_timestamp', 'code'], 'document_types', mylog)



# %% map to document types
document_types = pd.read_sql_query('select id as document_types_id, code as document_type, name from document_types', ddbconn)
df_1 = df_1.merge(document_types, left_on='Category', right_on='name')
df_2 = df_2.merge(document_types, left_on='Category', right_on='name')
df_3 = df_3.merge(document_types, left_on='Category', right_on='name')
df_5 = df_5.merge(document_types, left_on='Category', right_on='name')
df_6 = df_6.merge(document_types, left_on='Category', right_on='name')

# %% IMPORTANT: document_type KHONG DUOC DOI, VI DOI GIA TRI document_type, DUONG DAN DOWNLOAD FILE SE BI SAI

# df_1['uploaded_filename'] = df_1['file_name']
# vincere_custom_migration.psycopg2_bulk_update_tracking(df_1, ddbconn, ['document_type', 'document_types_id',], ['uploaded_filename',], 'candidate_document', mylog)
# df_2['uploaded_filename'] = df_2['file_name']
# vincere_custom_migration.psycopg2_bulk_update_tracking(df_2, ddbconn, ['document_type', 'document_types_id',], ['uploaded_filename',], 'candidate_document', mylog)
# df_3['uploaded_filename'] = df_3['file_name']
# vincere_custom_migration.psycopg2_bulk_update_tracking(df_3, ddbconn, ['document_type', 'document_types_id',], ['uploaded_filename',], 'candidate_document', mylog)
# df_4['uploaded_filename'] = df_4['file_name']
# vincere_custom_migration.psycopg2_bulk_update_tracking(df_4, ddbconn, ['document_type', 'document_types_id',], ['uploaded_filename',], 'candidate_document', mylog)
# df_5['uploaded_filename'] = df_5['file_name']
# vincere_custom_migration.psycopg2_bulk_update_tracking(df_5, ddbconn, ['document_type', 'document_types_id',], ['uploaded_filename',], 'candidate_document', mylog)
# df_6['uploaded_filename'] = df_6['file_name']
# vincere_custom_migration.psycopg2_bulk_update_tracking(df_6, ddbconn, ['document_type', 'document_types_id',], ['uploaded_filename',], 'candidate_document', mylog)

df_1['fileName'] = df_1.file_fullpath.map(lambda x: x.split('\\')[-1])
df_2['fileName'] = df_2.file_fullpath.map(lambda x: x.split('\\')[-1])

df_1['uploaded_filename'] = df_1['fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(df_1, ddbconn, ['document_types_id', ], ['uploaded_filename', ], 'candidate_document', mylog)
df_2['uploaded_filename'] = df_2['fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(df_2, ddbconn, ['document_types_id', ], ['uploaded_filename', ], 'candidate_document', mylog)
df_3['uploaded_filename'] = df_3['fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(df_3, ddbconn, ['document_types_id', ], ['uploaded_filename', ], 'candidate_document', mylog)
df_5['uploaded_filename'] = df_5['fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(df_5, ddbconn, ['document_types_id', ], ['uploaded_filename', ], 'candidate_document', mylog)
df_6['uploaded_filename'] = df_6['fileName']
vincere_custom_migration.psycopg2_bulk_update_tracking(df_6, ddbconn, ['document_types_id', ], ['uploaded_filename', ], 'candidate_document', mylog)




df_1.loc[df_1.APP_ID=='HQ00043498']

