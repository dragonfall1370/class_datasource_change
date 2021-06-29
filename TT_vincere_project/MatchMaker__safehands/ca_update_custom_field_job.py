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
connection = engine_postgre_review.raw_connection()
# %%
job_b = pd.read_sql("""select Reference_Id as job_externalid, Table_Name,ur.*
from UDF_Record ur
left join UDF_Table ut on ur.Table_Id = ut.Table_Id
where Table_Name = 'Source of Role (booking)'
and Reference_Id not in (8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544)""", engine_mssql)
job_b['job_externalid'] = job_b['job_externalid'].astype(str)
job_b['job_externalid'] = 'BK'+job_b['job_externalid']

# job_v = pd.read_sql("""select Reference_Id as job_externalid, Table_Name,ur.*
# from UDF_Record ur
# left join UDF_Table ut on ur.Table_Id = ut.Table_Id
# where Table_Name = 'Sourcing (vacancies)'""", engine_mssql)
# job_v['job_externalid'] = job_v['job_externalid'].astype(str)
# job_v['job_externalid'] = 'VC'+job_v['job_externalid']
# job = pd.concat([job_b[['job_externalid','Field_Value_1']].dropna(), job_v[['job_externalid','Field_Value_1']].dropna()])
assert False
api = '84540c1cdbae84d2291ea61b40e511ad'
tem = job_b[['job_externalid','Field_Value_1']].dropna()
tem['Field_Value_1'].unique()
tem = tem.loc[tem['Field_Value_1']!='N']
tem = tem.loc[tem['Field_Value_1']!='Y']
vincere_custom_migration.insert_job_drop_down_list_values(tem, 'job_externalid', 'Field_Value_1', api, connection)


