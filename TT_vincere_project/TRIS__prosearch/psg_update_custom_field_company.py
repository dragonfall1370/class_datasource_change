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
cf.read('psg_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
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

# %% connection
company = pd.read_sql("""
select
 ClientID
 , nullif(convert(varchar,ClientACN),'') as  account_sys
 , nullif(convert(varchar,ClientFax),'') as payroll_sys
  from Client""", engine_mssql)
company['company_externalid'] =company['ClientID'].apply(lambda x: str(x) if x else x)
assert False
# %% payroll
tem = company[['company_externalid','payroll_sys']].dropna().drop_duplicates()
api = 'cc2178c6dc02253ffbd09a10fd677307'
vincere_custom_migration.insert_company_text_field_values(tem, 'company_externalid', 'payroll_sys', api, connection)

# %% account
tem = company[['company_externalid','account_sys']].dropna().drop_duplicates()
api = '468d1bc87f726e462f3ed8db799405a1'
vincere_custom_migration.insert_company_text_field_values(tem, 'company_externalid', 'account_sys', api, connection)



