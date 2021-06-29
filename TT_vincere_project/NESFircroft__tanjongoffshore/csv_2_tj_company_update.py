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
cf.read('tj_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
company = pd.read_sql("""select * from Company""", engine_sqlite)
company['company_externalid'] = company['RMSCLIENTID'].astype(str)
assert False
# %% location name/address
company['address'] = company[['ADDRESS', 'ADDRESS2','CITY','COUNTY','POST CODE','COUNTRY']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company['location_name'] = company['address']
# %%
# assign contacts's addresses to their companies
comaddr = company[['company_externalid', 'address','location_name','ADDRESS', 'ADDRESS2','CITY','COUNTY','POST CODE','COUNTRY']].drop_duplicates()
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'ADDRESS']].dropna().drop_duplicates()
tem['address_line1'] = tem.ADDRESS
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'ADDRESS2']].dropna().drop_duplicates()
tem['address_line2'] = tem.ADDRESS2
cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'CITY']].dropna().drop_duplicates()
tem['city'] = tem.CITY
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'POST CODE']].dropna().drop_duplicates()
tem['post_code'] = tem['POST CODE']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'COUNTY']].dropna().drop_duplicates()
tem['state'] = tem.COUNTY
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'COUNTRY']].dropna().drop_duplicates()
tem['country_code'] = tem.COUNTRY.map(vcom.get_country_code)
tem['country'] = tem.COUNTRY
tem.loc[tem['country_code']=='']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'GENERAL TELEPHONE']].dropna()
tem['switch_board'] = tem['GENERAL TELEPHONE']
tem['phone'] = tem['switch_board']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %%  website
tem = company[['company_externalid', 'COMPANY URL']].dropna()
tem['website'] = tem['COMPANY URL']
tem['website'] = tem['website'].apply(lambda x: x[:100])
tem = tem.loc[tem['website']!='']
cp5 = vcom.update_website(tem, mylog)

# %% note
tem = company[['company_externalid', 'COMPANY COMMENTS']].dropna()
tem['note'] = tem[['company_externalid','COMPANY COMMENTS']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['RMSCLIENTID','COMPANY COMMENT'], x) if e[1]]), axis=1)
vcom.update_note_2(tem, dest_db, mylog)
