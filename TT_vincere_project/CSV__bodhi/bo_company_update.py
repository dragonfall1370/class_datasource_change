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
cf.read('bo_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
sql ="""select * from company"""
company = pd.read_sql(sql, engine_sqlite)
company['company_externalid'] = company['External ID']
assert False

# %% billing address
company['address'] = company[['Address (Location)', 'City', 'Post code','State (location)','Country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company['location_name'] = company['address']
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)
# %% city
company['city'] = company['City']
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company['Post code']
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% state
company['state'] = company['State (location)']
cp5 = vcom.update_location_state_2(company, dest_db, mylog)

# %% country
company['country_code'] = 'GB'
company['country'] = 'United Kingdom'
cp6 = vcom.update_location_country_2(company, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'Phone']].dropna()
tem['switch_board'] = tem['Phone']
tem['phone'] = tem['Phone']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %% website
tem = company[['company_externalid', 'Website']].dropna()
tem['website'] = tem['Website']
cp5 = vcom.update_website(tem, mylog)

# %% fax
tem = company[['company_externalid', 'Fax']].dropna()
tem['fax'] = tem['Fax']
cp5 = vcom.update_fax(tem, mylog)

# %% parent company
tem = company[['company_externalid', 'Headquarters Name']].dropna()
tem['head_quarter'] = tem['Headquarters Name']
cp5 = vcom.update_head_quarter_2(tem, dest_db, mylog)

# %% note
tem = company[['company_externalid', 'Note']].dropna()
tem['note'] = tem['Note']
vcom.update_note_2(tem, dest_db, mylog)