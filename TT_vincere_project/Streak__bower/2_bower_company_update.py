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
cf.read('bower_config.ini')
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
sql = """
select "Company ID" as company_externalid
     , Location
     , "Exclusive?"
     , "Company size"
     , Fee
     , "Invoicing email address"
     , "Company Address"
     , Postcode
     , Tel ,Industry
from Company
"""
company = pd.read_sql(sql, engine_sqlite)
company['company_externalid'] = company['company_externalid'].apply(lambda x: str(x) if x else x)
assert False

# %% billing address
company['address'] = company['Company Address']
tem = company[['company_externalid', 'address']].dropna()
cp2 = vcom.insert_company_location_2(tem, dest_db, mylog)

# %% postcode
tem = company[['company_externalid', 'address', 'Postcode']].dropna()
tem['post_code'] = tem['Postcode']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% phone / switchboard
company['switch_board'] = company['Tel']
vcom.update_switch_board(company, mylog)

# %% note
company['note'] = company[['Exclusive?', 'Company size', 'Fee', 'Invoicing email address']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Exclusive?', 'Company Size', 'Fee', 'Invoicing Email Address'], x) if e[1]]), axis=1)
vcom.update_note_2(company, dest_db, mylog)

# %% industry
industries = pd.read_csv('industries.csv')
industries = industries.fillna('N/A')
from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(connection)
tem = industries[['Industries Vincere']].drop_duplicates()
tem['insert_timestamp'] = datetime.datetime.now()
tem['name'] = tem['Industries Vincere']
tem = tem.loc[tem.name!='']
tem = tem.drop_duplicates()
vcand.insert_industry(tem, mylog)

company_industries = company[['company_externalid', 'Industry']].dropna().drop_duplicates()

company_industries['name'] = company_industries['Industry']
company_industries = company_industries.drop_duplicates().dropna()
cp10 = vcom.insert_company_industry(company_industries, mylog)
