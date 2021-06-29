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
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

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
SELECT
	  a.Id as company_externalid,
      a.Name,
      a.ParentId as parent_externalid,
      a.BillingStreet, a.BillingState, a.BillingCity, a.BillingPostalCode, a.BillingCountry,
      a.Phone,
      a.Website,
      a.Eden_Id__c,
      a.ts2__Invoice_Terms__c,
      a.Credit_checked__c,
      a.Terms_agreed__c,
      a.Target__c,
      u.Email as owner
FROM Account a
LEFT JOIN User u ON a.OwnerId = u.Id
WHERE a.IsDeleted = 0;
"""
company = pd.read_sql(sql, engine_sqlite)
assert False
# %% billing address
company['address'] = company[['BillingStreet', 'BillingState', 'BillingCity', 'BillingPostalCode', 'BillingCountry']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
comp_addr = company.loc[company['address'] != '']
cp2 = vcom.insert_company_location_2(comp_addr, dest_db, mylog)

# %% city
company['city'] = company['BillingCity']
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company['BillingPostalCode']
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% state
company['state'] = company['BillingState']
cp5 = vcom.update_location_state_2(company, dest_db, mylog)

# %% country
company['country_code'] = company.BillingCountry.map(vcom.get_country_code)
company['country'] = company.BillingCountry
company.loc[company['address'] != '']
cp6 = vcom.update_location_country_2(company, dest_db, mylog)

#%% parent company
cp2 = vcom.update_parent_company(company[['company_externalid', 'parent_externalid']], mylog)

# %% phone / switchboard
company['phone'] = company['Phone']
company['switch_board'] = company['Phone']
vcom.update_phone(company, mylog)
vcom.update_switch_board(company, mylog)

# %% website
company['website'] = company['Website']
ws = company.loc[company['website'].notnull()]
ws['website'] = ws['website'].apply(lambda x: x[:100])
vcom.update_website(ws, mylog)
# %% note
company.info()
company.loc[company['Credit_checked__c']=='0', 'Credit_checked__c'] = 'No'
company.loc[company['Credit_checked__c']=='1', 'Credit_checked__c'] = 'Yes'
note = company[[
    'company_externalid',
    'Eden_Id__c',
    'ts2__Invoice_Terms__c',
    'Credit_checked__c',
    'Terms_agreed__c',
    'Target__c']]

prefixes = [
'UA ID',
'Eden ID',
'Invoice Terms',
'Credit checked',
'Terms Agreed',
'Target Accounts'
]
note['note'] = note.apply(lambda x: '\n■ '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
vcom.update_note_2(note, dest_db, mylog)
