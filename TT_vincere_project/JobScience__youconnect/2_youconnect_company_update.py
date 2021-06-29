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
cf.read('yc_config.ini')
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
select
      a.Id as company_externalid,
      a.Name,
      a.BillingStreet, a.BillingState, a.BillingCity, a.BillingPostalCode, a.BillingCountry,
      u.Email as owner,
      a.ParentId as parent_externalid,
      a.Phone,
      a.Website,
      a.Description,
      a.Type,
      a.VAT__c as vat,
      a.Size_Legal_Department__c as size_legal_department,
      a.Rating,
      a.BD_Info__c as bd_info,
      a.No_Touch__c as no_touch,
      a.BillingCountryCode,
      cr.FirstName || ' ' || cr.LastName as CreatedBy,
      a.CreatedDate,
      a.Industry,
      a.Rating
from Account a
left join User u on a.OwnerId=u.Id
left join "User" cr on a.CreatedById = cr.Id
where a.IsDeleted=0
"""
company = pd.read_sql(sql, engine_sqlite)
assert False
# %% billing address
company['address'] = company[['BillingStreet', 'BillingState', 'BillingCity', 'BillingPostalCode', 'BillingCountry']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)

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

cp5 = vcom.update_website(ws, mylog)

# %% note
company['note'] = company[['company_externalid', 'Type', 'Description', 'vat', 'bd_info', 'size_legal_department']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['YC ID', 'Type', 'Description', 'VAT', 'BD_Info', 'Size Legal Department'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(company, dest_db, mylog)

#%% head company
company['head_quarter_externalid'] = company['company_externalid']
vcom.update_head_quarter(company, mylog)

# %% industry
industries = pd.read_csv('industries.csv')
# from common import vincere_candidate
# import datetime
# vcand = vincere_candidate.Candidate(connection)
# tem = industries[['Industries Vincere']].drop_duplicates()
# tem['insert_timestamp'] = datetime.datetime.now()
# tem['name'] = tem['Industries Vincere']
# tem = tem.loc[tem.name!='']
# vcand.insert_industry(tem, mylog)

industry = company[['company_externalid', 'Industry']].dropna()
industry = industry.Industry.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(industry[['company_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['company_externalid'], value_name='name') \
   .drop('variable', axis='columns') \
   .dropna()

industry['name'] = industry['name'].apply(lambda x: 'Utility Services' if x == 'Utility_Services' else x)
industry['matcher'] = industry['name']
industry = industry.merge(industries, left_on='matcher', right_on='Industry Experience Candidates Jobscience')
tem = industry[['company_externalid', 'Industries Vincere']]
tem['company_externalid'].value_counts()

tem['name'] = tem['Industries Vincere']
tem = tem.drop_duplicates()
cp10 = vcom.insert_company_industry(tem, mylog)

# %% make hot
tem = company[['company_externalid', 'no_touch']].dropna()
# now = datetime.datetime.now()
# end_date = now + datetime.timedelta(days=30)
# # tem['hot_end_date'] = end_date
# tem.loc[tem['is_hot']=='1', 'hot_end_date'] = end_date
tem['no_touch'] = pd.to_datetime(tem['no_touch'])
tem['hot_end_date'] = tem['no_touch']
vcom.update_make_hot(tem, mylog)

# %% reg date
tem = company[['company_externalid', 'CreatedDate']].dropna()
tem['reg_date'] = pd.to_datetime(tem['CreatedDate'])
vcom.update_reg_date(tem, mylog)