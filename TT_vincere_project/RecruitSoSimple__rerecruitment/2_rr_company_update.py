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
from datetime import datetime

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('rr_config.ini')
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

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
company = pd.read_sql("""
select concat('RSS',[Record ID]) as company_externalid
, nullif([Company Name],'') as company_name
     , Employees
     , [Standard Hours]
     , [Standard Holiday]
     , Notes
     , Twitter
     , LinkedIn
     , Website
     , [Main Site Address 1]
     , [Main Site Address 2]
     , [Main Site Town]
     , [Main Site County]
     , [Main Site Postcode]
     , [Main Site Country]
     , [Main Site Phone]
from Clients""", engine_mssql)
# company = company.drop_duplicates()
assert False
# %% location name/address
company['address'] = company[['Main Site Address 1', 'Main Site Address 2','Main Site Town','Main Site County','Main Site Postcode','Main Site Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company['location_name'] = company['address']
# %%
# assign contacts's addresses to their companies
comaddr = company[['company_externalid', 'address','location_name','Main Site Address 1', 'Main Site Address 2','Main Site Town','Main Site County','Main Site Postcode','Main Site Country']].drop_duplicates()
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'Main Site Address 1']].dropna().drop_duplicates()
tem['address_line1'] = tem['Main Site Address 1']
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 2
tem = comaddr[['company_externalid', 'address', 'Main Site Address 2']].dropna().drop_duplicates()
tem['address_line2'] = tem['Main Site Address 2']
cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'Main Site Town']].dropna().drop_duplicates()
tem['city'] = tem['Main Site Town']
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'Main Site Postcode']].dropna().drop_duplicates()
tem['post_code'] = tem['Main Site Postcode']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'Main Site County']].dropna().drop_duplicates()
tem['state'] = tem['Main Site County']
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% district
# tem = comaddr[['company_externalid', 'address', 'district']].dropna().drop_duplicates()
# cp5 = vcom.update_location_district_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'Main Site Country']].dropna().drop_duplicates()
tem['country_code'] = tem['Main Site Country'].map(vcom.get_country_code)
tem['country'] = tem['Main Site Country']
tem.loc[tem['country_code']=='']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% location type
tem = comaddr[['company_externalid','address']]
tem['location_type'] = 'HEADQUARTER'
cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

# %% location type
# tem = comaddr[['company_externalid','address']]
# tem['rn'] = tem.groupby('company_externalid').cumcount()
# tem = tem.loc[tem['rn']==0]
# tem['location_type'] = 'BILLING_ADDRESS'
# cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

tem = company[['company_externalid','Main Site Phone']].dropna().drop_duplicates()
tem['phone_number'] = tem['Main Site Phone']
tem2 = tem.merge(vcom.company, on=['company_externalid'])
tem2['company_id'] =  tem2['id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['phone_number', ], ['company_id', ],'company_location', mylog)
# vcom.update_business_number_tax(tem, mylog)
# vcom.update_payment_term(tem, mylog)

# %% phone / switchboard
# tem = company[['company_externalid','phone','switch_board']].dropna().drop_duplicates()
# vcom.update_switch_board(tem, mylog)
# vcom.update_phone(tem, mylog)

# %% headqquatername
tem = company[['company_externalid','company_name','Main Site Address 1']]
tem['head_quarter'] = tem[['company_name','Main Site Address 1']].apply(lambda x: ' - '.join([e for e in x if e]), axis=1)
cp5 = vcom.update_head_quarter_name(tem, mylog)

# %% website
tem = company[['company_externalid','Website']].dropna().drop_duplicates()
tem['website'] = tem['Website'].apply(lambda x: x[:100])
tem = tem.loc[tem['website']!='']
cp5 = vcom.update_website(tem, mylog)

# %% note
note = company[['company_externalid','Standard Hours','Standard Holiday','Notes','Twitter','Employees']]
note['note'] = note[['Standard Hours','Standard Holiday','Notes','Twitter','Employees']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Standard Business Hours','Standard Holiday','Notes','Twitter','Employees'], x) if e[1]]), axis=1)
note = note.loc[note['note']!='']
vcom.update_note_2(note, dest_db, mylog)

#%% linkedin
tem = company[['company_externalid','LinkedIn']].dropna().drop_duplicates()
tem['url_linkedin'] = tem['LinkedIn']
vcom.update_linkedin(tem,mylog)

#%% employee
tem = company[['company_externalid','Employees']].dropna().drop_duplicates()
tem['employees_number'] = tem['Employees'].astype(int)
vcom.update_employees_number(tem,mylog)


