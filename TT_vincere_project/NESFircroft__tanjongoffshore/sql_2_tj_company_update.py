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
company = pd.read_sql("""select ClientID, Phone, Fax ,WebPage, LinkedInURL, AboutUs from Clients""", engine_mssql)
company['company_externalid'] = 'FC'+company['ClientID'].astype(str)
assert False
# %% location name/address
company_location = pd.read_sql("""select  ClientID
         , nullif(Address1,'') as Address1
         , nullif(Address2,'') as Address2
         , nullif(Address3,'') as Address3
         , nullif(PostCode,'') as PostCode
         , nullif(Town,'') as Town
         , nullif(County,'') as County
         , nullif(Country,'') as Country
    from ClientAddresses""", engine_mssql)
company_location['company_externalid'] = 'FC'+company_location['ClientID'].astype(str)
company_location['address'] = company_location[['Address1', 'Address2','Address3','Town','PostCode','County','Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company_location['location_name'] = company_location['address']
company_location = company_location.drop_duplicates()
# %%
# assign contacts's addresses to their companies
comaddr = company_location[['company_externalid', 'address','location_name','Address1', 'Address2','Address3','Town','PostCode','County','Country']].drop_duplicates()
comaddr = comaddr.loc[comaddr['address']!='']
comaddr = comaddr.loc[comaddr['address']!='-, -, -, -']
comaddr = comaddr.loc[comaddr['address']!='-']

cp1 = vcom.insert_company_location(comaddr, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'Address1']].dropna().drop_duplicates()
tem['address_line1'] = tem.Address1
tem['rn'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['rn']<100]
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 2
tem = comaddr[['company_externalid', 'address', 'Address2','Address3']]
tem['address_line2'] = tem[[ 'Address2','Address3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['address_line2']!='']
tem['rn'] = tem['address_line2'].apply(lambda x: len(x))
tem = tem.loc[tem['rn']<100]
cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'Town']].dropna().drop_duplicates()
tem['city'] = tem.Town
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'PostCode']].dropna().drop_duplicates()
tem['post_code'] = tem['PostCode']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'County']].dropna().drop_duplicates()
tem['state'] = tem.County
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'Country']].dropna().drop_duplicates()
tem['country_code'] = tem.Country.map(vcom.get_country_code)
tem['country'] = tem.Country
# a = tem.loc[tem['country_code']=='']
# a['country'].unique()
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'Phone']].dropna()
tem['switch_board'] = tem['Phone']
tem['phone'] = tem['switch_board']
vcom.update_switch_board_2(tem, dest_db, mylog)
vcom.update_phone2(tem,dest_db, mylog)

# %%  website
tem = company[['company_externalid', 'WebPage']].dropna()
tem['website'] = tem['WebPage']
tem['website'] = tem['website'].apply(lambda x: x[:100])
tem = tem.loc[tem['website']!='']
cp5 = vcom.update_website2(tem, dest_db, mylog)

# %%  linkedin
tem = company[['company_externalid', 'LinkedInURL']].dropna()
tem['url_linkedin'] = tem['LinkedInURL']
tem = tem.loc[tem['url_linkedin']!='']
cp6 = vcom.update_linkedin(tem, mylog)

# %% note
tem = company[['company_externalid', 'AboutUs']].dropna()
tem['note'] = tem[['company_externalid','AboutUs']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Fircroft Client ID','About Us'], x) if e[1]]), axis=1)
tem['note'] = tem['note'].apply(lambda x: x.replace('Fircroft Client ID: FC','Fircroft Client ID: '))
vcom.update_note_2(tem, dest_db, mylog)
