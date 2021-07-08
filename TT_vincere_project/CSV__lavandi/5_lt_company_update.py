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
cf.read('lt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %%
from common import vincere_company
vcom = vincere_company.Company(connection)
company = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__lavandi\Data\Lavandi Talent - Import data - company_records.csv')
company['company_externalid'] = company['External_Id'].apply(lambda x: str(x) if x else x)
assert False #country citizenship jobtype
# %% name
tem = company[['company_externalid','Company Name']].dropna()
tem['name'] = tem['Company Name']
vcom.update_company_name(tem, mylog)

# %% phone
tem = company[['company_externalid','Phone']].dropna()
tem['phone'] = tem['Phone']
tem['switch_board'] = tem['Phone']
vcom.update_phone(tem, mylog)
vcom.update_switch_board(tem, mylog)

# %% address 1
tem = company[['company_externalid','Address (Link To Map)','Address Line 1']].dropna()
tem['address'] = tem['Address (Link To Map)']
tem['address_line1'] = tem['Address Line 1']
vcom.update_location_address_line1(tem, dest_db, mylog)

# %% address 2
tem = company[['company_externalid','Address (Link To Map)','Address Line 2']].dropna()
tem['address'] = tem['Address (Link To Map)']
tem['address_line2'] = tem['Address Line 2']
vcom.update_location_address_line2(tem, dest_db, mylog)

# %% location name/address
# company_address = company[['company_externalid','Company Address Town / City','Company ZIP (Postal) Code','Company Country']]
# company_address = company_address.where(company_address.notnull(),None)
# company_address['address'] = company_address[['Company Address Town / City','Company ZIP (Postal) Code','Company Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# company_address['location_name'] = company_address['address']
# # %%
# # assign contacts's addresses to their companies
# comaddr = company_address[['company_externalid', 'address','location_name','Company Address Town / City','Company ZIP (Postal) Code','Company Country']].drop_duplicates()
# comaddr = comaddr.loc[comaddr['address']!='']
# cp1 = vcom.insert_company_location(comaddr, mylog)
#
# # %% city
# tem = comaddr[['company_externalid', 'address', 'Company Address Town / City']].dropna().drop_duplicates()
# tem['city'] = tem['Company Address Town / City']
# cp3 = vcom.update_location_city_2(tem, dest_db, mylog)
#
# # %% postcode
# tem = comaddr[['company_externalid', 'address', 'Company ZIP (Postal) Code']].dropna().drop_duplicates()
# tem['post_code'] = tem['Company ZIP (Postal) Code']
# cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)
#
# # %% country
# tem = comaddr[['company_externalid', 'address', 'Company Country']].dropna().drop_duplicates()
# tem['country_code'] = tem['Company Country'].map(vcom.get_country_code)
# tem['country'] = tem['Company Country']
# tem.loc[tem['country_code']=='']
# cp6 = vcom.update_location_country_2(tem, dest_db, mylog)