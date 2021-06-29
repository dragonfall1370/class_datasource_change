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
cf.read('rt_config.ini')
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
select c.ID as company_externalid
     , c.Name
     , c.Address1
     , c.Address2
     , c.Address3
     , c.Town
     , c.PostCode
     , c.County
     , c.Country
     , c.Phone
     , c.WebAddress
     , c.LtdRegistration
from Company c
"""
company = pd.read_sql(sql, engine_sqlite)
company['company_externalid'] = company['company_externalid'].apply(lambda x: str(x) if x else x)
assert False

# %% billing address
company['address'] = company[['Address1', 'Address2', 'Address3', 'Town', 'PostCode','County','Country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)

# %% city
company['city'] = company['Town']
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company['PostCode']
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% state
company['state'] = company['County']
cp5 = vcom.update_location_state_2(company, dest_db, mylog)

# %% country
tem = company[['company_externalid', 'Country', 'address']]
tem['country_code'] = tem.Country.map(vcom.get_country_code)
tem['country'] = tem.Country
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% phone / switchboard
company['switch_board'] = company['Phone']
vcom.update_switch_board(company, mylog)

# %% website
company['website'] = company['WebAddress']
tem = company[['company_externalid', 'website']].dropna()
tem['website'] = tem['website'].apply(lambda x: x[:100])
cp5 = vcom.update_website(tem, mylog)

# %% note
# company['note'] = company[['company_externalid', 'Type', 'Description', 'vat', 'bd_info', 'size_legal_department']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['YC ID', 'Type', 'Description', 'VAT', 'BD_Info', 'Size Legal Department'], x) if e[1]]), axis=1)
# # company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
# vcom.update_note_2(company, dest_db, mylog)

# %% note
company['note'] = company[['company_externalid']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Rytons ID'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(company, dest_db, mylog)

# %% company number
tem = company[['company_externalid', 'LtdRegistration']].dropna()
tem['company_number'] = tem['LtdRegistration']
vcom.update_company_number(tem, dest_db, mylog)

# %% industry
# industries = pd.read_csv('industries.csv')
# from common import vincere_candidate
# import datetime
# vcand = vincere_candidate.Candidate(connection)
# tem = industries[['Industries Vincere']].drop_duplicates()
# tem['insert_timestamp'] = datetime.datetime.now()
# tem['name'] = tem['Industries Vincere']
# tem = tem.loc[tem.name!='']
# vcand.insert_industry(tem, mylog)

sql = """
select ID as company_externalid, rmi."Level 1" as industries
from Company c
left join rytons_mapping_industries_speciality rmi on lower(rmi."Primary ID") = lower(c.PrimarySectorWSIID)
"""
company_industries = pd.read_sql(sql, engine_sqlite)
company_industries['company_externalid'] = company_industries['company_externalid'].apply(lambda x: str(x) if x else x)

company_industries['name'] = company_industries['industries']
company_industries = company_industries.drop_duplicates().dropna()
cp10 = vcom.insert_company_industry(company_industries, mylog)
