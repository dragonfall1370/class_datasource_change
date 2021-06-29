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
cf.read('bw_config.ini')
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
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
candidate = pd.read_csv(r'candidate_import.csv')
candidate['candidate_externalid'] = candidate['Candidate ID']
candidate = candidate.where(candidate.notnull(),None)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','Current Address', 'Current Address 1','Current Address 2','Current Address District / Suburb','Current Address Town / City','Current Address State','Current Address ZIP (Postal) Code','Current Address Country']]
# c_location['address'] = c_location[['Current Address', 'Current Address 1','Current Address 2','Current Address District / Suburb','Current Address Town / City','Current Address State','Current Address ZIP (Postal) Code','Current Address Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# c_location['location_name'] = c_location['address']
# c_location = c_location.loc[c_location['address']!='']
# cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
comaddr = c_location[['candidate_externalid','Current Address', 'Current Address 1','Current Address 2','Current Address District / Suburb','Current Address Town / City','Current Address State','Current Address ZIP (Postal) Code','Current Address Country']].drop_duplicates()\
    .rename(columns={'Current Address Town / City': 'city', 'Current Address State': 'state', 'Current Address ZIP (Postal) Code': 'post_code', 'Current Address 1': 'address_line1', 'Current Address 2': 'address_line2','Current Address District / Suburb': 'district'})

tem = comaddr[['candidate_externalid', 'address_line1']].dropna()
tem['len'] = tem['address_line1'].apply(lambda x: len(x) if x else x)
tem = tem.loc[tem['len']<100]
cp3 = vcand.update_address_line1_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'address_line2']].dropna()
tem['len'] = tem['address_line2'].apply(lambda x: len(x) if x else x)
tem = tem.loc[tem['len']<100]
cp3 = vcand.update_address_line2_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update district
tem = comaddr[['candidate_externalid', 'district']].dropna()
cp4 = vcand.update_location_district(tem, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
from common import vincere_company
vcom = vincere_company.Company(connection)
tem = comaddr[['candidate_externalid','Current Address Country']].dropna()
tem['country_code'] = tem['Current Address Country'].map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'Date of Birth']].dropna().drop_duplicates()
tem['date_of_birth'] = pd.to_datetime(tem['Date of Birth'])
vcand.update_dob(tem, mylog)

# %% reg date
tem = candidate[['candidate_externalid', 'Registered Date']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['Registered Date'])
vcand.update_reg_date(tem, mylog)

# %% source
tem = candidate[['candidate_externalid', 'Source']].dropna().drop_duplicates()
tem['source'] = tem['Source']
vcand.insert_source(tem)

# %% web
tem = candidate[['candidate_externalid', 'Website']].dropna().drop_duplicates()
tem['website'] = tem['Website']
vcand.update_website(tem,mylog)
