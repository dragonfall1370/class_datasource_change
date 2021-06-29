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
cf.read('sr_config.ini')
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
candidate = pd.read_csv(r'D:\Tony\project\vincere_project\CSV__shine\Data\candidate.csv')
candidate['candidate_externalid'] = candidate['Candidate ID']
candidate = candidate.where(candidate.notnull(),None)
assert False
candidate['address'] = candidate[['Current Address', 'Current Address 1','Current Address 2','Current Address District / Suburb','Current Address Town / City','Current Address State','Current Address ZIP (Postal) Code','Current Address Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate['location_name'] = candidate['address']
candidate = candidate.loc[candidate['address']!='']
candidate = candidate.drop_duplicates()
cp2 = vcand.insert_common_location_v2(candidate, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
comaddr = candidate[['candidate_externalid','Current Address 1','Current Address 2','Current Address Town / City','Current Address State','Current Address ZIP (Postal) Code','Current Address Country']].drop_duplicates()\
    .rename(columns={'Current Address Town / City': 'city', 'Current Address State': 'state', 'Current Address ZIP (Postal) Code': 'post_code'})

tem = comaddr[['candidate_externalid', 'Current Address 1']].dropna()
tem['address_line1'] = tem['Current Address 1']
tem['count'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['count']<100]
cp3 = vcand.update_address_line1_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
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

# %% mobile
mphone = candidate[['candidate_externalid', 'Mobile']].dropna()
mphone['mobile_phone'] = mphone['Mobile']
cp = vcand.update_mobile_phone(mphone, mylog)

# %% primary phones
mphone = candidate[['candidate_externalid', 'Primary Phone']].dropna()
mphone['primary_phone'] = mphone['Primary Phone']
cp = vcand.update_primary_phone(mphone, mylog)

# %% linkib
tem = candidate[['candidate_externalid', 'LinkedIn']].dropna()
tem['linkedin'] = tem['LinkedIn']
cp = vcand.update_linkedin_v2(tem, dest_db, mylog)

# %% reg date
tem = candidate[['candidate_externalid', 'Registered Date']].dropna()
tem['reg_date'] = pd.to_datetime(tem['Registered Date'])
cp = vcand.update_reg_date(tem, mylog)