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
cf.read('wm_config.ini')
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
candidate['Current Address ZIP (Postal) Code'] = candidate['Current Address ZIP (Postal) Code'].apply(lambda x: str(x).split('.')[0] if x else x)
c_location = candidate[['candidate_externalid','Current Address Town / City', 'Current Address ZIP (Postal) Code','Current Address Country']]
c_location['address'] = c_location[['Current Address Town / City', 'Current Address ZIP (Postal) Code','Current Address Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['address']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
comaddr = c_location[['candidate_externalid','Current Address Town / City', 'Current Address ZIP (Postal) Code','Current Address Country']].drop_duplicates()\
    .rename(columns={'Current Address Town / City': 'city', 'Current Address ZIP (Postal) Code': 'post_code'})

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)

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

# %% relocate
tem = candidate[['candidate_externalid', 'Will Relocate (Yes / No)']].dropna().drop_duplicates()
tem.loc[(tem['Will Relocate (Yes / No)'] == 'No'), 'relocate'] = 0
tem.loc[(tem['Will Relocate (Yes / No)'] == 'Yes'), 'relocate'] = 1
vcand.update_will_relocate(tem, mylog)

# %% reg date
tem = candidate[['candidate_externalid', 'Registered Date']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['Registered Date'])
vcand.update_reg_date(tem, mylog)

# %% source
tem = candidate[['candidate_externalid', 'Source']].dropna().drop_duplicates()
tem['source'] = tem['Source']
vcand.insert_source(tem)

# %% gender
tem = candidate[['candidate_externalid', 'Gender (Male, Female, Other)']].dropna().drop_duplicates()
tem.loc[(tem['Gender (Male, Female, Other)'] == 'Male'), 'male'] = 1
tem.loc[(tem['Gender (Male, Female, Other)'] == 'Female'), 'male'] = 0
vcand.update_gender(tem,mylog)

# %% jon type
tem = candidate[['candidate_externalid', 'Job Type (Permanent, Contract or Temporary)']].dropna().drop_duplicates()
tem['Job Type (Permanent, Contract or Temporary)'].unique()
tem1 = tem.loc[tem['Job Type (Permanent, Contract or Temporary)']=='Perm']
tem1['desired_job_type'] = 'permanent'
tem2 = tem.loc[tem['Job Type (Permanent, Contract or Temporary)']=='Contract']
tem2['desired_job_type'] = 'contract'
tem3 = pd.concat([tem1,tem2])

tem4 = tem.loc[~tem['candidate_externalid'].isin(tem3['candidate_externalid'])]
tem4['desired_job_type'] = 'permanent'
tem5 = tem4.copy()
tem5['desired_job_type'] = 'contract'
jt = pd.concat([tem3,tem4,tem5])
vcand.update_desired_job_type(jt,mylog)

# %% linkedin
tem = candidate[['candidate_externalid', 'LinkedIn']].dropna().drop_duplicates()
tem['linkedin'] = tem['LinkedIn']
vcand.update_linkedin(tem, mylog)

# %% xing
tem = candidate[['candidate_externalid', 'Xing']].dropna().drop_duplicates()
tem['xing'] = tem['Xing']
vcand.update_xing(tem, mylog)

# %% fb
tem = candidate[['candidate_externalid', 'Facebook']].dropna().drop_duplicates()
tem['facebook'] = tem['Facebook']
vcand.update_facebook(tem, mylog)

# %% twitter
tem = candidate[['candidate_externalid', 'Twitter']].dropna().drop_duplicates()
tem['twitter'] = tem['Twitter']
vcand.update_twitter(tem, mylog)

# %% education
edu = candidate[['candidate_externalid', 'Degree Name','Qualification']].drop_duplicates()
edu['degreeName'] = edu['Degree Name']
edu['qualification'] = edu['Qualification']
cp9 = vcand.update_education(edu, dest_db, mylog)

# %% sub
tem = candidate[['Primary Email','Email Subscribed (Yes/No)']].dropna().drop_duplicates()
tem['email'] = tem['Primary Email']
tem.loc[(tem['Email Subscribed (Yes/No)'] == 'Yes'), 'subscribed'] = 1
tem.loc[(tem['Email Subscribed (Yes/No)'] == 'No'), 'subscribed'] = 0
tem = tem.drop_duplicates()
vcand.email_subscribe(tem,mylog)