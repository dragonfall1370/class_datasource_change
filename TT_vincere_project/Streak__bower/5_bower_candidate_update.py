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
cf.read('bower_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
candidate = pd.read_sql("""
select ID as candidate_externalid
     , Status
     , "Date Created"
     , Profession
     , "Notice Period"
     , Rating
     , "Current Salary"
     , "Full-time"
     , "Part-time"
     , Temp
     , Contract
     , "Referred from"
     , "Primary Location"
     , Languages
     , "Degree Y/N"
     , Telephone
     , "Passport: Checked in Person?"
     , "Passport: Electronic copy?"
     , "On a VISA?"
     , "VISA Type"
     , "VISA Expiry"
     , Notes
     , Stage, L1, L2, L3, "Meeting Notes Count", Currency, "Contract Length", "Contract rate", "Current Industry"
from Candidate
""", engine_sqlite)
candidate['candidate_externalid'] = candidate['candidate_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% job type
tem1 = candidate.loc[candidate['Temp'] == 'Checked']
tem2 = candidate.loc[candidate['Contract'] == 'Checked']
tem = pd.concat([tem1, tem2])
tem['desired_job_type'] = 'contract'
jobtype = tem[['candidate_externalid', 'desired_job_type']].dropna()
jobtype['desired_job_type'].unique()
cp = vcand.update_desired_job_type_2(jobtype, mylog)

# %% location name/address
tem = candidate[['candidate_externalid', 'Primary Location']].dropna()
tem['location_name'] = tem['Primary Location']
tem['address'] = tem.location_name
tem1 = tem[['candidate_externalid', 'address', 'location_name']].drop_duplicates()
cp2 = vcand.insert_common_location_v2(tem1, dest_db, mylog)

# %% phones
indt = candidate[['candidate_externalid', 'Telephone']].dropna()
indt['primary_phone'] = indt['Telephone']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid','Profession']].dropna()
cur_emp['current_employer'] = ''
cur_emp['current_job_title'] = cur_emp['Profession']
cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\u2028',''))
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

# %% note
candidate['Met?'].unique()
candidate.info()
candidate.loc[candidate['Meeting Notes Count'] == '0', 'Met?'] = 'Not met'
candidate.loc[candidate['Meeting Notes Count'] != '0', 'Met?'] = 'Met'
note = candidate[[
    'candidate_externalid',
    'Status',
    'Notice Period',
    'Rating',
    'Met?'
    , "Passport: Checked in Person?"
     , "Passport: Electronic copy?"
     , "On a VISA?"
     , "VISA Type"
     , "VISA Expiry"
     , 'Notes'
     , 'Stage', 'L1', 'L2', 'L3']]

note['note'] = note[['Status',
    'Notice Period',
    'Rating',
    'Met?'
    , "Passport: Checked in Person?"
     , "Passport: Electronic copy?"
     , "On a VISA?"
     , "VISA Type"
     , "VISA Expiry"
     , 'Notes'
     , 'Stage', 'L1', 'L2', 'L3']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Status',
    'Notice Period',
    'Rating',
    'Met?'
    , "Passport: Checked in Person?"
     , "Passport: Electronic copy?"
     , "On a VISA?"
     , "VISA Type"
     , "VISA Expiry"
     , 'Notes'
     , 'Stage', 'L1', 'L2', 'L3'], x) if e[1]]), axis=1)
cp11 = vcand.update_note2(note, dest_db, mylog)

# %% source
src = pd.read_csv('source.csv')
src['matcher'] = src['Vincere value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem = candidate[['candidate_externalid', 'Referred from']].dropna().drop_duplicates()
tem['matcher'] = tem['Referred from'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem = tem.merge(src, on='matcher')
tem['Vincere value'].unique()
tem['source'] = tem['Vincere value']
tem2 = tem[['candidate_externalid', 'source']]

tem3 = pd.DataFrame({"candidate_externalid":['A', 'B', 'C','D','E','F'],
                    "source":['word of mouth referral','Candidate referral','Events',
                              'Online Job Board', 'Events','Groups & Lists']})
source = pd.concat([tem2,tem3])
vcand.insert_source(source)

# %% current salary
tem = candidate[['candidate_externalid', 'Current Salary']].dropna().drop_duplicates()
tem['current_salary'] = tem['Current Salary']
tem['current_salary'] = tem['current_salary'].apply(lambda x: x.split('-')[0])
tem['current_salary'] = tem['current_salary'].str.strip()
tem['current_salary'] = tem['current_salary'].apply(lambda x: x.replace('$75','75000'))
tem['current_salary'] = tem['current_salary'].apply(lambda x: x.replace('¬£','')
                                                      .replace(',','').replace('k','000').replace('K','000')
                                                      .replace('ph','').replace('per day','').replace('+','')
                                                      .replace('pro rata','').replace('Up to','')
                                                      .replace('per hour','').replace('day rate','')
                                                      .replace('plus','').replace('‚Ç¨','').replace('$',''))
tem['current_salary'] = tem['current_salary'].apply(lambda x: x.replace('',''))
tem['current_salary'] = tem['current_salary'].str.strip()
tem['current_salary'] = tem['current_salary'].apply(lambda x: x.split(' ')[0])
tem["actual_salary_bool"]= tem['current_salary'].str.isdigit()
tem1 = tem.loc[tem['actual_salary_bool']==True]
tem1['current_salary'] = tem1['current_salary'].astype(float)
vcand.update_current_salary(tem1, mylog)

# %% education summary
tem = candidate[['candidate_externalid', 'Languages']].drop_duplicates()
tem['education_summary'] = tem[['Languages']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Languages'], x) if e[1]]), axis=1)
vcand.update_education_summary_v2(tem, dest_db, mylog)

# %% education
edu = candidate[['candidate_externalid','Degree Y/N']].dropna().drop_duplicates()
edu.loc[edu['Degree Y/N'] == 'Y', 'educationId'] = '4'
tem = edu[['candidate_externalid','educationId']].dropna()
cp9 = vcand.update_education(tem, mylog)

# %% employment type
tem1 = candidate.loc[candidate['Full-time'] == 'Checked']
tem2 = candidate.loc[candidate['Part-time'] == 'Checked']
tem = pd.concat([tem1, tem2])
tem.loc[tem['Full-time'] == 'Checked', 'employment_type'] = 'fulltime'
tem.loc[tem['Part-time'] == 'Checked', 'employment_type'] = 'parttime'
emptype = tem[['candidate_externalid', 'employment_type']].dropna()
emptype['employment_type'].unique()
vcand.update_employment_type(emptype, mylog)

# %% reg date
reg_date = candidate[['candidate_externalid', 'Date Created']].dropna().drop_duplicates()
reg_date['reg_date'] = pd.to_datetime(reg_date['Date Created'])
vcand.update_reg_date(reg_date, mylog)

# %% Currency
tem = candidate[['candidate_externalid', 'Currency']].dropna().drop_duplicates()
tem['Currency'].unique()
tem.loc[tem['Currency'] == 'United Kingdom Pound Sterling', 'currency'] = 'pound'
tem.loc[tem['Currency'] == 'European Euro', 'currency'] = 'euro'
tem.loc[tem['Currency'] == 'United States Dollar', 'currency'] = 'usd'
tem2 = tem[['candidate_externalid', 'currency']].dropna()
vcand.update_currency_type(tem2, mylog)

# %% interval
tem = candidate[['candidate_externalid', 'Contract Length']].dropna()
tem['Contract Length'].unique()
tem['contract_interval'] = tem['Contract Length'].str.lower()
cp5 = vcand.update_contract_interval(tem, mylog)

# %% interval
tem = candidate[['candidate_externalid', 'Contract rate']].dropna()
tem['Contract rate'].unique()
tem['contract_rate'] = tem['Contract rate']
tem['contract_rate'] = tem['contract_rate'].astype(float)
cp5 = vcand.update_contract_rate(tem, mylog)

# %% industries
industry = candidate[['candidate_externalid', 'Current Industry']].dropna().drop_duplicates()
industry['name'] = industry['Current Industry']
industry = industry.drop_duplicates().dropna()
industry['name'] = industry['name'].str.strip()
cp8 = vcand.insert_candidate_industry(industry, mylog)

# %% fix salary
df = pd.read_csv(r'C:\Users\tony\Desktop\rytons\bower_talent_update_candidate_salary.csv')
df['id'] = df['candidate_id']
df['current_salary'] = df['salary']
vincere_custom_migration.psycopg2_bulk_update_tracking(df, connection, ['current_salary', ], ['id', ], 'candidate', mylog)
