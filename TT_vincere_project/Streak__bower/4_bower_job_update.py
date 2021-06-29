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

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'),
                                                                  dest_db.get('server'), dest_db.get('port'),
                                                                  dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8',
                                          use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_job

vjob = vincere_job.Job(connection)

# %%
sql = """
select ID as job_externalid
     , Stage
     , Salary
     , "Company Size"
     , "Yrs Exp"
     , "Exclusive?"
     , "Perm/Temp/FTC"
     , "Contract type"
     , Essential
     , Desired
     , "Degree?"
     , Notes
     , "Role Source"
     , "Client type (New/Returning)"
     , "Agreed terms?"
     , "Fee %"
     , "Rebate Clause"
     , "Reason for Loss (if applicable)"
     , "Start date" 
     , "End date"
     , "Role close ID"
     , "Date Created"
     , "In role at 1 month?"
     , "In role 12 weeks?"
     , "In role 1 yr?"
     , "Reason for departure"
     , Country
     , Currency
     , "Time interval"
     , "Contract Length"
     , "Contract Unit (Time)", Industry
from Job
"""
job = pd.read_sql(sql, engine_sqlite)
job['job_externalid'] = job['job_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% currency
tem = job[['job_externalid', 'Currency']].dropna()
tem.loc[tem['Currency'] == 'United Kingdom Pound Sterling', 'currency_type'] = 'pound'
tem.loc[tem['Currency'] == 'European Euro', 'currency_type'] = 'euro'
tem.loc[tem['Currency'] == 'United States Dollar', 'currency_type'] = 'usd'
tem2 = tem[['job_externalid', 'currency_type']].dropna()
vjob.update_currency_type(tem2, mylog)

# %% country
tem = job[['job_externalid', 'Country']].dropna()
tem['country_code'] = tem.Country.map(vincere_common.get_country_code)
vjob.update_country(tem, mylog)

# %% open date
od = job[['job_externalid', 'Start date']].dropna()
od['start_date'] = pd.to_datetime(od['Start date'])
cp2 = vjob.update_start_date(od, mylog)

ed = job[['job_externalid', 'End date']].dropna()
ed['close_date'] = pd.to_datetime(ed['End date'])
cp2 = vjob.update_close_date(ed, mylog)

# %% job type
jt = job[['job_externalid', 'Perm/Temp/FTC']].dropna()
jt.loc[jt['Perm/Temp/FTC'] == 'Perm', 'job_type'] = 'permanent'
jt.loc[jt['Perm/Temp/FTC'] == 'Temp', 'job_type'] = 'contract'
jt = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt, mylog)

# %% interval
tem = job[['job_externalid', 'Time interval']].dropna()
tem['pay_interval'] = tem['Time interval'].str.lower()
cp5 = vjob.update_pay_interval(tem, mylog)

# %% length & type
tem = job[['job_externalid', 'Contract Length', 'Contract Unit (Time)']].dropna()
tem['Contract Unit (Time)'].unique()
tem.loc[tem['Contract Unit (Time)'] == 'Months', 'contract_length_type'] = 'month'
tem.loc[tem['Contract Unit (Time)'] == 'Month', 'contract_length_type'] = 'month'
tem.loc[tem['Contract Unit (Time)'] == 'Weeks', 'contract_length_type'] = 'week'
tem.loc[tem['Contract Unit (Time)'] == 'Week', 'contract_length_type'] = 'week'
tem.loc[tem['Contract Unit (Time)'] == 'Days', 'contract_length_type'] = 'day'
tem.loc[tem['Contract Unit (Time)'] == 'Day', 'contract_length_type'] = 'day'
tem['contract_length'] = tem['Contract Length']
tem['contract_length'] = tem['contract_length'].astype(float)
cp5 = vjob.update_contract_length(tem, mylog)

# %% annual salary
actsa = job[['job_externalid', 'Salary']].dropna()
actsa['actual_salary'] = actsa['Salary']
actsa['actual_salary'] = actsa['actual_salary'].apply(lambda x: x.split('-')[0])
actsa['actual_salary'] = actsa['actual_salary'].str.strip()
actsa['actual_salary'] = actsa['actual_salary'].apply(lambda x: x.replace('$75', '75000'))
actsa['actual_salary'] = actsa['actual_salary'].apply(lambda x: x.replace('¬£', '')
                                                      .replace(',', '').replace('k', '000')
                                                      .replace('ph', '').replace('per day', '')
                                                      .replace('pro rata', '').replace('Up to', '')
                                                      .replace('per hour', '').replace('day rate', '')
                                                      .replace('plus', '').replace('‚Ç¨', '').replace('$', ''))
actsa['actual_salary'] = actsa['actual_salary'].apply(lambda x: x.replace('', ''))
actsa['actual_salary'] = actsa['actual_salary'].str.strip()
actsa['actual_salary'] = actsa['actual_salary'].apply(lambda x: x.split(' ')[0])
actsa["actual_salary_bool"] = actsa["actual_salary"].str.isdigit()
tem1 = actsa.loc[actsa["actual_salary_bool"] == True]
tem1['actual_salary'] = tem1['actual_salary'].astype(float)
cp7 = vjob.update_actual_salary(tem1, mylog)

# %% quick fee forcast
qikfeefor = job[['job_externalid', 'Fee %']].dropna()
qikfeefor['Fee %'] = qikfeefor['Fee %'].apply(lambda x: x.replace('%', ''))
qikfeefor['fee_bool'] = qikfeefor["Fee %"].str.isdigit()
tem = qikfeefor.loc[qikfeefor['fee_bool'] == True]
tem['use_quick_fee_forecast'] = 1
tem['percentage_of_annual_salary'] = tem['Fee %']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
cp10 = vjob.update_use_quick_fee_forecast(tem, mylog)
cp11 = vjob.update_percentage_of_annual_salary(tem, mylog)

# %% markup
qikfeefor = job[['job_externalid', 'Fee %']].dropna()
qikfeefor['Fee %'] = qikfeefor['Fee %'].apply(lambda x: x.replace('%', ''))
qikfeefor['fee_bool'] = qikfeefor["Fee %"].str.isdigit()
tem = qikfeefor.loc[qikfeefor['fee_bool'] == True]
tem['markup_percent'] = tem['Fee %']
tem['markup_percent'] = tem['markup_percent'].astype(float)
tem.loc[tem['job_externalid'] == '37']
cp11 = vjob.update_compensation_markup_percent(tem, mylog)

# %% update skills
tem = job[['job_externalid', 'Essential', 'Desired']]
tem['key_words'] = tem[['Essential', 'Desired']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
vjob.update_key_words(tem, mylog)

# %% rate_type
employment_type = job[['job_externalid', 'Contract type']].dropna()
employment_type['employment_type'].unique()
employment_type.loc[(employment_type['Contract type'] == 'Full-time'), 'employment_type'] = 0
employment_type.loc[(employment_type['Contract type'] == 'Part-time'), 'employment_type'] = 1
tem = employment_type[['job_externalid', 'employment_type']].dropna()
employment_type.loc[employment_type['employment_type'] == 1]
vjob.update_employment_type(tem, mylog)

# %% note
note = job[[
    'job_externalid',
    'Stage',
    'Company Size',
    'Yrs Exp',
    'Exclusive?',
    'Degree?',
    'Notes',
    'Role Source',
    'Client type (New/Returning)',
    'Agreed terms?',
    'Rebate Clause',
    'Reason for Loss (if applicable)',
    'Role close ID'
    , 'In role at 1 month?'
    , 'In role 12 weeks?'
    , 'In role 1 yr?'
    , 'Reason for departure']]

note['note'] = note[['Stage',
                     'Company Size',
                     'Yrs Exp',
                     'Exclusive?',
                     'Degree?',
                     'Notes',
                     'Role Source',
                     'Client type (New/Returning)',
                     'Agreed terms?',
                     'Rebate Clause',
                     'Reason for Loss (if applicable)',
                     'Role close ID'
    , 'In role at 1 month?'
    , 'In role 12 weeks?'
    , 'In role 1 yr?'
    , 'Reason for departure']] \
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Stage',
                                                           'Company Size',
                                                           'Yrs Exp',
                                                           'Exclusive?',
                                                           'Degree?',
                                                           'Notes',
                                                           'Role Source',
                                                           'Client type (New/Returning)',
                                                           'Agreed terms?',
                                                           'Rebate Clause',
                                                           'Reason for Loss (if applicable)',
                                                           'Role close ID'
                                                              , 'In role at 1 month?'
                                                              , 'In role 12 weeks?'
                                                              , 'In role 1 yr?'
                                                              , 'Reason for departure'], x) if e[1]]), axis=1)
vjob.update_note(note, mylog)

# %% reg date
reg_date = job[['job_externalid', 'Date Created']].dropna().drop_duplicates()
reg_date['reg_date'] = pd.to_datetime(reg_date['Date Created'])
vjob.update_reg_date(reg_date, mylog)

# %% industries
industry = job[['job_externalid', 'Industry']].dropna().drop_duplicates()
industry['name'] = industry['Industry']
industry = industry.drop_duplicates().dropna()
industry['name'] = industry['name'].str.strip()
cp10 = vjob.insert_job_industry(industry, mylog)

# %% fix name
sql = """
select id, name from position_description
"""
job = pd.read_sql(sql, engine_postgre)
job['name'] = job['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.load_data_to_vincere(job, dest_db, 'update', 'position_description', ['name', ], ['id'], mylog)