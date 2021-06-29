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
from datetime import datetime
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

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(connection)

def count_days(start_date, end_date):
    date_format = "yyyy-MM-dd HH:mm:ss"
    a = datetime.strptime(start_date, date_format)
    b = datetime.strptime(end_date, date_format)
    delta = b - a
    return delta.days



# %%
sql = """
select CandidateID as candidate_externalid
     , JobID as job_externalid
     , Date
     , StartDate
     , AgreedSalary
     , SalesPercent
     , Notes
     , AdditionalRemuneration
     , AdditionalAmount, NetHours
from PermanentPlacement
"""
placement_detail_info_perm = pd.read_sql(sql, engine_sqlite)

sql1 = """
select CandidateID as candidate_externalid
     , JobID as job_externalid
     , Date
     , StartDate, EndDate, ClientRate, Margin, Rate
     , SalesPercent
     , Notes
     , AdditionalRemuneration
     , AdditionalAmount, NetHours
from ContractPlacement
"""
placement_detail_info_contract = pd.read_sql(sql1, engine_sqlite)
assert False
# %% start date/end date
stdate = placement_detail_info_perm[['job_externalid', 'candidate_externalid', 'StartDate']].dropna()
stdate['start_date'] = stdate['StartDate']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% offer date/place date
jobapp_created_date = placement_detail_info_perm[['job_externalid', 'candidate_externalid', 'Date']].dropna()
jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['Date'])
jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['Date'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
vplace.update_offerdate(jobapp_created_date, mylog)
vplace.update_placeddate(jobapp_created_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% salary
tem = placement_detail_info_perm[['job_externalid', 'candidate_externalid', 'AgreedSalary']].dropna().rename(columns={'AgreedSalary':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()

tem = placement_detail_info_perm[['job_externalid', 'candidate_externalid', 'SalesPercent']].dropna()
tem['percentage_of_annual_salary'] = tem['SalesPercent']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% placement note
note = placement_detail_info_perm[[
    'Notes',
    'AdditionalRemuneration',
    'AdditionalAmount',
    'NetHours',
    'job_externalid',
    'candidate_externalid'
    ]]

note = note.where(note.notnull(), None)
note['AdditionalAmount'] = note['AdditionalAmount'].apply(lambda x: str(x) if x else x)
note['NetHours'] = note['NetHours'].apply(lambda x: str(x) if x else x)
note['TotalRemuneration'] = note['NetHours'].apply(lambda x: str(x) if x else x)
note['note'] = note[['Notes',
    'AdditionalRemuneration',
    'AdditionalAmount',
    'NetHours',]] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Notes', 'Additional Benefits',
                                                                       'Benefits Value', 'Standard Hours'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)






# %% start date/end date
stdate = placement_detail_info_contract[['job_externalid', 'candidate_externalid', 'StartDate', 'EndDate']].dropna()
stdate['start_date'] = stdate['StartDate']
stdate['end_date'] = stdate['EndDate']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
stdate['end_date'] = pd.to_datetime(stdate['end_date'])
cp1 = vplace.update_startdate_enddate(stdate, mylog)

# %% offer date/place date
jobapp_created_date = placement_detail_info_contract[['job_externalid', 'candidate_externalid', 'Date']].dropna()
jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['Date'])
jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['Date'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
vplace.update_offerdate(jobapp_created_date, mylog)
vplace.update_placeddate(jobapp_created_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% markup
tem = placement_detail_info_contract[['job_externalid', 'candidate_externalid', 'SalesPercent']].dropna()
tem['markup_percent'] = tem['SalesPercent'].astype(float)
cp8 = vplace.update_markup_percent(tem, mylog)

# %% payrate
tem = placement_detail_info_contract[['job_externalid', 'candidate_externalid', 'Rate']].dropna()
tem['pay_rate'] = tem['Rate'].astype(float)
cp8 = vplace.update_pay_rate(tem, mylog)

# %% charge rate
tem = placement_detail_info_contract[['job_externalid', 'candidate_externalid', 'ClientRate']].dropna()
tem['charge_rate'] = tem['ClientRate'].astype(float)
cp1 = vplace.update_charge_rate(tem, mylog)

# %% profit
tem = placement_detail_info_contract[['job_externalid', 'candidate_externalid', 'Margin']].dropna()
tem['profit'] = tem['Margin'].astype(float)
cp1 = vplace.update_profit(tem, mylog)

# %% placement note
note = placement_detail_info_perm[[
    'Notes',
    'AdditionalRemuneration',
    'AdditionalAmount',
    'NetHours',
    'job_externalid',
    'candidate_externalid'
    ]]

note = note.where(note.notnull(), None)
note['AdditionalAmount'] = note['AdditionalAmount'].apply(lambda x: str(x) if x else x)
note['NetHours'] = note['NetHours'].apply(lambda x: str(x) if x else x)
note['TotalRemuneration'] = note['NetHours'].apply(lambda x: str(x) if x else x)
note['note'] = note[['Notes',
    'AdditionalRemuneration',
    'AdditionalAmount',
    'NetHours',]] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Notes', 'Additional Benefits',
                                                                       'Benefits Value', 'Standard Hours'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)


