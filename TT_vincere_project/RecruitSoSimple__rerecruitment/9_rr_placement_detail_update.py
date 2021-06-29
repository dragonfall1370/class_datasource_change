# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
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
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
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

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(connection)


# %% permanent
placed_info = pd.read_sql("""
select concat('RSS',Candidate) as candidate_externalid
     , concat('RSS',Vacancy) as job_externalid
     , Status
     , [Start Date]
     , [Contract Duration]
     , Revenue
     , Salary,Status
from Vacancies_Candidates
where Status in ('Placed','Accepted')
""", engine_mssql)
placed_info['contract_length'] = placed_info['Contract Duration'].apply(lambda x: x.split(' ')[0] if x else x)
placed_info['contract_length'] = placed_info['contract_length'].apply(lambda x: int(x) if x else x)
placed_info['contract_length_type'] = placed_info['Contract Duration'].apply(lambda x: x.split(' ')[1] if x else x)

placed_info['profit'] = placed_info['Revenue'].apply(lambda x: x.split(' ')[1] if x else x)
placed_info['sal'] = placed_info['Salary'].apply(lambda x: x.split(' ')[1] if x else x)
placed_info['type'] = placed_info['Salary'].apply(lambda x: x.split(' ')[-1] if x else x)
placed_info = placed_info.where(placed_info.notnull(), None)
assert False
# %% start date/end date
stdate = placed_info[['job_externalid', 'candidate_externalid', 'Start Date']].dropna()
stdate['start_date'] = pd.to_datetime(stdate['Start Date'], format='%d/%m/%Y')
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% offer date/place date
jobapp_created_date = placed_info[['job_externalid', 'candidate_externalid', 'Start Date']].dropna()
jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['Start Date'], format='%d/%m/%Y')
jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['Start Date'], format='%d/%m/%Y')
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
vplace.update_offerdate(jobapp_created_date, mylog)
vplace.update_placeddate(jobapp_created_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% contract length
tem = placed_info[['job_externalid', 'candidate_externalid', 'contract_length']].dropna()
tem['contract_length'] = tem['contract_length'].astype(int)
cp1 = vplace.update_contract_length(tem, mylog)

# %% contract length type
tem = placed_info[['job_externalid', 'candidate_externalid', 'contract_length_type']].dropna()
tem['contract_length_type'].unique()
tem1 = tem.loc[tem['contract_length_type']=='Months']
vplace.update_contract_length_type(tem1,'month', mylog)

tem2 = tem.loc[tem['contract_length_type']=='Weeks']
vplace.update_contract_length_type(tem2,'week', mylog)

tem3 = tem.loc[tem['contract_length_type']=='Days']
vplace.update_contract_length_type(tem3,'day', mylog)

# %% salary type
tem = placed_info[['job_externalid', 'candidate_externalid', 'type']].dropna()
tem['type'].unique()
tem.loc[tem['type'] == 'P/M']
tem.loc[(tem['type'] == 'P/A'), 'salary_type'] = 1
tem.loc[(tem['type'] == 'P/M'), 'salary_type'] = 2
tem2 = tem[['job_externalid', 'candidate_externalid', 'salary_type']].dropna()
tem2['salary_type'] = tem2['salary_type'].astype(int)
vplace.update_salary_type(tem2,mylog)

# # %% payrate
# tem = placed_info[['job_externalid', 'candidate_externalid', 'Pay_Rate']].dropna()
# tem['pay_rate'] = tem['Pay_Rate'].astype(float)
# cp8 = vplace.update_pay_rate(tem, mylog)
#
# # %% charge rate
# tem = placed_info[['job_externalid', 'candidate_externalid', 'Charge_Rate']].dropna()
# tem['charge_rate'] = tem['Charge_Rate'].astype(float)
# cp1 = vplace.update_charge_rate(tem, mylog)
#
# # %% profit
# tem = placed_info[['job_externalid', 'candidate_externalid', 'Charge_Rate', 'Pay_Rate']].dropna()
# tem['profit'] = tem['Charge_Rate'].astype(float) - tem['Pay_Rate'].astype(float)
# tem['profit'] = tem['profit'].astype(float)
# cp1 = vplace.update_profit(tem, mylog)
#
# # %% split
# user = pd.read_csv('user.csv')
# user['matcher'] = user['Email in Gel'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem = placed_info[['job_externalid', 'candidate_externalid', 'owner']].dropna()
# tem['matcher'] = tem['owner'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem = tem.merge(user, on='matcher')
# tem['user_email'] = tem['Email for Vincere login']
# tem['shared']=100
# vplace.insert_profit_split_mode_percentage(tem, mylog)
monthlysalary = placed_info[['job_externalid', 'candidate_externalid', 'type','sal']].dropna()
monthlysalary = monthlysalary.loc[monthlysalary['type'] =='P/M']
monthlysalary['present_salary_rate'] = monthlysalary['sal']
monthlysalary['present_salary_rate'] = monthlysalary['present_salary_rate'].astype(float)
vplace.update_salary_monthly(monthlysalary,mylog)

# %%
payinterval = placed_info[['job_externalid', 'candidate_externalid', 'type']].dropna()
payinterval['type'].unique()
payinterval.loc[(payinterval['type'] == 'P/A'), 'pay_interval'] = 'annual'
payinterval.loc[(payinterval['type'] == 'P/H'), 'pay_interval'] = 'hourly'
payinterval.loc[(payinterval['type'] == 'P/D'), 'pay_interval'] = 'daily'
payinterval.loc[(payinterval['type'] == 'P/M'), 'pay_interval'] = 'monthly'
payinterval= payinterval[['job_externalid', 'candidate_externalid', 'pay_interval']].dropna()
vplace.update_pay_interval(payinterval,mylog)

# %% salary
tem = placed_info[['job_externalid', 'candidate_externalid', 'sal']].dropna().rename(columns={'sal':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% charge_rate
tem = placed_info[['job_externalid', 'candidate_externalid', 'profit']].dropna()
tem['charge_rate'] = tem['profit'].astype(float)
cp6 = vplace.update_charge_rate(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()
tem = placed_info[['job_externalid', 'candidate_externalid', 'profit','sal']].dropna()
tem['percentage_of_annual_salary'] = tem['profit'].astype(float)/tem['sal'].astype(float)
tem['percentage_of_annual_salary'].unique()
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary']*100
tem = tem.loc[tem['percentage_of_annual_salary']<=100]
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% placement note
note = placed_info[['job_externalid', 'candidate_externalid', 'Status']].dropna()
note['note'] = note[['Status']].apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Status'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)

