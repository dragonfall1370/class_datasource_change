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
cf.read('psg_config.ini')
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
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_job
vjob = vincere_job.Job(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)

# %%
sql = """ 
select JobID as job_externalid,
       JobTitle,
       jt.DisplayName as jobtype,
       js.DisplayName as status,
       nullif(convert(varchar,JobLocation),'') as JobLocation,
       nullif(convert(varchar,JobPayRate),'') as JobPayRate,
       nullif(convert(varchar,JobNumberOfPositions),'') as JobNumberOfPositions,
       nullif(convert(varchar,JobChargeRate),'') as JobChargeRate,
       nullif(convert(varchar,JobUpdatedDate),'') as JobUpdatedDate,
       nullif(convert(varchar,JobClosingDate),'') as JobClosingDate,
       nullif(convert(nvarchar(max),JobTechnicalNotes),'') as JobTechnicalNotes,
       nullif(convert(varchar,JobRateSalaryComm),'') as JobRateSalaryComm, 
       nullif(convert(nvarchar(max),JobMainSkills),'') as JobMainSkills,
       nullif(convert(varchar,JobReceivedDate),'') as JobReceivedDate,
       nullif(convert(varchar,JobStartDate),'') as JobStartDate,
       nullif(convert(varchar,JobEndDate),'') as JobEndDate,
       tRP.RPName as chargedUnit, 
       tRP2.RPName as payUnit
from Job j
left join JobStatus js on js.JobStatusID = j.JobStatusID
left join JobType jt on jt.JobTypeID = j.JobTypeID
left join tblRatePeriod tRP on j.JobChargeRatePeriodID = tRP.RatePeriodID
left join tblRatePeriod tRP2 on j.JobPayRatePeriodID = tRP.RatePeriodID
"""
job = pd.read_sql(sql, engine_mssql.raw_connection())
job['job_externalid'] =job['job_externalid'].apply(lambda x: str(x) if x else x)
assert False

# %% currency country
tem = job[['job_externalid']]
tem['currency_type'] = 'aud'
tem['country_code'] = 'AU'
vjob.update_currency_type(tem, mylog)
vjob.update_country_code(tem, mylog)

# %% public description
sql = """ 
select JobID as job_externalid, HTMLDescription as public_description from InternetAdvertising where HTMLDescription is not null 
"""
tem = pd.read_sql(sql, engine_mssql.raw_connection())
tem['job_externalid'] =tem['job_externalid'].apply(lambda x: str(x) if x else x)
cp8 = vjob.update_public_description(tem, mylog)
#
# # %% rate_type
# employment_type = job[['job_externalid', 'FullPart']].dropna()
# employment_type['FullPart'].unique()
# employment_type.loc[(employment_type['FullPart'] == 'Full-time'), 'employment_type'] = 0
# employment_type.loc[(employment_type['FullPart'] == 'Part-time'), 'employment_type'] = 1
# tem = employment_type[['job_externalid', 'employment_type']].dropna()
# employment_type.loc[employment_type['employment_type'] == 1]
# vjob.update_employment_type(tem, mylog)

# %% head count
hc = job[['job_externalid', 'JobNumberOfPositions']].dropna()
hc['head_count'] = hc['JobNumberOfPositions']
# hc['head_count'] = hc['head_count'].astype(float)
hc['head_count'] = hc['head_count'].astype(int)
cp1 = vjob.update_head_count(hc, mylog)

# %% job type
jt_mapping = pd.read_csv('job_type.csv')
jt_mapping['matcher'] = jt_mapping['TRIS Employment Type'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
jt = job[['job_externalid', 'jobtype']].dropna()
jt['matcher'] = jt['jobtype'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
jt = jt.merge(jt_mapping, on='matcher', how ='left')
jt['Vincere Job Type'].unique()
jt['job_type'] = jt['Vincere Job Type'].apply(lambda x: x.lower())
cp5 = vjob.update_job_type(jt, mylog)

# %% interval
jt = job[['job_externalid', 'payUnit']].dropna()
jt['payUnit'].unique()
jt.loc[jt['payUnit']=='per hour', 'pay_interval'] = 'hourly'
jt.loc[jt['payUnit']=='per day', 'pay_interval'] = 'daily'
jt.loc[jt['payUnit']=='per week', 'pay_interval'] = 'weekly'
jt.loc[jt['payUnit']=='per month', 'pay_interval'] = 'monthly'
jt.loc[jt['payUnit']=='per annum', 'pay_interval'] = 'yearly'
jt2 = jt[['job_externalid', 'pay_interval']].dropna()
cp5 = vjob.update_pay_interval(jt2, mylog)

# %% start date close date
tem = job[['job_externalid', 'JobReceivedDate']].dropna().rename(columns={'JobReceivedDate': 'start_date'})
tem['start_date'] = pd.to_datetime(tem['start_date'])
vjob.update_start_date(tem, mylog)

from datetime import date, timedelta
tem = job[['job_externalid']]
today = date.today()
yesterday = today - timedelta(days = 1)
tem['close_date'] = yesterday
vjob.update_close_date(tem, mylog)
#
# tem = job[['job_externalid', 'JobClosingDate']].dropna().rename(columns={'JobClosingDate': 'close_date'})
# tem['close_date'] = pd.to_datetime(tem['close_date'])
# vjob.update_close_date(tem, mylog)
#
# tem = job[['job_externalid', 'JobEndDate']].dropna().rename(columns={'JobEndDate': 'close_date'})
# tem['close_date'] = pd.to_datetime(tem['close_date'])
# vjob.update_close_date(tem, mylog)
#
# # %% salary from/to
# frsala = job[['job_externalid', 'Salary']].dropna()
# frsala['salary_from'] = frsala['Salary']
# frsala['salary_from'] = frsala['salary_from'].astype(float)
# cp6 = vjob.update_salary_from(frsala, mylog)
#
# tosala = job[['job_externalid', 'Salary1']].dropna()
# tosala['salary_to'] = tosala['Salary1']
# tosala['salary_to'] = tosala['salary_to'].astype(float)
# cp7 = vjob.update_salary_to(tosala, mylog)

# %% last activity date
tem = job[['job_externalid', 'JobUpdatedDate']].dropna().drop_duplicates()
tem['last_activity_date'] = pd.to_datetime(tem['JobUpdatedDate'])
vjob.update_last_activity_date(tem, mylog)

# %% annual salary
sal = job[['job_externalid', 'JobPayRate']].dropna()
sal['actual_salary'] = sal['JobPayRate']
sal['actual_salary'] = sal['actual_salary'].astype(float)
vjob.update_actual_salary(sal, mylog)

# %% charged
sal = job[['job_externalid', 'JobChargeRate']].dropna()
sal['charge_rate'] = sal['JobChargeRate']
sal['charge_rate'] = sal['charge_rate'].astype(float)
vjob.update_charge_rate(sal, mylog)

# %% quick fee forcast
# qikfeefor = job[['job_externalid', 'Fee']].dropna()
#
# qikfeefor['use_quick_fee_forecast'] = 1
# qikfeefor['percentage_of_annual_salary'] = qikfeefor['EstimatedFee']
# qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
# vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
# vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% reg date
tem = job[['job_externalid', 'JobReceivedDate']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['JobReceivedDate'])
vjob.update_reg_date(tem, mylog)

# %% note
# job['note'] = job[['job_externalid','status'
#     , 'JobRateSalaryComm', 'JobLocation', 'chargedUnit', 'JobTechnicalNotes'
#     , 'JobMainSkills']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID','Status', 'Rate/Salary Comm', 'Location', 'Charge Unit', 'Job Information'
#                                                             , 'Main Skills\Technical Notes'], x) if e[1]]), axis=1)
job['note'] = job[['job_externalid', 'JobTechnicalNotes']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID','Job Information'], x) if e[1]]), axis=1)
vjob.update_note2(job,dest_db, mylog)

# %% payrate
# contract_job_payrate = job[['job_externalid', 'contractorrate']].dropna()
# contract_job_payrate['pay_rate'] = contract_job_payrate['contractorrate']
# contract_job_payrate['pay_rate'] = contract_job_payrate['pay_rate'].astype(float)
# vjob.update_pay_rate(contract_job_payrate, mylog)
#
# # %% charge rate
# contract_job_chargerate = job[['job_externalid', 'clientrate']].dropna()
# contract_job_chargerate['charge_rate'] = contract_job_chargerate['clientrate']
# contract_job_chargerate['charge_rate'] = contract_job_chargerate['charge_rate'].astype(float)
# cp13 = vjob.update_charge_rate(contract_job_chargerate, mylog)


# %%
# p_date = pd.read_sql("""select position_description_id, of.placed_date from position_candidate pc
# join(
# select position_candidate_id, placed_date from offer o
# join offer_personal_info opi on o.id = opi.offer_id) of on pc.id = of.position_candidate_id""", engine_postgre_review)
#
# job_date = pd.read_sql("""select id, head_count_close_date from position_description where head_count_close_date = '2021-09-10 00:00:00.000000'""", engine_postgre_review)
# tem =p_date.loc[p_date['position_description_id'].isin(job_date['id'])]
# tem['head_count_close_date'] = tem['placed_date']
# tem['id'] = tem['position_description_id']
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vjob.ddbconn, ['head_count_close_date', ], ['id', ], 'position_description', mylog)
#
# # %%
# c_job = pd.read_sql("""select JobID as job_externalid from Job where JobStatusID = 5""", engine_mssql.raw_connection())
# c_job['job_externalid'] = c_job['job_externalid'].apply(lambda x: str(x) if x else x)
#
# job_date = pd.read_sql("""select id, head_count_close_date, external_id from position_description where head_count_close_date = '2021-09-10 00:00:00.000000'""", engine_postgre_review)
# tem =job_date.loc[job_date['external_id'].isin(c_job['job_externalid'])]
# tem['head_count_close_date'] = datetime.datetime(2011, 1, 1)
# # tem['id'] = tem['position_description_id']
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vjob.ddbconn, ['head_count_close_date', ], ['id', ], 'position_description', mylog)

# %% name
job_name = pd.read_sql("""select id, name, external_id as job_externalid from position_description where external_id is not null""", engine_postgre_review)
job_name['name'] = job_name['name']+'_'+job_name['job_externalid']
vincere_custom_migration.psycopg2_bulk_update_tracking(job_name, vjob.ddbconn, ['name', ], ['id', ], 'position_description', mylog)

# %% open date
# tem = job[['job_externalid', 'JobReceivedDate']].dropna().drop_duplicates()
# tem['start_date'] = pd.to_datetime(tem['JobReceivedDate'])
# tem2 = tem[['job_externalid', 'start_date']]
# tem2['head_count_open_date'] = tem2['start_date']
# # transform data
# tem2 = tem2.merge(pd.read_sql('select id, external_id as job_externlaid from position_description where',vjob.ddbconn), on=['job_externalid'])
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vjob.ddbconn, ['head_count_open_date', ], ['id', ], 'position_description', logger)

# job_date = pd.read_sql("""select id, external_id as job_externlaid from position_description where head_count_close_date = '2021-12-25 00:00:00.000000'""", engine_postgre_review)
# job_date['head_count_close_date'] = datetime.datetime(2020, 12, 27)
# # tem['id'] = tem['position_description_id']
# vincere_custom_migration.psycopg2_bulk_update_tracking(job_date, vjob.ddbconn, ['head_count_close_date', ], ['id', ], 'position_description', mylog)

# %% industry
jt_mapping = pd.read_csv('job_type.csv')
jt_mapping['matcher'] = jt_mapping['TRIS Employment Type'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
jt = job[['job_externalid', 'jobtype']].dropna()
jt['matcher'] = jt['jobtype'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
jt = jt.merge(jt_mapping, on='matcher', how ='left')
tem = jt[['job_externalid','Industry','Sub-Industry']]
tem = tem.where(tem.notnull(),None)
tem1 = tem[['job_externalid','Industry']].dropna()
tem1['name'] = tem1['Industry']
cp5 = vjob.insert_job_industry_subindustry(tem1, mylog, True)

tem2 = tem[['job_externalid','Sub-Industry']].dropna()
tem2['name'] = tem2['Sub-Industry']
cp5 = vjob.insert_job_industry_subindustry(tem2, mylog, False)