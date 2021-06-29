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
cf.read('ec_config.ini')
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
       jt.JobTypeName as jobtype,
       js.DisplayName as status,
       nullif(convert(varchar,JobLocation),'') as JobLocation,
       nullif(convert(varchar,JobPayRate),'') as JobPayRate,
       nullif(convert(varchar,JobNumberOfPositions),'') as JobNumberOfPositions,
       nullif(convert(varchar,JobChargeRate),'') as JobChargeRate,
       nullif(convert(varchar,JobUpdatedDate),'') as JobUpdatedDate,
       nullif(convert(varchar,JobClosingDate),'') as JobClosingDate,
       nullif(convert(varchar,JobTechnicalNotes),'') as JobTechnicalNotes,
       nullif(convert(varchar,JobRateSalaryComm),'') as JobRateSalaryComm, 
       nullif(convert(varchar,JobMainSkills),'') as JobMainSkills,
       nullif(convert(varchar,JobReceivedDate),'') as JobReceivedDate
from Job j
left join JobStatus js on js.JobStatusID = j.JobStatusID
left join JobType jt on jt.JobTypeID = j.JobTypeID
"""
job = pd.read_sql(sql, engine_mssql)
job['job_externalid'] =job['job_externalid'].apply(lambda x: str(x) if x else x)
assert False

# %% currency
# tem = job[['job_externalid', 'Currency2']].dropna()
# tem['Currency2'].unique()
# tem.loc[(tem['Currency2'] == 'USD'), 'currency_type'] = 'usd'
# tem.loc[(tem['Currency2'] == 'GBP'), 'currency_type'] = 'pound'
# tem.loc[(tem['Currency2'] == 'Euro'), 'currency_type'] = 'euro'
# vjob.update_currency_type(tem, mylog)

# %% country
# int = pd.read_sql("""select ac.idassignment as job_externalid, i.value as international
# from assignmentcode ac
# left join International i on i.idInternational = ac.codeid
# where idtablemd = '94b9bb6a-5f20-41bd-bc1d-59d34b2550ac'""", engine_sqlite)
# int['country_code'] = int['international'].map(vcom.get_country_code)
# vjob.update_country_code(int, mylog)

# # %% internal_description
# tem = job[['job_externalid', 'RebatePeriod','VacancyDetails','RegDate']]
# tem['RegDate'] = tem['RegDate'].astype(str)
# tem['internal_description'] = tem[['RebatePeriod','VacancyDetails','RegDate']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Rebate Period', 'Notes','RegDate'], x) if e[1]]), axis=1)
# tem = tem.loc[tem.internal_description != '']
# tem.loc[tem['RebatePeriod'].notnull()]
# cp8 = vjob.update_internal_description2(tem, dest_db, mylog)
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
jt = job[['job_externalid', 'jobtype']].dropna()
jt['jobtype'].unique()
jt.loc[jt['jobtype']=='Permanent', 'job_type'] = 'permanent'
jt.loc[jt['jobtype']=='Contract', 'job_type'] = 'contract'
jt.loc[jt['jobtype']=='Temporary', 'job_type'] = 'contract'
jt.loc[jt['jobtype']=='Pipeline', 'job_type'] = 'permanent'
jt2 = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt2, mylog)

# # %% start date close date
# tem = job[['job_externalid', 'StartDate']].dropna().rename(columns={'StartDate': 'start_date'})
# tem['start_date'] = pd.to_datetime(tem['start_date'])
# vjob.update_start_date(tem, mylog)
#
tem = job[['job_externalid', 'JobClosingDate']].dropna().rename(columns={'JobClosingDate': 'close_date'})
tem['close_date'] = pd.to_datetime(tem['close_date'])
vjob.update_close_date(tem, mylog)
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
job['note'] = job[['job_externalid','status'
    , 'JobRateSalaryComm', 'JobLocation', 'JobChargeRate', 'JobTechnicalNotes'
    , 'JobMainSkills']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID','Status', 'Rate/Salary Comm', 'Location', 'Charge Rate', 'Job Information'
                                                            , 'Main Skills\Technical Notes'], x) if e[1]]), axis=1)
vjob.update_note(job, mylog)

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


# %% industry
sql = """
select ac.idassignment as job_externalid, ac.codeid as idIndustry
from assignmentcode ac
where idtablemd = '6e748cd6-b1cd-4886-bd65-43a5acbb66a1'
"""
job_industries = pd.read_sql(sql, engine_sqlite)
job_industries = job_industries.dropna()
job_industries['idIndustry'] = job_industries['idIndustry'].str.lower()

industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()

industry_1 = job_industries.merge(industries, on='idIndustry')
industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
j_industries = industry_1.merge(industries_csv, on='matcher')

j_industries_2 = j_industries[['job_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
j_industries_2 = j_industries_2.where(j_industries_2.notnull(),None)

tem1 = j_industries_2[['job_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
cp10 = vjob.insert_job_industry_subindustry(tem1, mylog, True)

tem2 = j_industries_2[['job_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
cp10 = vjob.insert_job_industry_subindustry(tem2, mylog, False)