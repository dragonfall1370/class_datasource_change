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
cf.read('en_config.ini')
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
select JobNumber as job_externalid
     , nullif(VacancyType,'') as VacancyType
     , nullif(DisplayName,'') as DisplayName
     , nullif(convert(varchar,StartDate),'') as StartDate
     , nullif(convert(varchar,EndDate),'') as EndDate
     , nullif(FullPart,'') as FullPart
     , nullif(Fee,'') as Fee
     , nullif(Currency1,'') as Currency1
     , nullif(RebatePeriod,'') as RebatePeriod
     , nullif(Salary,'') as Salary
     , nullif(Salary1,'') as Salary1
     , nullif(KeySkills,'') as KeySkills
     , nullif(VacancyDetails,'') as VacancyDetails
     , nullif(Postcode,'') as Postcode
     , nullif(Location,'') as Location
     , nullif(SubLocation,'') as SubLocation
     , nullif(Benefits,'') as Benefits
     , nullif(Currency2,'') as Currency2
     , nullif(RegDate,'') as RegDate
from Vacancies
"""
job = pd.read_sql(sql, engine_mssql)
job['job_externalid'] = 'EUK'+job['job_externalid']
assert False
# %%
vjob.set_job_location_by_contact_location(mylog)

# %% currency
tem = job[['job_externalid', 'Currency1']].dropna()
tem['Currency1'].unique()
tem.loc[(tem['Currency1'] == 'USD'), 'currency_type'] = 'usd'
tem.loc[(tem['Currency1'] == 'GBP'), 'currency_type'] = 'pound'
tem.loc[(tem['Currency1'] == 'Euro'), 'currency_type'] = 'euro'
tem2 = tem[['job_externalid', 'currency_type']].dropna()
vjob.update_currency_type(tem2, mylog)

# %% country
# int = pd.read_sql("""select ac.idassignment as job_externalid, i.value as international
# from assignmentcode ac
# left join International i on i.idInternational = ac.codeid
# where idtablemd = '94b9bb6a-5f20-41bd-bc1d-59d34b2550ac'""", engine_sqlite)
# int['country_code'] = int['international'].map(vcom.get_country_code)
# vjob.update_country_code(int, mylog)

# %% key words
tem = job[['job_externalid', 'KeySkills']].dropna().drop_duplicates()
tem['key_words'] = tem['KeySkills']
vjob.update_key_words(tem, mylog)

# %% internal_description
tem = job[['job_externalid', 'RebatePeriod','VacancyDetails','RegDate']]
tem['RegDate'] = tem['RegDate'].astype(str)
tem['internal_description'] = tem[['RebatePeriod','VacancyDetails','RegDate']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Rebate Period', 'Notes','RegDate'], x) if e[1]]), axis=1)
tem = tem.loc[tem.internal_description != '']
tem.loc[tem['RebatePeriod'].notnull()]
cp8 = vjob.update_internal_description2(tem, dest_db, mylog)

# %% rate_type
employment_type = job[['job_externalid', 'FullPart']].dropna()
employment_type['FullPart'].unique()
employment_type.loc[(employment_type['FullPart'] == 'Full-time'), 'employment_type'] = 0
employment_type.loc[(employment_type['FullPart'] == 'Part-time'), 'employment_type'] = 1
tem = employment_type[['job_externalid', 'employment_type']].dropna()
employment_type.loc[employment_type['employment_type'] == 1]
vjob.update_employment_type(tem, mylog)

# %% head count
# hc = job[['job_externalid', 'ts2__Openings__c']].dropna()
# hc['head_count'] = hc['ts2__Openings__c']
# hc['head_count'] = hc['head_count'].astype(float)
# hc['head_count'] = hc['head_count'].astype(int)
# cp1 = vjob.update_head_count(hc, mylog)

# %% job type
jt = job[['job_externalid', 'VacancyType']].dropna()
jt['VacancyType'].unique()
jt.loc[jt['VacancyType']=='Permanent', 'job_type'] = 'permanent'
jt.loc[jt['VacancyType']=='Contract', 'job_type'] = 'contract'
jt = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt, mylog)

# %% start date close date
# tem = job[['job_externalid', 'StartDate']].dropna().rename(columns={'StartDate': 'start_date'})
# tem['start_date'] = pd.to_datetime(tem['start_date'])
# vjob.update_start_date(tem, mylog)
#
# tem = job[['job_externalid', 'EndDate']].dropna().rename(columns={'EndDate': 'close_date'})
# tem['close_date'] = pd.to_datetime(tem['close_date'])
# vjob.update_close_date(tem, mylog)

# %% salary from/to
frsala = job[['job_externalid', 'Salary']].dropna()
frsala['salary_from'] = frsala['Salary']
frsala['salary_from'] = frsala['salary_from'].astype(float)
cp6 = vjob.update_salary_from(frsala, mylog)

tosala = job[['job_externalid', 'Salary1']].dropna()
tosala['salary_to'] = tosala['Salary1']
tosala['salary_to'] = tosala['salary_to'].astype(float)
cp7 = vjob.update_salary_to(tosala, mylog)

# %% annual salary
sal = job[['job_externalid', 'Salary']].dropna()
sal['actual_salary'] = sal['Salary']
sal['actual_salary'] = sal['actual_salary'].astype(float)
vjob.update_actual_salary(sal, mylog)

# %% quick fee forcast
# qikfeefor = job[['job_externalid', 'Fee']].dropna()
#
# qikfeefor['use_quick_fee_forecast'] = 1
# qikfeefor['percentage_of_annual_salary'] = qikfeefor['EstimatedFee']
# qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
# vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
# vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% reg date
# tem = job[['job_externalid', 'CreatedOn']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['CreatedOn'])
# vjob.update_reg_date(tem, mylog)

# %% start date
tem = job[['job_externalid', 'RegDate']].dropna().drop_duplicates()
tem['start_date'] = pd.to_datetime(tem['RegDate'])
vjob.update_start_date(tem, mylog)

# %% note
job['note'] = job[['DisplayName','Postcode'
    , 'Location', 'SubLocation', 'Benefits', 'Fee','StartDate','EndDate'
    , 'Currency2']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Contact','Postcode', 'Location', 'Sub Location', ' Benefits', 'Fee','Start Date','End Date'
                                                            , 'Currency'], x) if e[1]]), axis=1)
vjob.update_note2(job, dest_db, mylog)

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
sql ="""select skc.* from
(select sc.ObjectId, s.Sector from SectorInstances sc
left join Sectors s on s.SectorId = sc.SectorId) skc
join Vacancies j on j.JobNumber = skc.ObjectId
where skc.Sector is not null"""
industry = pd.read_sql(sql, engine_mssql)
industry['job_externalid'] = 'EUK'+industry['ObjectId']
industry['name'] = industry['Sector']
cp10 = vjob.insert_job_industry_subindustry(tem2, mylog, False)

# %%
job_location = pd.read_sql("""select pd.id as position_id, country_code from position_description pd
join company_location cl on pd.company_location_id = cl.id
where nullif(country_code,'') is not null""", vjob.ddbconn)

compen = pd.read_sql("""select position_id, country_code from compensation where country_code is null""", vjob.ddbconn)

tem = job_location.loc[job_location['position_id'].isin(compen['position_id'])]
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vjob.ddbconn, ['country_code', ], ['position_id', ], 'compensation', mylog)

