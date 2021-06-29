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
cf.read('yc_config.ini')
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

from common import vincere_job
vjob = vincere_job.Job(connection)

# %%
sql = """
SELECT
j.Id as job_externalid
, j.Name as job_title
, j.ts2__Openings__c as Openings
, j.ts2__Openings__c - ts2__Placement_Count__c as remaining
, j.ts2__Date_Posted__c as Date_Posted
, j.ts2__Date_Filled__c as Date_Closed
, j.Expected_Close_Date__c as Expected_Date_Closed
, j.ts2__Status__c as status
, j.ts2__Job_Function__c as Job_Function
, j.ts2__Closed_Reason__c as closed_reason
, j.ts2__Job_Description__c as job_advertisement
, j.ts2__Max_Pay_Rate__c as max_pay_rate
, j.ts2__Max_Bill_Rate__c as max_bill_rate
, j.ts2__Location__c as location
, j.ts2__Skill_Codes__c as skill_codes
, rt.Name as Job_type
, j.ts2__Post_Job__c as post_job
, j.CreatedDate
, u1.Email as primary_recruiter
, u.Email as owner
, u2.Email as secondary_recruiter
-- Temp
, j.ts2__Estimated_Start_Date__c as estimated_start_date
, j.ts2__Estimated_End_Date__c as estimated_end_date
, j.Rate_Type__c as rate_type
, j.ts2__Per_Diem_Bill_Rate__c as per_diem_bill_rate
, j.ts2__Per_Diem_Pay_Rate__c as per_diem_pay_rate
-- Perm
, j.Retainer__c as retainer
, j.ts2__Min_Salary__c
, j.ts2__Max_Salary__c
, j.ts2__Fee_Pct__c
, j.ts2__Max_Salary__c * j.ts2__Fee_Pct__c / 100 as estimated_fee
, Cast ((julianday('now') - julianday(j.ts2__Date_Posted__c)) as Integer) as Days_open
FROM ts2__Job__c j
LEFT JOIN (
    SELECT
    c.Id as contactid,
    c.AccountId as companyid
    FROM Contact c
    JOIN RecordType r ON (c.RecordTypeId || 'AA2') = r.Id
    WHERE c.IsDeleted = 0 AND r.Name = 'Contact') cont on (j.ts2__Account__c = cont.companyid and j.ts2__Contact__c = cont.contactid)
LEFT JOIN USER u ON j.OwnerId = u.Id
LEFT JOIN USER u1 ON j.ts2__Recruiter__c = u1.Id
LEFT JOIN USER u2 ON j.ts2__Secondary_Recruiter__c = u2.Id
left join RecordType rt on j.RecordTypeId = substr(rt.Id, 1, 15)
WHERE j.IsDeleted = 0
"""
job = pd.read_sql(sql, engine_sqlite)
assert False
# %% currency
job['currency_type'] = 'euro'
vjob.update_currency_type(job, mylog)
# %% open date
od = job[['job_externalid', 'CreatedDate']].dropna()
od['start_date'] = pd.to_datetime(od['CreatedDate'])
cp2 = vjob.update_start_date(od, mylog)

# %% close date
cd = job[['job_externalid', 'Date_Closed']].dropna()
cd['close_date'] = pd.to_datetime(cd['Date_Closed'])
cp3 = vjob.update_close_date(cd, mylog)

# %% public job desc
pubdis = job[['job_externalid', 'job_advertisement']].dropna()
pubdis['public_description'] = pubdis['job_advertisement']
cp4 = vjob.update_public_description2(pubdis, dest_db, mylog)

# %% job type
jt = job[['job_externalid', 'Job_type']].dropna()
jt.loc[jt.Job_type == 'Temp']
jt.loc[jt['Job_type']=='Perm', 'job_type'] = 'permanent'
jt.loc[jt['Job_type']=='Temp', 'job_type'] = 'contract'
jt = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt, mylog)

# %% change job type
jt = job[['job_externalid', 'job_title']].dropna()
jt.loc[jt['job_title'].str.contains("LIM") | jt['job_title'].str.contains("HIM"), 'job_type'] = 'contract'
jt = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt, mylog)

# %% job owner
jo = pd.concat([
job[['job_externalid', 'primary_recruiter']].rename(columns={'primary_recruiter': 'email'}),
job[['job_externalid', 'owner']].rename(columns={'owner': 'email'}),
job[['job_externalid', 'secondary_recruiter']].rename(columns={'secondary_recruiter': 'email'}),
]).dropna().drop_duplicates()

cp6 = vjob.insert_owner(jo, mylog)
# cp6['rn'] = cp6.groupby('id').cumcount()
# cp6.query("rn > 0")

# %% annual salary
actsa = job[['job_externalid', 'ts2__Max_Salary__c']].dropna()
actsa['actual_salary'] = actsa['ts2__Max_Salary__c']
actsa['actual_salary'] = actsa['actual_salary'].astype(float)
cp7 = vjob.update_actual_salary(actsa, mylog)

# %% salary from/to
frsala = job[['job_externalid', 'ts2__Min_Salary__c']].dropna()
frsala['salary_from'] = frsala['ts2__Min_Salary__c']
frsala['salary_from'] = frsala['salary_from'].astype(float)
cp8 = vjob.update_salary_from(frsala, mylog)

tosala = job[['job_externalid', 'ts2__Max_Salary__c']].dropna()
tosala['salary_to'] = tosala['ts2__Max_Salary__c']
tosala['salary_to'] = tosala['salary_to'].astype(float)
cp9 = vjob.update_salary_to(tosala, mylog)

# %% quick fee forcast
qikfeefor = job[['job_externalid', 'ts2__Fee_Pct__c']].dropna()
qikfeefor['use_quick_fee_forecast'] = 1
qikfeefor['percentage_of_annual_salary'] = qikfeefor['ts2__Fee_Pct__c']
qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
cp10 = vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
cp11 = vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% payrate
contract_job_payrate = job[['job_externalid', 'per_diem_pay_rate']].dropna()
contract_job_payrate['pay_rate'] = contract_job_payrate['per_diem_pay_rate']
contract_job_payrate['pay_rate'] = contract_job_payrate['pay_rate'].astype(float)
vjob.update_pay_rate(contract_job_payrate, mylog)

# %% rate_type
contract_job_rate_type = job[['job_externalid', 'rate_type']].dropna()
contract_job_rate_type['rate_type_temp'] = -1

contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Hourly', flags=re.IGNORECASE)), 'rate_type_temp'] = 1
contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Daily', flags=re.IGNORECASE)), 'rate_type_temp'] = 2
contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Weekly', flags=re.IGNORECASE)), 'rate_type_temp'] = 3
contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Monthly', flags=re.IGNORECASE)), 'rate_type_temp'] = 4

contract_job_rate_type['contract_rate_type'] = contract_job_rate_type['rate_type_temp']
vjob.update_contract_rate_type(contract_job_rate_type, mylog)

# %% charge rate
contract_job_chargerate = job[['job_externalid', 'per_diem_bill_rate']].dropna()
contract_job_chargerate['charge_rate'] = contract_job_chargerate['per_diem_bill_rate']
contract_job_chargerate['charge_rate'] = contract_job_chargerate['charge_rate'].astype(float)
cp13 = vjob.update_charge_rate(contract_job_chargerate, mylog)

# %% note
note = job[[
    'job_externalid',
    'Job_Function',
    'retainer',
    'estimated_fee',
    'status',
    'closed_reason',
    'location',
    'post_job']]

prefixes = [
'YC ID',
'Job Type',
'Retainer',
'Estimated Fee',
'Status',
'Close Reason',
'Location',
'Post Job'
]
note = note.where(note.notnull(), None)
note['post_job'] = note['post_job'].apply(lambda x: 'Yes' if int(x) == 1 else 'No')
note['estimated_fee'] = note['estimated_fee'].apply(lambda x: str(x) if x else x)
note['note'] = note.apply(lambda x: '\n '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
vjob.update_note(note, mylog)

# %% reg date
tem = job[['job_externalid', 'CreatedDate']].dropna()
tem['reg_date'] = pd.to_datetime(tem['CreatedDate'])
vjob.update_reg_date(tem, mylog)