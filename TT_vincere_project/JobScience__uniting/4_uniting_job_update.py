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
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

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
j.Id as job_externalid,
j.Department__c,
j.Hiring_Consultant__c,
j.Type__c,
j.KN_Job_Posting__c,
j.ts2__Openings__c,
j.Exclusive__c,
j.Retained__c,
j.ts2__Estimated_Start_Date__c,
j.ts2__Date_Filled__c,
j.ts2__Job_Description__c,
j.ts2__Job_Advertisement__c,
j.PO__c,
j.ts2__Min_Salary__c,
j.ts2__Max_Salary__c,
j.ts2__Bonus__c,
j.Fee_percent__c, 
j.ts2__Min_Bill_Rate__c, 
j.ts2__Min_Pay_Rate__c,
cont.companyid AS companyId,
cont.contactid AS contactId
FROM ts2__Job__c j
LEFT JOIN (
    SELECT
    c.Id as contactid,
    c.AccountId as companyid
    FROM Contacts c
    JOIN RecordType r ON (c.RecordTypeId || 'IAQ') = r.Id
    WHERE c.IsDeleted = 0 AND r.Name = 'Contact') cont on (j.ts2__Account__c = cont.companyid and j.ts2__Contact__c = cont.contactid)
LEFT JOIN USER u ON j.OwnerId = u.Id
WHERE j.IsDeleted = 0
and j.LastModifiedDate >= '2017-01-01';
"""
job = pd.read_sql(sql, engine_sqlite)
assert False
# %% currency
job['currency_type'] = 'pound'
vjob.update_currency_type(job, mylog)

# %% head count
hc = job[['job_externalid', 'ts2__Openings__c']].dropna()
hc['head_count'] = hc['ts2__Openings__c']
hc['head_count'] = hc['head_count'].astype(float)
hc['head_count'] = hc['head_count'].astype(int)
cp1 = vjob.update_head_count(hc, mylog)

# %% start date close date
tem = job[['job_externalid', 'ts2__Estimated_Start_Date__c']].dropna().rename(columns={'ts2__Estimated_Start_Date__c': 'start_date'})
tem['start_date'] = pd.to_datetime(tem['start_date'])
vjob.update_start_date(tem, mylog)

tem = job[['job_externalid', 'ts2__Date_Filled__c']].dropna().rename(columns={'ts2__Date_Filled__c': 'close_date'})
tem['close_date'] = pd.to_datetime(tem['close_date'])
vjob.update_close_date(tem, mylog)

# %% job description
tem = job[['job_externalid', 'ts2__Job_Advertisement__c']].rename(columns={'ts2__Job_Advertisement__c': 'public_description'})
tem = tem.where(tem.notnull(), '')
tem = tem.loc[tem.public_description != '']
cp9 = vjob.update_public_description(tem, mylog)

tem = job[['job_externalid', 'ts2__Job_Description__c']].rename(columns={'ts2__Job_Description__c': 'internal_description'})
tem = tem.where(tem.notnull(), '')
tem = tem.loc[tem.internal_description != '']
cp8 = vjob.update_internal_description(tem, mylog)

# %% job type
jt = job[['job_externalid', 'Type__c']].dropna()
jt.loc[jt['Type__c']=='Perm', 'job_type'] = 'permanent'
jt.loc[jt['Type__c']=='Permanent', 'job_type'] = 'permanent'
jt.loc[jt['Type__c']=='Executive', 'job_type'] = 'permanent'
jt.loc[jt['Type__c']=='Contract/Interim', 'job_type'] = 'contract'
jt.loc[jt['Type__c']=='Interim/Contract', 'job_type'] = 'contract'
jt = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt, mylog)

# %% salary from/to
frsala = job[['job_externalid', 'ts2__Min_Salary__c']].dropna()
frsala['salary_from'] = frsala['ts2__Min_Salary__c']
frsala['salary_from'] = frsala['salary_from'].astype(float)
cp6 = vjob.update_salary_from(frsala, mylog)

tosala = job[['job_externalid', 'ts2__Max_Salary__c']].dropna()
tosala['salary_to'] = tosala['ts2__Max_Salary__c']
tosala['salary_to'] = tosala['salary_to'].astype(float)
cp7 = vjob.update_salary_to(tosala, mylog)

# %% quick fee forcast
qikfeefor = job[['job_externalid', 'Fee_percent__c']].dropna()
qikfeefor['use_quick_fee_forecast'] = 1
qikfeefor['percentage_of_annual_salary'] = qikfeefor['Fee_percent__c']
qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% note
job.loc[job['KN_Job_Posting__c']=='0', 'KN_Job_Posting__c'] = 'No'
job.loc[job['KN_Job_Posting__c']=='1', 'KN_Job_Posting__c'] = 'Yes'
note = job[[
    'job_externalid',
    'Department__c',
    'KN_Job_Posting__c',
    'Exclusive__c',
    'Retained__c',
    'ts2__Bonus__c']]

prefixes = [
'UA ID',
'Department',
'Posting',
'Exclusive',
'Retained',
'Bonus'
]
note['note'] = note.apply(lambda x: '\n '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
vjob.update_note(note, mylog)

# %% payrate
contract_job_payrate = job[['job_externalid', 'ts2__Min_Pay_Rate__c']].dropna()
contract_job_payrate['pay_rate'] = contract_job_payrate['ts2__Min_Pay_Rate__c']
contract_job_payrate['pay_rate'] = contract_job_payrate['pay_rate'].astype(float)
vjob.update_pay_rate(contract_job_payrate, mylog)

# %% charge rate
contract_job_chargerate = job[['job_externalid', 'ts2__Min_Bill_Rate__c']].dropna()
contract_job_chargerate['charge_rate'] = contract_job_chargerate['ts2__Min_Bill_Rate__c']
contract_job_chargerate['charge_rate'] = contract_job_chargerate['charge_rate'].astype(float)
contract_job_chargerate.loc[contract_job_chargerate['job_externalid'] == 'a0xD0000009OrnAIAS']
cp13 = vjob.update_charge_rate(contract_job_chargerate, mylog)
