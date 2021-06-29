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
cf.read('ak_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_job
vjob = vincere_job.Job(connection)

# %%
sql = """ 
select a.idassignment as job_externalid
     , f.idassignment as type
     , finalfee
     , salaryfrom
     , salaryto
     , estimatedstartdate
     , f.numberofpositions
     , f.contractenddate
     , f.contractstartdate
     , f.hoursperday
     , f.daysperweek
     , f.idunittype
     , f.clientrate
     , f.contractorrate
     , f.ratecomment
     , f.unit
from assignment a
left join (select flex.*, unittype.value as unit from flex left join unittype on flex.idunittype = unittype.idunittype) f on f.idassignment = a.idassignment
"""
job = pd.read_sql(sql, engine_postgre_src)
assert False
# %% currency
job['currency_type'] = 'pound'
vjob.update_currency_type(job, mylog)

# %% head count
hc = job[['job_externalid', 'numberofpositions']].dropna()
hc['head_count'] = hc['numberofpositions']
hc['head_count'] = hc['head_count'].astype(float)
hc['head_count'] = hc['head_count'].astype(int)
cp1 = vjob.update_head_count(hc, mylog)

# %% start date close date
tem = job[['job_externalid', 'estimatedstartdate']].dropna().rename(columns={'estimatedstartdate': 'start_date'})
tem['start_date'] = pd.to_datetime(tem['start_date'])
vjob.update_start_date(tem, mylog)

# tem = job[['job_externalid', 'ts2__Date_Filled__c']].dropna().rename(columns={'ts2__Date_Filled__c': 'close_date'})
# tem['close_date'] = pd.to_datetime(tem['close_date'])
# vjob.update_close_date(tem, mylog)

# %% job description
# tem = job[['job_externalid', 'ts2__Job_Advertisement__c']].rename(columns={'ts2__Job_Advertisement__c': 'public_description'})
# tem = tem.where(tem.notnull(), '')
# tem = tem.loc[tem.public_description != '']
# cp9 = vjob.update_public_description(tem, mylog)
#
# tem = job[['job_externalid', 'ts2__Job_Description__c']].rename(columns={'ts2__Job_Description__c': 'internal_description'})
# tem = tem.where(tem.notnull(), '')
# tem = tem.loc[tem.internal_description != '']
# cp8 = vjob.update_internal_description(tem, mylog)

# %% job type
jt = job[['job_externalid', 'type']]
jt.loc[jt['type'].isnull(), 'job_type'] = 'permanent'
jt.loc[jt['type'].notnull(), 'job_type'] = 'contract'
jt = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt, mylog)

# %% salary from/to
frsala = job[['job_externalid', 'salaryfrom']].dropna()
frsala['salary_from'] = frsala['salaryfrom']
frsala['salary_from'] = frsala['salaryfrom'].astype(float)
cp6 = vjob.update_salary_from(frsala, mylog)

tosala = job[['job_externalid', 'salaryto']].dropna()
tosala['salary_to'] = tosala['salaryto']
tosala['salary_to'] = tosala['salaryto'].astype(float)
cp7 = vjob.update_salary_to(tosala, mylog)

# %% quick fee forcast
qikfeefor = job[['job_externalid', 'Fee_percent__c']].dropna()
qikfeefor['use_quick_fee_forecast'] = 1
qikfeefor['percentage_of_annual_salary'] = qikfeefor['Fee_percent__c']
qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% note
note = pd.read_sql("""
select a.idassignment as job_externalid
     , a.assignmentreference
     , ams.value as status
     , ags.value as sector
     , ast.value as type
     , asst.value as strategy
     , aso.value as origin
     , a.estimatedvalue
     , a.successprobability
     , a.pitchdate
     , a.assignmentcomment
     , a.estimatedfee
     , a.feecomment, a.packagecomment, a.assignmentbrief
from assignment a
left join assignmentstatus ams on a.idassignmentstatus = ams.idassignmentstatus
left join assignmentsector ags on a.idassignmentsector = ags.idassignmentsector
LEFT JOIN assignmenttype ast ON ast.idassignmenttype = a.idassignmenttype
LEFT JOIN assignmentstrategy asst ON asst.idassignmentstrategy = a.idassignmentstrategy
LEFT JOIN assignmentorigin aso ON  aso.idassignmentorigin = a.idassignmentorigin""", engine_postgre_src)

note['assignmentcomment'] = note['assignmentcomment'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['assignmentbrief'] = note['assignmentbrief'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['note'] = note[['assignmentreference'
    , 'status'
    , 'sector', 'type', 'strategy'
    , 'estimatedvalue', 'successprobability', 'origin'
    , 'pitchdate', 'assignmentcomment', 'estimatedfee', 'feecomment'
    , 'packagecomment', 'assignmentbrief']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Reference'
                                                            , 'Status', 'Sector', 'Type', ' Strategy'
                                                            , 'Estimated Value', 'Success Probability (%)', 'Origin', 'Pitch Date'
                                                            , 'Comment', 'Estimated Fee', 'Fee Comment', 'Package Comment', 'Brief'], x) if e[1]]), axis=1)

vjob.update_note(note, mylog)

# %% payrate
contract_job_payrate = job[['job_externalid', 'contractorrate']].dropna()
contract_job_payrate['pay_rate'] = contract_job_payrate['contractorrate']
contract_job_payrate['pay_rate'] = contract_job_payrate['pay_rate'].astype(float)
vjob.update_pay_rate(contract_job_payrate, mylog)

# %% charge rate
contract_job_chargerate = job[['job_externalid', 'clientrate']].dropna()
contract_job_chargerate['charge_rate'] = contract_job_chargerate['clientrate']
contract_job_chargerate['charge_rate'] = contract_job_chargerate['charge_rate'].astype(float)
cp13 = vjob.update_charge_rate(contract_job_chargerate, mylog)

# %% industry
sql = """
select ac.idassignment as job_externalid, i.value as industries
from assignmentcode ac
left join industry i on i.idindustry = ac.codeid
where idtablemd = '6e748cd6-b1cd-4886-bd65-43a5acbb66a1'
"""
job_industries = pd.read_sql(sql, engine_postgre_src)
job_industries = job_industries.dropna()

job_industries['matcher'] = job_industries['industries'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['INDUSTRY'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job_industries = job_industries.merge(industries_csv, on='matcher')

job_industries['name'] = job_industries['INDUSTRY']
job_industries = job_industries.drop_duplicates().dropna()
cp10 = vjob.insert_job_industry(job_industries, mylog)