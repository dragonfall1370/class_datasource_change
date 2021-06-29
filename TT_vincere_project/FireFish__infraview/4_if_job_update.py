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
cf.read('if_config.ini')
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
select j.ID as job_externalid
     , jt.Description as job_type
     , j.Title
     , js.Description as job_status
     , j.StartDate
     , j.EndDate
     , j.PositionsAvailable
     , j.IsPlaced
     , j.Keywords
     , j.PermFrom as min_sal
     , j.PermTo as max_sal
     , j.AdditionalRemuneration
     , j.AdditionalAmount
     , j.NetHours
     , j.PermProposedSalary
     , j.ProposedSalesPercent, jt.Value as saljobtype, UpdatedDate, CreatedDate
from Job j
left join JobType jt on jt.JobTypeID = j.JobTypeID
left join JobStatus js on js.JobStatusID = j.JobStatusID
left join Drop_Down___SalaryBasedJobType jt on jt.ID = j.SalaryBasedJobTypeWSIID
"""
job = pd.read_sql(sql, engine_sqlite)
job['job_externalid'] = job['job_externalid'].apply(lambda x: str(x) if x else x)
assert False
j_prod = pd.read_sql("""select id, name from position_description""", engine_postgre)
j_prod['name'] = j_prod['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, vjob.ddbconn, ['name'], ['id'], 'position_description', mylog)
# %% currency
job['currency_type'] = 'pound'
vjob.update_currency_type(job, mylog)

# %% head count
hc = job[['job_externalid', 'PositionsAvailable']].dropna()
hc.loc[hc['PositionsAvailable']=='0', 'PositionsAvailable'] = '1'
hc['head_count'] = hc['PositionsAvailable'].astype(int)
vjob.update_head_count(hc, mylog)

# %% open date
od = job[['job_externalid', 'StartDate']].dropna()
od['start_date'] = pd.to_datetime(od['StartDate'])
cp2 = vjob.update_start_date(od, mylog)

# %% projected placement date
od = job[['job_externalid', 'StartDate']].dropna()
od['projected_placement_date'] = pd.to_datetime(od['StartDate'])
cp2 = vjob.update_projected_placement_date(od, mylog)

# %% close date
cd = job[['job_externalid', 'EndDate']].dropna()
cd['close_date'] = pd.to_datetime(cd['EndDate'])
cp3 = vjob.update_close_date(cd, mylog)

# %% public job desc
pubdis = job[['job_externalid', 'AdditionalAmount','PermProposedSalary']].dropna()
pubdis['internal_description'] = pubdis['AdditionalAmount'].astype(float)+pubdis['PermProposedSalary'].astype(float)
pubdis['internal_description'] = 'Total Remuneration: ' + pubdis['internal_description'].astype(str)
cp4 = vjob.update_internal_description(pubdis, mylog)

# %% job type
jt = job[['job_externalid', 'job_type']].dropna()
jt['job_type'].unique()
jt.loc[jt['job_type']=='Permanent', 'job_type'] = 'permanent'
jt.loc[jt['job_type']=='Contract', 'job_type'] = 'contract'
jt = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt, mylog)

# %% annual salary
actsa = job[['job_externalid', 'PermProposedSalary']].dropna()
actsa['actual_salary'] = actsa['PermProposedSalary']
actsa['actual_salary'] = actsa['actual_salary'].astype(float)
cp7 = vjob.update_actual_salary(actsa, mylog)

# %% salary from/to
frsala = job[['job_externalid', 'min_sal']].dropna()
frsala['salary_from'] = frsala['min_sal']
frsala['salary_from'] = frsala['salary_from'].astype(float)
cp8 = vjob.update_salary_from(frsala, mylog)
#
tosala = job[['job_externalid', 'max_sal']].dropna()
tosala['salary_to'] = tosala['max_sal']
tosala['salary_to'] = tosala['salary_to'].astype(float)
cp9 = vjob.update_salary_to(tosala, mylog)

# %% quick fee forcast
qikfeefor = job[['job_externalid', 'ProposedSalesPercent']].dropna()
qikfeefor['use_quick_fee_forecast'] = 1
qikfeefor['percentage_of_annual_salary'] = qikfeefor['ProposedSalesPercent']
qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
cp10 = vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
cp11 = vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% update key words
tem = job[['job_externalid', 'Keywords']].dropna()
tem['key_words'] = tem['Keywords']
vjob.update_key_words(tem, mylog)

# %% last activity date
tem = job[['job_externalid', 'UpdatedDate']].dropna()
tem['last_activity_date'] = pd.to_datetime(tem['UpdatedDate'])
vjob.update_last_activity_date(tem, mylog)

# %% reg date
tem = job[['job_externalid', 'CreatedDate']].dropna()
tem['reg_date'] = pd.to_datetime(tem['CreatedDate'])
vjob.update_reg_date(tem, mylog)

# # %% payrate
# contract_job_payrate = job[['job_externalid', 'per_diem_pay_rate']].dropna()
# contract_job_payrate['pay_rate'] = contract_job_payrate['per_diem_pay_rate']
# contract_job_payrate['pay_rate'] = contract_job_payrate['pay_rate'].astype(float)
# vjob.update_pay_rate(contract_job_payrate, mylog)
#
# # %% rate_type
# contract_job_rate_type = job[['job_externalid', 'rate_type']].dropna()
# contract_job_rate_type['rate_type_temp'] = -1
#
# contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Hourly', flags=re.IGNORECASE)), 'rate_type_temp'] = 1
# contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Daily', flags=re.IGNORECASE)), 'rate_type_temp'] = 2
# contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Weekly', flags=re.IGNORECASE)), 'rate_type_temp'] = 3
# contract_job_rate_type.loc[(contract_job_rate_type['rate_type_temp'] == -1) & (contract_job_rate_type['rate_type'].str.contains('Monthly', flags=re.IGNORECASE)), 'rate_type_temp'] = 4
#
# contract_job_rate_type['contract_rate_type'] = contract_job_rate_type['rate_type_temp']
# vjob.update_contract_rate_type(contract_job_rate_type, mylog)

# # %% charge rate
# contract_job_chargerate = job[['job_externalid', 'per_diem_bill_rate']].dropna()
# contract_job_chargerate['charge_rate'] = contract_job_chargerate['per_diem_bill_rate']
# contract_job_chargerate['charge_rate'] = contract_job_chargerate['charge_rate'].astype(float)
# cp13 = vjob.update_charge_rate(contract_job_chargerate, mylog)

# %% note
note = job[[
    'job_externalid',
    'AdditionalRemuneration',
    'AdditionalAmount',
    'saljobtype','PermProposedSalary']]
note['TotalRemuneration'] = note['AdditionalAmount'].astype(float) + note['PermProposedSalary'].astype(float)
note['TotalRemuneration'] = note['TotalRemuneration'].astype(str)
note['note'] = note[['job_externalid', 'AdditionalRemuneration', 'AdditionalAmount', 'saljobtype', 'TotalRemuneration']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Infraview ID', 'Additional Remuneration', 'Additional Amount', 'Job Type', 'Total Remuneration'], x) if e[1]]), axis=1)
vjob.update_note(note, mylog)

# %%
sql = """
select CandidateID as candidate_externalid
     , JobID as job_externalid
     , Date
from ContractPlacement
"""
placement_detail_info_perm = pd.read_sql(sql, engine_sqlite)
placement_detail_info_perm['close_date'] = placement_detail_info_perm['Date']
placement_detail_info_perm['close_date'] = pd.to_datetime(placement_detail_info_perm['close_date'])
vjob.update_close_date(placement_detail_info_perm, mylog)
