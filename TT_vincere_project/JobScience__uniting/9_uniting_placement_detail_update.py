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

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(connection)

# %% placement detail
placement_detail_info = pd.read_sql("""
select 
 p.Id as placementid
     , ts2__Job__c as job_externalid
     , ts2__Employee__c as candidate_externalid
     , ts2__Start_Date__c
     , p.ts2__End_Date__c
     , p.Name, ts2__Related_Application__c
     -- contract
     , Eden_Candidate_Id__c
     , Contractor_Rate__c
     , Contractor_Margin__c
     , Length_of_Contract_Months__c
     , Payment_Terms__c
     , ts2__Status__c
     , Proof_of_ID__c
     -- perm
     , ts2__Salary__c
     , Fee_Percentage__c
     , ts2__Filled_Pct__c
     , ts2__Filled_Pct_2__c
     , p.CreatedDate
     , u1.Email as primary_recruiter
     , u2.Email as secondary_recruiter
from ts2__Placement__c p
LEFT JOIN USER u1 ON p.ts2__Filled_By__c = u1.Id
LEFT JOIN USER u2 ON p.ts2__Filled_By_2__c = u2.Id
""", engine_sqlite)
# placement_detail_info.loc[placement_detail_info['Name'] == 'PLC-111318-15959']
assert False
# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'ts2__Salary__c']].dropna()
tem['annual_salary'] = tem['ts2__Salary__c']
tem['annual_salary'] = tem['annual_salary'].astype(float)
vplace.update_offer_annual_salary(tem, mylog)

tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'Fee_Percentage__c']].dropna()
tem['percentage_of_annual_salary'] = tem['Fee_Percentage__c']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
# tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% payrate
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'Contractor_Rate__c']].dropna()
tem['pay_rate'] = tem['Contractor_Rate__c']
tem['pay_rate'] = tem['pay_rate'].astype(float)
cp8 = vplace.update_pay_rate(tem, mylog)

# %% charge rate
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'Contractor_Rate__c', 'Contractor_Margin__c']].dropna()
tem['Contractor_Rate__c'] = tem['Contractor_Rate__c'].astype(float)
tem['Contractor_Margin__c'] = tem['Contractor_Margin__c'].astype(float)
tem['charge_rate'] = tem['Contractor_Rate__c'] + tem['Contractor_Margin__c']
tem['charge_rate'] = tem['charge_rate'].astype(float)
cp1 = vplace.update_charge_rate(tem, mylog)

# %% profit
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'Contractor_Margin__c']].dropna()
tem['profit'] = tem['Contractor_Margin__c']
tem['profit'] = tem['profit'].astype(float)
cp2 = vplace.update_profit(tem, mylog)

# %% margin
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'Contractor_Rate__c', 'Contractor_Margin__c']].dropna()
tem['Contractor_Rate__c'] = tem['Contractor_Rate__c'].astype(float)
tem['Contractor_Margin__c'] = tem['Contractor_Margin__c'].astype(float)
tem['margin_percent'] = round(tem['Contractor_Margin__c']/tem['Contractor_Rate__c']*100, 2)
cp3 = vplace.update_margin_percent(tem, mylog)

# %% markup
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'Contractor_Rate__c', 'Contractor_Margin__c']].dropna()
tem['Contractor_Rate__c'] = tem['Contractor_Rate__c'].astype(float)
tem['Contractor_Margin__c'] = tem['Contractor_Margin__c'].astype(float)
tem['markup_percent'] = round(tem['Contractor_Margin__c']/(tem['Contractor_Rate__c'] + tem['Contractor_Margin__c'])*100, 2)
cp4 = vplace.update_markup_percent(tem, mylog)

# %% contract length
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'Length_of_Contract_Months__c']].dropna()
tem['contract_length'] = tem['Length_of_Contract_Months__c']
tem['contract_length'] = tem['contract_length'].astype(float)
cp7 = vplace.update_contract_length(tem, mylog)
vplace.update_contract_length_type(tem, 'month', mylog)

# %% contract type
tem = placement_detail_info[['job_externalid', 'candidate_externalid']].dropna()
tem['pay_interval'] = 'daily'
tem = tem.loc[tem.pay_interval.str.strip()!= '']
cp10 = vplace.update_pay_interval(tem, mylog)

# %% placement note
# note = placement_detail_info[[
#     'placementid',
#     'job_externalid',
#     'candidate_externalid',
#     'Eden_Candidate_Id__c',
#     'Payment_Terms__c',
#     'ts2__Status__c',
#     'Proof_of_ID__c']]
#
# prefixes = [
# 'UA Placement Id',
# 'UA Job Id',
# 'UA Candidate Id',
# 'Eden Candidate Id',
# 'Payment Terms',
# 'Status',
# 'Proof of ID'
# ]
# note['note'] = note.apply(lambda x: '\n '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
# cp0 = vplace.update_internal_note(note, mylog)


# %% placement note
note = placement_detail_info[[
    'job_externalid',
    'candidate_externalid',
    'Name',
    'Eden_Candidate_Id__c',
    'Payment_Terms__c',
    'ts2__Status__c',
    'Proof_of_ID__c']]

note = note.where(note.notnull(), None)
note['note'] = note[['Name','Eden_Candidate_Id__c',
                    'Payment_Terms__c', 'ts2__Status__c', 'Proof_of_ID__c']] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['UA Placement', 'Eden Candidate Id',
                                                                       'Payment Terms', 'Status', 'Proof of ID'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)

# %% split
tem1 = placement_detail_info[['job_externalid', 'candidate_externalid', 'primary_recruiter', 'ts2__Filled_Pct__c']].dropna()
tem1['user_email'] = tem1['primary_recruiter']
tem1['shared'] = tem1['ts2__Filled_Pct__c']
tem1.drop(['primary_recruiter', 'ts2__Filled_Pct__c'], axis=1, inplace=True)

tem2 = placement_detail_info[['job_externalid', 'candidate_externalid', 'secondary_recruiter', 'ts2__Filled_Pct_2__c']].dropna()
tem2['user_email'] = tem2['secondary_recruiter']
tem2['shared'] = tem2['ts2__Filled_Pct_2__c']
tem2.drop(['secondary_recruiter', 'ts2__Filled_Pct_2__c'], axis=1, inplace=True)

tem = pd.concat([tem1, tem2])
tem = tem.drop_duplicates()
vplace.insert_profit_split_mode_percentage(tem, mylog, override=True)

# %% start date
# tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'ts2__Start_Date__c']].dropna().rename(columns={'ts2__Start_Date__c':'start_date'})
# tem.start_date = pd.to_datetime(tem.start_date)
# cp2 = vplace.update_startdate_only_for_placement_detail(tem, mylog)

tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'ts2__Start_Date__c', 'ts2__End_Date__c']]\
    .rename(columns={'ts2__Start_Date__c':'start_date', 'ts2__End_Date__c':'end_date'})
tem.start_date = pd.to_datetime(tem.start_date)
tem.end_date = pd.to_datetime(tem.end_date)
cp2 = vplace.update_startdate_enddate(tem, mylog)

# %% place date
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'CreatedDate']].dropna()
tem['placed_date'] = tem['CreatedDate']
tem['placed_date'] = pd.to_datetime(tem['placed_date'])
cp3 = vplace.update_placeddate(tem, mylog)

# %% offer date
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'CreatedDate']].dropna()
tem['offer_date'] = tem['CreatedDate']
tem['offer_date'] = pd.to_datetime(tem['offer_date'])
cp3 = vplace.update_offerdate(tem, mylog)

# %% sent date
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'CreatedDate']].dropna()
tem['sent_date'] = tem['CreatedDate']
tem['sent_date'] = pd.to_datetime(tem['sent_date'])
cp3 = vplace.update_sent_date(tem, mylog)

# %% job type
# tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'employmentType']].dropna()
# tem.replace({'Permanent': 'permanent', 'Contract': 'contract', 'Fixed Term': 'contract', 'Temporary': 'contract', 'Opportunity': 'contract'}, inplace=True)
# tem.employmentType.unique()
# tem['placement_type'] = tem.employmentType
# cp4 = vplace.update_placementtype_or_jobtype(tem, mylog)