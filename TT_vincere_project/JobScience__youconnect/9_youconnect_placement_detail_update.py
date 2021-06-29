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
select
 p.Id as placementid
     , ts2__Job__c as job_externalid
     , ts2__Employee__c as candidate_externalid
     , ts2__Start_Date__c
     , ts2__End_Date__c

     -- contract
     , p.ts2extams__Substatus__c
     , p.Rate_Type__c
     , p.Days_Per_Week__c as placement_days_per_weeks
     , j.Days_Per_Week__c
     , j.ts2__Per_Diem_Bill_Rate__c as charge_rate
     , j.ts2__Per_Diem_Pay_Rate__c as pay_rate

     -- perm
     , p.ts2__Status__c
     , p.ts2__Salary__c
     , p.ts2__Fee_Pct__c
     , p.ts2__Fall_Off_Date__c
     , p.ts2__Guarantee__c
     , p.Invoice__c
     , p.Status_PO_Number__c
     , p.Name
     , p.Item__c
     , p.Qty__c
     , p.GL_Account__c
     , p.Class__c
     , p.Terms__c
     , p.Invoice_Sent__c
     , p.ts2__Salary__c * p.ts2__Fee_Pct__c / 100 as amount
     , p.ts2__App_PDate__c
     , j.ts2__CDate__c

     -- general
     , u1.Email as owner
     , p.CreatedDate
     , p.LastModifiedDate
     , u2.FirstName || ' ' || u2.LastName as ModifiedBy
from ts2__Placement__c p
left join ts2__Job__c j on p.ts2__Job__c = j.Id
left join User u1 on u1.Id = p.CreatedById
left join User u2 on u2.Id = p.LastModifiedById
"""
placement_detail_info = pd.read_sql(sql, engine_sqlite)
assert False
# %% start date/end date
stdate = placement_detail_info[['job_externalid', 'candidate_externalid', 'ts2__Start_Date__c', 'ts2__End_Date__c']]
stdate['start_date'] = stdate['ts2__Start_Date__c']
stdate['end_date'] = stdate['ts2__End_Date__c']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
stdate['end_date'] = pd.to_datetime(stdate['end_date'])
cp1 = vplace.update_startdate_enddate(stdate, mylog)

# %% offer date/place date
jobapp_created_date = placement_detail_info[['job_externalid', 'candidate_externalid', 'CreatedDate']]
jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['CreatedDate'])
jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['CreatedDate'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
vplace.update_offerdate(jobapp_created_date, mylog)
vplace.update_placeddate(jobapp_created_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% payrate
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'pay_rate']].dropna()
tem['pay_rate'] = tem['pay_rate'].astype(float)
cp8 = vplace.update_pay_rate(tem, mylog)

# %% charge rate
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'charge_rate']].dropna()
tem['charge_rate'] = tem['charge_rate'].astype(float)
cp1 = vplace.update_charge_rate(tem, mylog)

# %% salary
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'ts2__Salary__c']].dropna().rename(columns={'ts2__Salary__c':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()

tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'ts2__Fee_Pct__c']].dropna()
tem['percentage_of_annual_salary'] = tem['ts2__Fee_Pct__c']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% placement note
note = placement_detail_info[[
    'placementid',
    'job_externalid',
    'candidate_externalid',
    'ts2__Status__c',
    'ts2extams__Substatus__c',
    'Days_per_week__c',
    'ts2__Fall_Off_Date__c',
    'ts2__Guarantee__c',
    'Invoice__c',
    'Status_PO_Number__c',
    'Name',
    'Item__c',
    'Qty__c',
    'GL_Account__c',
    'Class__c',
    'Terms__c',
    'Invoice_Sent__c',
    'amount',
    'ts2__App_PDate__c',
    'ts2__CDate__c',
    'LastModifiedDate',
    'ModifiedBy']]

note['ts2__CDate__c'] = pd.to_datetime(note['ts2__CDate__c'])
note['ts2__App_PDate__c'] = pd.to_datetime(note['ts2__App_PDate__c'])
note['day_count'] = (note['ts2__App_PDate__c'] - note['ts2__CDate__c']).astype('timedelta64[D]')

note = note.where(note.notnull(), None)
note['amount'] = note['amount'].apply(lambda x: str(x) if x else x)
note['Qty__c'] = note['Qty__c'].apply(lambda x: str(x) if x else x)
note['day_count'] = note['day_count'].apply(lambda x: str(x).split('.')[0] if x else x)
note['note'] = note[['Name','ts2__Status__c',
                    'ts2extams__Substatus__c', 'Days_per_week__c', 'ts2__Fall_Off_Date__c', 'day_count',
                    'ts2__Guarantee__c', 'Invoice__c', 'Status_PO_Number__c', 'Item__c', 'Qty__c', 'amount',
                    'GL_Account__c', 'Class__c', 'Terms__c', 'Invoice_Sent__c']] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Placement', 'Status',
                                                                       'Sub Status', 'Days Per Week', 'Fall Off Date', 'Days to Fill',
                                                                       'Guarantee', 'Invoice #', 'Status / PO Number', 'Item #', 'Qty', 'Amount',
                                                                       'GL Account', 'Class', 'Terms', 'Invoice Sent'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)

# %% split
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'owner']].dropna()
tem['user_email'] = tem['owner']
tem['shared'] = 100
tem['shared'] = tem['shared'].astype(float)
vplace.insert_profit_split_mode_percentage(tem, mylog)

