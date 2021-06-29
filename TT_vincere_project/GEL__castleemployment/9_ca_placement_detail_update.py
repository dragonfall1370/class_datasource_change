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
cf.read('ca_config.ini')
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
with min as (select Placement_Id, min(Rate_Id) as Rate_Id from Placement_Rate group by Placement_Id)
select p.Placement_Id
     , Booking_Role_Id
     , Candidate_Id
     , Placement_Code
     , Booking_Ref
     , Rate_Group
     , rate_type
     , prr.Description as rate_des
     , Pay_Rate
     , Charge_Rate
     , Hours_Per_Unit
     , p.Notes
     , d.Description as division
     , Email_Address as owner
from Placement p
left join (
select prate.*, rt.Description as rate_type
from Placement_Rate prate
join min m on m.Rate_Id = prate.Rate_Id
left join Rate_Type rt on rt.Type_Id = prate.Type_Id) prr on p.Placement_Id = prr.Placement_Id
left join Division d on d.Division_Id = p.Division_Id
left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = p.Consultant_Id
where Booking_Role_Id is not null
and Placement_Code not in ('PL41909'
,'PL41910'
,'PL44078'
,'PL43810'
,'PL44075'
,'PL 43186'
,'PL 42798'
,'PL41961'
,'PL44292'
,'PL41958'
,'PL42798'
,'PL42816'
,'PL42826'
,'PL42960'
,'PL43049'
,'PL43343'
,'PL43100'
,'PL43186'
,'PL43641'
)
""", engine_mssql)
placed_info['job_externalid'] = 'BK'+placed_info['Booking_Role_Id'].astype(str)
placed_info['candidate_externalid'] = placed_info['Candidate_Id'].astype(str)
placed_info['Placement_Id'] = placed_info['Placement_Id'].astype(str)

date = pd.read_sql(""" 
select Placement_Id, max(Placement_DT) as end_date, min(Placement_DT) as start_date
from Placement_Day
group by Placement_Id
""", engine_mssql)
date['Placement_Id'] = date['Placement_Id'].astype(str)
date['end_date'] = date['end_date'].apply(lambda x: x[0:10] if x else x)
date['start_date'] = date['start_date'].apply(lambda x: x[0:10] if x else x)
date = date.drop_duplicates()

# sche = pd.read_sql("""
# select Placement_Id, Placement_DT, Start_TM, End_TM, Breaks
# from Placement_Day
# """, engine_mssql)
# date['Placement_Id'] = date['Placement_Id'].astype(str)
# sche['Placement_DT'] = sche['Placement_DT'].apply(lambda x: x[0:10] if x else x)
# sche['Start_TM'] = sche['Start_TM'].apply(lambda x: x[11:16] if x else x)
# sche['End_TM'] = sche['End_TM'].apply(lambda x: x[11:16] if x else x)
# sche['Breaks'] = sche['Breaks'].apply(lambda x: x[11:16] if x else x)
# sche['sche'] = sche[['Placement_DT','Start_TM','End_TM','Breaks']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Date','Start','End','Breaks'], x) if e[1]]), axis=1)
# sche = sche.groupby('Placement_Id')['sche'].apply(lambda x: '\n\n'.join(x)).reset_index()
# sche['sche'] = '\n---Daily Schedule---\n'+sche['sche']

placed_info = placed_info.merge(date,on='Placement_Id', how='left')
# placed_info = placed_info.merge(sche,on='Placement_Id', how='left')
placed_info = placed_info.where(placed_info.notnull(), None)
assert False
# %% placement note
note = placed_info[['candidate_externalid','job_externalid',
    'Rate_Group'
    ,'rate_type'
    ,'rate_des'
    ,'Hours_Per_Unit'
    ,'division','Notes'
    ]]

note['rate_type'].unique()
note.loc[(note['rate_type'] == 'Hourly Rate'), 'WTR'] = 'Yes'
note.loc[(note['rate_type'] == 'Hourly Rate'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'Overtime Rate'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'Overtime Rate'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'Bonus Payment'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'Bonus Payment'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'Holiday Pay'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'Holiday Pay'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'Charge Rate'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'Charge Rate'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'ZZ  Pay'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'ZZ  Pay'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'ZZhourly Rate'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'ZZhourly Rate'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'NO WTR Rate'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'NO WTR Rate'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'ZzSupplier Hourly Rate'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'ZzSupplier Hourly Rate'), 'WTR2'] = 'No'
note.loc[(note['rate_type'] == 'ZzSupplier Daily Rate'), 'WTR'] = 'No'
note.loc[(note['rate_type'] == 'ZzSupplier Daily Rate'), 'WTR2'] = 'No'
note = note.where(note.notnull(), None)
note['Hours_Per_Unit'] = note['Hours_Per_Unit'].astype(str)
note['note'] = note[[
    'Rate_Group'
    ,'rate_type'
    ,'rate_des'
    ,'Hours_Per_Unit','Notes','WTR','WTR2','division']] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip([
    'Rate Group'
    ,'Rate Type'
    ,'Rate Description'
    ,'Hours Per Unit','Notes','WTR','WTR 2','Division'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)

# %% start date/end date
stdate = placed_info[['job_externalid', 'candidate_externalid', 'start_date', 'end_date']].dropna()
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
stdate['end_date'] = pd.to_datetime(stdate['end_date'])
cp1 = vplace.update_startdate_enddate(stdate, mylog)

# %% offer date/place date
# jobapp_created_date = placement_detail_info_contract[['job_externalid', 'candidate_externalid', 'Date']].dropna()
# jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['Date'])
# jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['Date'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
# vplace.update_offerdate(jobapp_created_date, mylog)
# vplace.update_placeddate(jobapp_created_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% payrate
tem = placed_info[['job_externalid', 'candidate_externalid', 'Pay_Rate']].dropna()
tem['pay_rate'] = tem['Pay_Rate'].astype(float)
cp8 = vplace.update_pay_rate(tem, mylog)

# %% charge rate
tem = placed_info[['job_externalid', 'candidate_externalid', 'Charge_Rate']].dropna()
tem['charge_rate'] = tem['Charge_Rate'].astype(float)
cp1 = vplace.update_charge_rate(tem, mylog)

# %% profit
tem = placed_info[['job_externalid', 'candidate_externalid', 'Charge_Rate', 'Pay_Rate']].dropna()
tem['profit'] = tem['Charge_Rate'].astype(float) - tem['Pay_Rate'].astype(float)
tem['profit'] = tem['profit'].astype(float)
cp1 = vplace.update_profit(tem, mylog)

# %% split
user = pd.read_csv('user.csv')
user['matcher'] = user['Email in Gel'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem = placed_info[['job_externalid', 'candidate_externalid', 'owner']].dropna()
tem['matcher'] = tem['owner'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem = tem.merge(user, on='matcher')
tem['user_email'] = tem['Email for Vincere login']
tem['shared']=100
vplace.insert_profit_split_mode_percentage(tem, mylog)

