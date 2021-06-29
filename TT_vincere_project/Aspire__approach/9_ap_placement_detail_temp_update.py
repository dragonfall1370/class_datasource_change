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
cf.read('ap_config.ini')
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
assert False
# %% permanent
contract_info = pd.read_sql("""
with temp as (select
       concat('', Person_Reference) as candidate_externalid
     , concat('', Agreement_Reference) as job_externalid
     , Starts_At
     , Ends_At
     , Pay_Rate
     , Charge_Rate
     , ROW_NUMBER() OVER(PARTITION BY concat(Person_Reference,Agreement_Reference) ORDER BY Ends_At DESC) rn
from Timesheet_Header_View thv
join Timesheet_Element_With_Units te on thv.Timesheet_Header_Reference = te.Timesheet_Header_Reference)
select * from temp where rn = 1
""", engine_mssql)
# assert False

# %% start date/end date
stdate = contract_info[['job_externalid', 'candidate_externalid', 'Starts_At','Ends_At']]
stdate['start_date'] = stdate['Starts_At']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
stdate['end_date'] = stdate['Ends_At']
stdate['end_date'] = pd.to_datetime(stdate['end_date'])
cp1 = vplace.update_startdate_enddate(stdate, mylog)

# %% offer date/place date
# jobapp_offer_date = placement_detail_info[['job_externalid', 'candidate_externalid', 'OfferAgreedDate']]
# jobapp_offer_date['offer_date'] = pd.to_datetime(jobapp_offer_date['OfferAgreedDate'])
# jobapp_placed_date = contract_info[['job_externalid', 'candidate_externalid', 'ContractDate']].dropna()
# jobapp_placed_date['placed_date'] = pd.to_datetime(jobapp_placed_date['ContractDate'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
# vplace.update_offerdate(jobapp_offer_date, mylog)
# vplace.update_placeddate(jobapp_placed_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% salary
tem = contract_info[['job_externalid', 'candidate_externalid', 'Pay_Rate']].dropna().rename(columns={'Pay_Rate':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% charge rate
tem = contract_info[['job_externalid', 'candidate_externalid', 'Charge_Rate']].dropna().rename(columns={'Charge_Rate':'charge_rate'})
tem['charge_rate'] = tem['charge_rate'].astype(float)
cp6 = vplace.update_charge_rate(tem, mylog)

# %% interval
tem = contract_info[['job_externalid', 'candidate_externalid']].dropna()
tem['pay_interval'] = 'hourly'
cp6 = vplace.update_pay_interval(tem, mylog)

# %% split
# tem1 = contract_info[['job_externalid', 'candidate_externalid', 'onwer1','AM1percent']].dropna()
# tem1['user_email'] = tem1['onwer1']
# tem1['shared'] = tem1['AM1percent']
# tem1['shared'] = tem1['shared'].astype(float)
# tem1.drop(['onwer1', 'AM1percent'], axis=1, inplace=True)
# tem2 = contract_info[['job_externalid', 'candidate_externalid', 'onwer2','AM2percent']].dropna()
# tem2['user_email'] = tem2['onwer2']
# tem2['shared'] = tem2['AM2percent']
# tem2['shared'] = tem2['shared'].astype(float)
# tem2.drop(['onwer2', 'AM2percent'], axis=1, inplace=True)
# tem = pd.concat([tem1,tem2])
# tem=tem.loc[tem['shared']!=0.0]
# vplace.insert_profit_split_mode_percentage(tem, mylog)

# %% permanent
contract_info2 = pd.read_sql("""
select concat('',w.Reference) as candidate_externalid
, concat('',j.Reference) as job_externalid
, Starting_Date 
, Ending_Date
from Work_History_ViewQ1 w
left join ds_job_basic_information_view j on w.Agreement_Ref = j.Aspire_Job_Ref
""", engine_mssql)
# assert False
assert False
# %% start date/end date
stdate = contract_info2[['job_externalid', 'candidate_externalid', 'Starting_Date','Ending_Date']]
stdate['start_date'] = pd.to_datetime(stdate['Starting_Date'])
stdate['end_date'] = pd.to_datetime(stdate['Ending_Date'])
cp1 = vplace.update_startdate_enddate(stdate, mylog)
