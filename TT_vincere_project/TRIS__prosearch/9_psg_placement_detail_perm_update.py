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
cf.read('psg_config.ini')
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
perm_info_rate = pd.read_sql("""
select JobID as job_externalid
     , CandidateID as candidate_externalid
     , PlacementDate, ApprovedDate
     , pp.StartDate
     , coalesce(Salary, 0) as Salary
     , nullif(convert(varchar,PONumber),'') as PONumber
     , coalesce(PlacementFee, 0) as PlacementFee
     , nullif(convert(nvarchar(max),SpecialInvoiceDetails),'') as SpecialInvoiceDetails
     , FeeRate
     , A.AMEMail as onwer1
     , AM1percent
     , A2.AMEMail as onwer2
     , AM2percent
     , A3.AMEMail as onwer3
     , AM3percent
     , A4.AMEMail as onwer4
     , AM4percent
     , A5.AMEMail as onwer5
     , AM5percent
     , A6.AMEMail as onwer6
     , AM6percent
from PermanentPlacement pp
left join AM A on pp.AM1 = A.AMID
left join AM A2 on pp.AM2 = A2.AMID
left join AM A3 on pp.AM3 = A3.AMID
left join AM A4 on pp.AM4 = A4.AMID
left join AM A5 on pp.AM5 = A5.AMID
left join AM A6 on pp.AM6 = A6.AMID
where nullif(FeeRate,'') is not null
""", engine_mssql)
tem1 = perm_info_rate.loc[perm_info_rate['onwer1'].str.contains('@')]
tem2 = perm_info_rate.loc[~perm_info_rate['onwer1'].str.contains('@')]
tem2['onwer1'] = tem2['onwer1'].apply(lambda x: x.lower().replace(' ',''))
tem2['onwer1'] = tem2['onwer1']+'@email.com'
perm_info_rate = pd.concat([tem1, tem2])

perm_info_no_rate = pd.read_sql("""
select JobID as job_externalid
     , CandidateID as candidate_externalid
     , PlacementDate, ApprovedDate
     , pp.StartDate
     , coalesce(Salary, 0) as Salary
     , nullif(convert(varchar,PONumber),'') as PONumber
     , coalesce(PlacementFee, 0) as PlacementFee
     , nullif(convert(nvarchar(max),SpecialInvoiceDetails),'') as SpecialInvoiceDetails
     , case when coalesce(Salary, 0) = 0 then 0 else PlacementFee/coalesce(Salary, 0)*100 end as FeeRate
     , A.AMEMail as onwer1
     , AM1percent
     , A2.AMEMail as onwer2
     , AM2percent
     , A3.AMEMail as onwer3
     , AM3percent
     , A4.AMEMail as onwer4
     , AM4percent
     , A5.AMEMail as onwer5
     , AM5percent
     , A6.AMEMail as onwer6
     , AM6percent
from PermanentPlacement pp
left join AM A on pp.AM1 = A.AMID
left join AM A2 on pp.AM2 = A2.AMID
left join AM A3 on pp.AM3 = A3.AMID
left join AM A4 on pp.AM4 = A4.AMID
left join AM A5 on pp.AM5 = A5.AMID
left join AM A6 on pp.AM6 = A6.AMID
where nullif(FeeRate,'') is null
""", engine_mssql)
perm_info_no_rate.FeeRate.fillna(0, inplace=True)
tem1 = perm_info_no_rate.loc[perm_info_no_rate['onwer1'].str.contains('@')]
tem2 = perm_info_no_rate.loc[~perm_info_no_rate['onwer1'].str.contains('@')]
tem2['onwer1'] = tem2['onwer1'].apply(lambda x: x.lower().replace(' ',''))
tem2['onwer1'] = tem2['onwer1']+'@email.com'
perm_info_no_rate = pd.concat([tem1, tem2])
perm_info = pd.concat([perm_info_rate, perm_info_no_rate])
perm_info['job_externalid'] = perm_info['job_externalid'].astype(str)
perm_info['candidate_externalid'] = perm_info['candidate_externalid'].astype(str)
assert False
# %% start date/end date
stdate = perm_info[['job_externalid', 'candidate_externalid', 'StartDate']].dropna()
stdate['start_date'] = stdate['StartDate']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% offer date/place date
jobapp_offer_date = perm_info[['job_externalid', 'candidate_externalid', 'ApprovedDate']].dropna()
jobapp_offer_date['offer_date'] = pd.to_datetime(jobapp_offer_date['ApprovedDate'])
jobapp_placed_date = perm_info[['job_externalid', 'candidate_externalid', 'PlacementDate']].dropna()
jobapp_placed_date['placed_date'] = pd.to_datetime(jobapp_placed_date['PlacementDate'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
vplace.update_offerdate(jobapp_offer_date, mylog)
vplace.update_placeddate(jobapp_placed_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% po number
tem = perm_info[['job_externalid', 'candidate_externalid', 'PONumber']].dropna().rename(columns={'PONumber':'client_purchase_order'})
cp6 = vplace.update_purchase_order_number(tem,connection, mylog)

# %% salary
tem = perm_info[['job_externalid', 'candidate_externalid', 'Salary']].dropna().rename(columns={'Salary':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()

tem = perm_info[['job_externalid', 'candidate_externalid', 'FeeRate']].dropna()
tem['percentage_of_annual_salary'] = tem['FeeRate']
tem['percentage_of_annual_salary'].unique()
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% placement note
note = perm_info[['job_externalid', 'candidate_externalid', 'SpecialInvoiceDetails']].dropna()
note['note'] = note['SpecialInvoiceDetails']
cp0 = vplace.update_internal_note(note, mylog)

# %% split
tem1 = perm_info[['job_externalid', 'candidate_externalid', 'onwer1','AM1percent']].dropna()
tem1['user_email'] = tem1['onwer1']
tem1['shared'] = tem1['AM1percent']
tem1['shared'] = tem1['shared'].astype(float)
tem1.drop(['onwer1', 'AM1percent'], axis=1, inplace=True)
tem2 = perm_info[['job_externalid', 'candidate_externalid', 'onwer2','AM2percent']].dropna()
tem2['user_email'] = tem2['onwer2']
tem2['shared'] = tem2['AM2percent']
tem2['shared'] = tem2['shared'].astype(float)
tem2.drop(['onwer2', 'AM2percent'], axis=1, inplace=True)
tem3 = perm_info[['job_externalid', 'candidate_externalid', 'onwer3','AM3percent']].dropna()
tem3['user_email'] = tem3['onwer3']
tem3['shared'] = tem3['AM3percent']
tem3['shared'] = tem3['shared'].astype(float)
tem3.drop(['onwer3', 'AM3percent'], axis=1, inplace=True)
tem4 = perm_info[['job_externalid', 'candidate_externalid', 'onwer4','AM4percent']].dropna()
tem4['user_email'] = tem4['onwer4']
tem4['shared'] = tem4['AM4percent']
tem4['shared'] = tem4['shared'].astype(float)
tem4.drop(['onwer4', 'AM4percent'], axis=1, inplace=True)
tem = pd.concat([tem1,tem2,tem3,tem4])
tem=tem.loc[tem['shared']!=0.0]
vplace.insert_profit_split_mode_percentage(tem, mylog)

