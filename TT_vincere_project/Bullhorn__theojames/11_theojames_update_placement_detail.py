# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import datetime
import re
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
cf.read('theo_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(engine_postgre.raw_connection())

# %% job
placement_detail_info = pd.read_sql("""
select 
pl.jobPostingID as job_externalid
, C.candidateid as candidate_externalid
, C.fullname
, pl.dateadded 
, pl.clientBillRate
, pl.overtimeRate
, pl.dateBegin
, pl.dateEnd
, pl.dateEffective
, pl.employmentType
, pl.fee
, pl.payRate
, pl.salary
, pl.salaryUnit
, pl.hoursPerDay
, stuff( 'PLACEMENT: ' + char(10)
	  + coalesce('Placement ID: ' + NULLIF(cast(pl.placementID as nvarchar(max)), '') + char(10), '')
      + coalesce('Billing Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL)
								+ coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,'') + char(10), NULL), ''), NULL)  --pl.billingUserID
	--+ coalesce('Bill Rate Information: ' + NULLIF(cast(pl.billRateInfoHeader as nvarchar(max)), '') + char(10), '')
      + coalesce('Employee Type: ' + NULLIF(cast(pl.employmentType as nvarchar(max)), '') + char(10), '')
      + coalesce('Over-time Bill Rate: ' + NULLIF(cast(pl.clientOverTimeRate as nvarchar(max)), '') + char(10), '')
      
      --+ coalesce('Number Previous Weeks: ' + NULLIF(cast(pl.customInt3 as nvarchar(max)), '') + char(10), '')
      + coalesce('Comments: ' + NULLIF(cast(pl.comments as nvarchar(max)), '') + char(10), '')
      --+ coalesce('Pay Rate (Weekend Rate): ' + NULLIF(cast(pl.customPayRate1 as nvarchar(max)), '') + char(10), '')
      --+ coalesce('Pay Rate (International): ' + NULLIF(cast(pl.customPayRate2 as nvarchar(max)), '') + char(10), '')
      --+ coalesce('Other: ' + NULLIF(cast(pl.customText5 as nvarchar(max)), '') + char(10), '')
      --+ coalesce('Referral Fee: ' + NULLIF(cast(pl.customText6 as nvarchar(max)), '') + char(10), '')
      --+ coalesce('Travelling Expenses: ' + NULLIF(cast(pl.customText7 as nvarchar(max)), '') + char(10), '')
      --+ coalesce('Specific Contract Terms: ' + NULLIF(cast(pl.customTextBlock1 as nvarchar(max)), '') + char(10), '')
      --+ coalesce('Date Added: ' + NULLIF(cast(pl.dateadded as nvarchar(max)), '') + char(10), '')
      + coalesce('Reporting to: ' + NULLIF(cast(pl.reportTo as nvarchar(max)), '') + char(10), '')   
      + coalesce('Employee Payment Type: ' + NULLIF(cast(pl.employeeType as nvarchar(max)), '') + char(10), '') 
      + coalesce('Status: ' + NULLIF(cast(pl.status as nvarchar(max)), '') + char(10), '')  
	
, 1, 0, '') as 'content'
from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
left join bullhorn1.BH_UserContact UC1 ON UC1.userID = pl.billingUserID
left join ( select candidateID, concat(FirstName,' ',LastName) as fullname, userid from bullhorn1.Candidate) C on C.userID = pl.userid

""", engine_mssql)
assert False
# placement_detail_info.rename(columns={'dateBegin': 'start_date', 'dateEnd': 'end_date'}, inplace=True)
placement_detail_info['start_date'] = pd.to_datetime(placement_detail_info.dateBegin)
placement_detail_info['end_date'] = pd.to_datetime(placement_detail_info.dateEnd)
placement_detail_info.job_externalid = placement_detail_info.job_externalid.astype(str)
placement_detail_info.candidate_externalid = placement_detail_info.candidate_externalid.astype(str)


# %% placement note
placement_detail_info['note'] = placement_detail_info.content
cp0 = vplace.update_internal_note(placement_detail_info, mylog)

# %% charge rate
placement_detail_info['charge_rate'] = placement_detail_info.clientBillRate
cp1 = vplace.update_charge_rate(placement_detail_info, mylog)

# %% start date
placement_detail_info.loc[placement_detail_info.start_date.notnull(), 'start_date'] = placement_detail_info.loc[placement_detail_info.start_date.notnull(), ].start_date.dt.strftime('%Y-%m-%d %H:%M:%S')
placement_detail_info.loc[placement_detail_info.end_date.notnull(), 'end_date'] = placement_detail_info.loc[placement_detail_info.end_date.notnull(), ].end_date.dt.strftime('%Y-%m-%d %H:%M:%S')
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'start_date']].dropna()
tem.start_date = pd.to_datetime(tem.start_date)
cp2 = vplace.update_startdate_only_for_placement_detail(tem, mylog)

tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'start_date', 'end_date']].dropna()
tem.start_date = pd.to_datetime(tem.start_date)
tem.end_date = pd.to_datetime(tem.end_date)
cp2 = vplace.update_startdate_enddate(tem, mylog)

# %% place date
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'dateEffective']].dropna()
tem['placed_date'] = tem.dateEffective
cp3 = vplace.update_placeddate(tem, mylog)

# %% job type
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'employmentType']].dropna()
tem.replace({'Permanent': 'permanent', 'Contract': 'contract', 'Fixed Term': 'contract', 'Temporary': 'contract', 'Opportunity': 'contract'}, inplace=True)
tem.employmentType.unique()
tem['placement_type'] = tem.employmentType
cp4 = vplace.update_placementtype_or_jobtype(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()

tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'fee']].dropna()
tem['percentage_of_annual_salary'] = pd.to_numeric(tem.fee)*100
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
tem.info()
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)
# %% test
# df = tem
# logger = mylog
# tem2 = df[['job_externalid', 'candidate_externalid', 'percentage_of_annual_salary']].dropna()
# tem2 = tem2.merge(vplace.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
# tem2['id'] = tem2['offer_id']
# updating_off = tem2.query("_merge=='both'")
# vincere_custom_migration.psycopg2_bulk_update_tracking(updating_off, vplace.ddbconn, ['percentage_of_annual_salary'], ['id', ], 'offer', logger)
#
# # update profit
# off = pd.read_sql("""
#     select
#         id
#         , percentage_of_annual_salary*gross_annual_salary/100 as profit
#         , percentage_of_annual_salary*gross_annual_salary/100 as projected_profit
#     from offer
# """, vplace.ddbconn)
#
# off = off.loc[off['id'].isin(updating_off['id'])]
# vincere_custom_migration.psycopg2_bulk_update_tracking(off, vplace.ddbconn, ['profit', 'projected_profit'], ['id', ], 'offer', logger)


# %% hours per day
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'hoursPerDay']].dropna()
tem['working_hour_per_day'] = tem.hoursPerDay
cp7 = vplace.update_offer_working_hour_per_day(tem, mylog)

# %% payrate
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'payRate']].dropna()
tem['pay_rate'] = tem.payRate
cp8 = vplace.update_pay_rate(tem, mylog)

# %% update salary
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'salary']].dropna()
tem['salary_from'] = tem.salary
cp9 = vplace.update_salary_from(tem, mylog)

# %% pay interval
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'salaryUnit']].dropna()
tem.salaryUnit.replace({'Per Hour': 'hourly', 'Per Month': 'monthly', 'Per Day': 'daily'}, inplace = True)
tem['pay_interval'] = tem.salaryUnit
tem = tem.loc[tem.pay_interval.str.strip()!= '']
cp10 = vplace.update_pay_interval(tem, mylog)
