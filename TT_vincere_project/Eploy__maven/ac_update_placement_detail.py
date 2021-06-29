# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
from dateutil import relativedelta
import sqlalchemy
import datetime
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('maven_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_placement_detail
vplaced = vincere_placement_detail.PlacementDetail(engine_postgre.raw_connection())
# assert False

# %% placement detail
placement_detail = pd.read_sql("""
select 
p.ID
, p.JOB_ID as job_externalid
, p.APP_ID as candidate_externalid
, p.START_DATE as start_date
, p.END_DATE as end_date
, u.EmailAddress as job_owner
, p.LOCATION
, p.CostCenterID as cost_center
, p.LEAVE_REASON
, p.PO_NUMBER as client_general_po_number
, isnull(p.pay, j.Payment) as pay
, p.SPLIT_FEE_COUNT
, p.CONSULTANT
, isnull(p.CHARGE, j.Charge) as CHARGE
, p.CREATED_ON
, p.CHARGE_INT
, p.CHARGE_CHAR
, case j.JobType
                 when 5 then 'Contract'
                 when 6 then 'Permanent'
                 when 7 then 'Temporary'
				 end as jobtype
from NewPlacements p
join Job j on p.JOB_ID = j.Id
left join [User] u on p.PLACED_BY = u.Name
""", engine_mssql)

assert False
# %% calculate profit and percentage of annual salary
def _cal_profit_and_annual_salary_percentage(df):
    # for Permanent
    filter_contract = (df.CHARGE_INT.isin([0, 1, 2, 3, 4, 5])) & (df.jobtype.isin(['Permanent']))
    percentage_charge = df.loc[filter_contract]
    percentage_charge['profit'] = percentage_charge.CHARGE * percentage_charge.pay/100

    filter_contract = (df.CHARGE_INT.isin([6, 7])) & (df.jobtype.isin(['Permanent']))
    fixedfee_charge = df.loc[filter_contract]
    fixedfee_charge['profit'] = fixedfee_charge.CHARGE

    # for contract and tem
    filter_contract = (df.CHARGE_INT.isin([0, 1, 2, 3, 4, 5])) & (df.jobtype.isin(['Contract', 'Temporary']))
    percentage_charge_contract = df.loc[filter_contract]
    percentage_charge_contract['profit'] = percentage_charge_contract.CHARGE - percentage_charge_contract.pay
    percentage_charge_contract.loc[percentage_charge_contract.profit < 0, 'profit'] = 0

    filter_contract = (df.CHARGE_INT.isin([6, 7])) & (df.jobtype.isin(['Contract', 'Temporary'])) # annually and fixed_fee
    fixedfee_charge_contract = df.loc[filter_contract]
    fixedfee_charge_contract['profit'] = fixedfee_charge_contract.CHARGE
    fixedfee_charge_contract.loc[fixedfee_charge_contract.profit < 0, 'profit'] = 0
    return pd.concat([percentage_charge, fixedfee_charge, percentage_charge_contract, fixedfee_charge_contract])

# get latest placement details
placement_detail = placement_detail.groupby(['job_externalid', 'candidate_externalid']).apply(lambda subdf: subdf.sort_values('CREATED_ON', ascending=False))
placement_detail.index
placement_detail['rn'] = placement_detail.groupby(placement_detail.index).cumcount()
placement_detail = placement_detail.query("rn==0").reset_index(drop=True)

placement_detail = _cal_profit_and_annual_salary_percentage(placement_detail)

# %% apply quick fee forecast
vplaced.update_use_quick_fee_forecast_for_permanent_job()

# %%
tem = placement_detail[['job_externalid', 'candidate_externalid', 'pay']].rename(columns={'pay':'annual_salary'})
cp6 = vplaced.update_offer_annual_salary(tem, mylog)

tem = placement_detail[['job_externalid', 'candidate_externalid', 'CHARGE']].rename(columns={'CHARGE':'percentage_of_annual_salary'})

df = tem
logger = mylog
tem2 = df[['job_externalid', 'candidate_externalid', 'percentage_of_annual_salary']].dropna()
tem2 = tem2.merge(vplaced.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
tem2['id'] = tem2['offer_id']
updating_off = tem2.query("_merge=='both'")
vincere_custom_migration.psycopg2_bulk_update_tracking(updating_off, vplaced.ddbconn, ['percentage_of_annual_salary'], ['id', ], 'offer', logger)

# update profit
off = pd.read_sql("""
    select 
        id
        , percentage_of_annual_salary*gross_annual_salary/100 as profit
        , percentage_of_annual_salary*gross_annual_salary/100 as projected_profit
    from offer
""", vplaced.ddbconn)

off = off.loc[off['id'].isin(updating_off['id'])]
off.info()
off.profit = off.profit.astype(int)
off.projected_profit = off.projected_profit.astype(int)
vincere_custom_migration.psycopg2_bulk_update_tracking(off, vplaced.ddbconn, ['profit', 'projected_profit'], ['id', ], 'offer', logger)
# vplaced.update_percentage_of_annual_salary(tem, mylog)

tem = placement_detail[['job_externalid', 'candidate_externalid', 'profit']]
vplaced.update_offer_profit(tem, mylog)

# %% start date end date
cp = vplaced.update_startdate_enddate(placement_detail, mylog)

# %% offer date, placedate
placement_detail['offer_date'] = placement_detail.start_date
placement_detail['placed_date'] = placement_detail.start_date
vplaced.update_offerdate(placement_detail, mylog)

vplaced.update_placeddate(placement_detail, mylog)

# %% note
note = pd.read_sql("""
select 
p.ID
, p.JOB_ID as job_externalid
, p.APP_ID as candidate_externalid
, p.MODIFIEDBY
, p.MODIFY_ON
from NewPlacements p
join Job j on p.JOB_ID = j.Id
left join [User] u on p.PLACED_BY = u.Name
""", engine_mssql)

note = note.where(note.notnull(), None)
note.MODIFY_ON = note.MODIFY_ON.astype(object).where(note.MODIFY_ON.notnull(), None)
note.MODIFY_ON = note.MODIFY_ON.map(lambda x: datetime.datetime.strftime(x, '%d-%b-%Y %H:%M') if x else x)
note['note'] = note[['ID', 'MODIFIEDBY', 'MODIFY_ON']] \
    .apply(lambda x: '\n\n'.join([': '.join(str(e1) for e1 in e) for e in zip(['Placement External ID', 'Modified By', 'Modified On'], x) if e[1]]), axis=1)

vplaced.update_internal_note(note, mylog)

# # %% job owner
# jobowner = placement_detail[['job_externalid', 'job_owner']].rename(columns={'job_owner': 'email'})
# from common import vincere_job
# vjob = vincere_job.Job(engine_postgre.raw_connection())
# vjob.insert_owner(jobowner, mylog)

# %% split fee
split_fee = pd.read_sql("""
select 
sf.RECORD_ID as placement_id
, p.JOB_ID as job_externalid
, p.APP_ID as candidate_externalid
, sf.PERCENTAGE as shared  
, p.PAY * p.CHARGE / 100 as profit
, (p.PAY * p.CHARGE / 100) * sf.PERCENTAGE as amount
, u.EmailAddress as user_email
, p.CREATED_ON
, p.START_DATE
, p.END_DATE
, p.EXPECTED_DURATION
from SplitFees sf 
join NewPlacements p on sf.RECORD_ID = p.ID
left join (
    select 
    Id
    , case 
		when EmailAddress is null then concat(FirstName, '.', LastName, '@acuityconsultants.co.za') 
		when EmailAddress = '' then concat(FirstName, '.', LastName, '@acuityconsultants.co.za') 
		else EmailAddress end as EmailAddress
    from [User]
) 
u on sf.EMP_ID = u.Id
-- where RECORD_ID='HQ00001311';
""", engine_mssql)
# get latest placement details
split_fee = split_fee.groupby(['job_externalid', 'candidate_externalid']).apply(lambda subdf: subdf.sort_values('CREATED_ON', ascending=False))
split_fee.shared = split_fee.shared.astype(float)
split_fee['rn'] = split_fee.groupby(split_fee.index).cumcount()
split_fee = split_fee.query("rn==0").reset_index(drop=True)

# %% contract placement
contract_placement = split_fee.loc[split_fee.EXPECTED_DURATION != 'Permanent']
contract_placement['contract_length'] = contract_placement[['END_DATE', 'START_DATE']].apply(lambda x: relativedelta.relativedelta(x[0], x[1]), axis=1)
contract_placement['contract_length'] = contract_placement['contract_length'].map(lambda x: x.years*12 + x.months)
cp4 = vplaced.update_contract_length_type(contract_placement, 'month', mylog)
cp3 = vplaced.update_contract_length(contract_placement, mylog)

contract_placement['placement_type'] = 'contract'
vplaced.update_placementtype_or_jobtype(contract_placement, mylog)

# %% permanent placement
permanent_placement = split_fee.loc[split_fee.EXPECTED_DURATION == 'Permanent']
permanent_placement['placement_type'] = 'permanent'
cp5 = vplaced.update_placementtype_or_jobtype(permanent_placement, mylog)

# %%
cp2 = vplaced.insert_profit_split_mode_percentage(split_fee, mylog, override=True)
# cp2 = vplaced.update_offer_profit(split_fee, mylog)

# %% set default contract lenght
vplaced.update_default_enddate_by_startdate_for_contract_jobs(mylog)

# # %% currency
# tem = placement_detail[['job_externalid', 'candidate_externalid', 'CHARGE_CHAR']]
#
# tem['currency_type'] = tem.CHARGE_CHAR.map(lambda x: vplaced.map_currency_code(x))
# placement_detail.head()
# vplaced.update_offer_currency_type(tem, mylog)

# %% work start date: work_start_date position_candidate
tem = placement_detail[['job_externalid', 'candidate_externalid', 'start_date']].rename(columns={'start_date':'work_start_date'})
cp7 = vplaced.update_work_start_date_position_candidate(tem, mylog)

# %% place date: hire_date position_candidate
tem = placement_detail[['job_externalid', 'candidate_externalid', 'CREATED_ON']].rename(columns={'CREATED_ON':'hire_date'})
cp8 = vplaced.update_hire_date_position_candidate(tem, mylog)