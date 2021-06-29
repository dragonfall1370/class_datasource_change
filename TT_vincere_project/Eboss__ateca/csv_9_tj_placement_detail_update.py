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
cf.read('tj_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
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
select  p.*, "RATE PAY", "RATE PAY CURRENCY", "RATE BILL", "RATE BILL CURRENCY", "RATE DESCRIPTION", "RATE PAY UNIT", RMSBASERATE, RMSRATEID  from Placement p
left join Rates r on p.RMSPLACEMENTID = r.RMSPLACEMENTID
""", engine_sqlite)
placed_info['job_externalid'] = placed_info['RMSPLACEMENTID'].astype(str)
placed_info['candidate_externalid'] = placed_info['RMSCANDIDATEID'].astype(str)
placed_info['job_externalid'] = placed_info['RMSPLACEMENTID'].astype(str)
placed_info['rn'] = placed_info.groupby('RMSPLACEMENTID').cumcount()
placed_info = placed_info.loc[placed_info['rn']==0]

assert False
# %% offer date/place date
jobapp_created_date = placed_info[['job_externalid', 'candidate_externalid','EFFECTIVE DATE OF CHANGE']].dropna()
jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['EFFECTIVE DATE OF CHANGE'])
jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['EFFECTIVE DATE OF CHANGE'])
vplace.update_offerdate(jobapp_created_date, mylog)
vplace.update_placeddate(jobapp_created_date, mylog)

# %% start date/end date
stdate = placed_info[['job_externalid', 'candidate_externalid', 'CANDIDATE START DATE', 'ACTUAL END DATE']]
stdate['start_date'] = pd.to_datetime(stdate['CANDIDATE START DATE'])
stdate['end_date'] = pd.to_datetime(stdate['ACTUAL END DATE'])
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% po number
tem = placed_info[['job_externalid', 'candidate_externalid', 'PO NUMBER']].dropna()
tem['client_purchase_order'] = tem['PO NUMBER']
vplace.update_purchase_order_number(tem, connection, mylog)

# %% payrate
tem = placed_info[['job_externalid', 'candidate_externalid', 'RATE PAY']].dropna()
tem['pay_rate'] = tem['RATE PAY'].astype(float)
cp8 = vplace.update_pay_rate(tem, mylog)

# %% charge rate
tem = placed_info[['job_externalid', 'candidate_externalid', 'RATE BILL']].dropna()
tem['charge_rate'] = tem['RATE BILL'].astype(float)
cp1 = vplace.update_charge_rate(tem, mylog)

# %% currency country
tem = placed_info[['job_externalid', 'candidate_externalid','RATE PAY CURRENCY']].dropna()
tem['RATE PAY CURRENCY'].unique()
tem.loc[(tem['RATE PAY CURRENCY'] == 'MYR    '), 'currency_type'] = 'myr'
tem.loc[(tem['RATE PAY CURRENCY'] == 'USD    '), 'currency_type'] = 'usd'
vplace.update_offer_currency_type(tem, mylog)

# %% interval
tem = placed_info[['job_externalid','candidate_externalid','RATE PAY UNIT']].dropna() #pay_interval
tem['RATE PAY UNIT'].unique()
tem.loc[(tem['RATE PAY UNIT'] == 'Monthly'), 'pay_interval'] = 'monthly'
tem.loc[(tem['RATE PAY UNIT'] == 'Hourly'), 'pay_interval'] = 'hourly'
tem.loc[(tem['RATE PAY UNIT'] == 'Daily'), 'pay_interval'] = 'daily'
tem.loc[(tem['RATE PAY UNIT'] == 'Per Diem'), 'pay_interval'] = 'daily'
vplace.update_pay_interval(tem, mylog)

# %% placement note
note = placed_info[['candidate_externalid','job_externalid'
    ,'RMSPLACEMENTID'
    ,'POSITION TITLE',
    'CLIENT',
    'CONTACT'
    ,'INDUSTRIES'
    ,'JOB TITLE(S)'
    ,'SCHEDULED END DATE'
    ,'PAYROLL BURDEN/TAX ID'
    ,'HOURS PER DAY'
    ,'NOTICE PERIOD'
    ,'WORK ORDER NUMBER'
    ,'WORK ORDER START DATE'
    ,'WORK ORDER END DATE'
    ,'SPECIAL INVOICE DETAILS'
    ,'INVOICE CONTACT AND ADDRESS'
    ,'PAY FREQUENCY'
    ,'ADDITIONAL PAY INFORMATION'
    ,'SPECIFIC CONTRACT TERMS (NOT USED FOR PAYROLL)'
    ,'PRINCIPAL WORKING COUNTRY'
    ,'NORMAL WORKING LOCATION'
    ,'SECTOR/INDUSTRY'
    ,'OFFSHORE?'
    ,'COMMENTS'
    ,'LOCATION'
    ,'CITY'
    ,'POST CODE'
    ,'RATE DESCRIPTION'
    ,'RMSRATEID'
    ,'RMSBASERATE'
    ]]

note['note'] = note[[
    'RMSPLACEMENTID'
    ,'POSITION TITLE',
    'CLIENT',
    'CONTACT'
    ,'INDUSTRIES'
    ,'JOB TITLE(S)'
    ,'SCHEDULED END DATE'
    ,'PAYROLL BURDEN/TAX ID'
    ,'HOURS PER DAY'
    ,'NOTICE PERIOD'
    ,'WORK ORDER NUMBER'
    ,'WORK ORDER START DATE'
    ,'WORK ORDER END DATE'
    ,'SPECIAL INVOICE DETAILS'
    ,'INVOICE CONTACT AND ADDRESS'
    ,'PAY FREQUENCY'
    ,'ADDITIONAL PAY INFORMATION'
    ,'SPECIFIC CONTRACT TERMS (NOT USED FOR PAYROLL)'
    ,'PRINCIPAL WORKING COUNTRY'
    ,'NORMAL WORKING LOCATION'
    ,'SECTOR/INDUSTRY'
    ,'OFFSHORE?'
    ,'COMMENTS'
    ,'LOCATION'
    ,'CITY'
    ,'POST CODE'
    ,'RATE DESCRIPTION'
    ,'RMSRATEID'
    ,'RMSBASERATE']] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip([
    'RMSPLACEMENTID'
    ,'POSITION TITLE',
    'CLIENT',
    'CONTACT'
    ,'INDUSTRIES'
    ,'JOB TITLE(S)'
    ,'SCHEDULED END DATE'
    ,'PAYROLL BURDEN/TAX ID'
    ,'HOURS PER DAY'
    ,'NOTICE PERIOD'
    ,'WORK ORDER NUMBER'
    ,'WORK ORDER START DATE'
    ,'WORK ORDER END DATE'
    ,'SPECIAL INVOICE DETAILS'
    ,'INVOICE CONTACT AND ADDRESS'
    ,'PAY FREQUENCY'
    ,'ADDITIONAL PAY INFORMATION'
    ,'SPECIFIC CONTRACT TERMS (NOT USED FOR PAYROLL)'
    ,'PRINCIPAL WORKING COUNTRY'
    ,'NORMAL WORKING LOCATION'
    ,'SECTOR/INDUSTRY'
    ,'OFFSHORE?'
    ,'COMMENTS'
    ,'LOCATION'
    ,'CITY'
    ,'POST CODE'
    ,'RATE DESCRIPTION'
    ,'RMSRATEID'
    ,'RMSBASERATE'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)
