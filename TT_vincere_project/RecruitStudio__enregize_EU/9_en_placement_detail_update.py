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
cf.read('en_config.ini')
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
# %%
sql = """
select VacancyId as job_externalid
     , ContactId as candidate_externalid
     , StartDate
     , InvoiceDate
     , OfferAgreedDate
     , BillableSalary
     , Currency1, fee, InvoiceTotal
from Placements
"""
placement_detail_info = pd.read_sql(sql, engine_mssql)
placement_detail_info['job_externalid'] = 'EUK'+placement_detail_info['job_externalid']
placement_detail_info['candidate_externalid'] = 'EUK'+placement_detail_info['candidate_externalid']
assert False
# %% start date/end date
stdate = placement_detail_info[['job_externalid', 'candidate_externalid', 'StartDate']]
stdate['start_date'] = stdate['StartDate']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% offer date/place date
jobapp_offer_date = placement_detail_info[['job_externalid', 'candidate_externalid', 'OfferAgreedDate']]
jobapp_offer_date['offer_date'] = pd.to_datetime(jobapp_offer_date['OfferAgreedDate'])
jobapp_placed_date = placement_detail_info[['job_externalid', 'candidate_externalid', 'InvoiceDate']]
jobapp_placed_date['placed_date'] = pd.to_datetime(jobapp_placed_date['InvoiceDate'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
vplace.update_offerdate(jobapp_offer_date, mylog)
vplace.update_placeddate(jobapp_placed_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% invoice date
stdate = placement_detail_info[['job_externalid', 'candidate_externalid', 'InvoiceDate']].dropna()
stdate['invoice_date'] = stdate['InvoiceDate']
stdate['invoice_date'] = pd.to_datetime(stdate['invoice_date'])
cp1 = vplace.update_invoice_date(stdate, mylog)

# %% salary
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'BillableSalary']].dropna().rename(columns={'BillableSalary':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary(tem, mylog)

# %% salary
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'InvoiceTotal']].dropna().rename(columns={'InvoiceTotal':'invoice_total'})
tem['invoice_total'] = tem['invoice_total'].astype(float)
cp6 = vplace.update_invoice_total(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()

tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'fee']].dropna()
tem['percentage_of_annual_salary'] = tem['fee']
tem['percentage_of_annual_salary'].unique()
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary(tem, mylog)

# %% placement note
sql = """
select VacancyId as job_externalid
     , ContactId as candidate_externalid
     , nullif(Notes, '') as notes
     , nullif(Fee, '') as Fee
     , nullif(Benefits, '') as Benefits
     , nullif(CandidateSource, '') as CandidateSource
     , nullif(convert(varchar,BillableSalary), '') as BillableSalary
     , nullif(convert(varchar,InvoiceTotal), '') as InvoiceTotal
     , nullif(RebatePeriod, '') as RebatePeriod
     , nullif(PaymentTerms, '') as PaymentTerms
     , nullif(PurchaseOrder, '') as PurchaseOrder
from Placements
"""
note = pd.read_sql(sql, engine_mssql)
note['job_externalid'] = 'EUK'+note['job_externalid']
note['candidate_externalid'] = 'EUK'+note['candidate_externalid']
note = note.where(note.notnull(),None)
note['PaymentTerms'] = note['PaymentTerms'].apply(lambda x: str(x) if x else x)
note['note'] = note[['notes','Fee',
                    'Benefits', 'CandidateSource', 'BillableSalary', 'InvoiceTotal',
                    'RebatePeriod', 'PaymentTerms', 'PurchaseOrder']] \
   .apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Notes', 'Fee',
                                                                       'Benefits', 'Source', 'Billable Salary', 'Invoice Total',
                                                                       'Rebate Period', 'Payment Terms', 'Purchase Order'], x) if e[1]]), axis=1)
cp0 = vplace.update_internal_note(note, mylog)

# %% split
tem = placement_detail_info[['job_externalid', 'candidate_externalid', 'owner']].dropna()
tem['user_email'] = tem['owner']
tem['shared'] = 100
tem['shared'] = tem['shared'].astype(float)
vplace.insert_profit_split_mode_percentage(tem, mylog)

