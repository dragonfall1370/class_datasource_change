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
cf.read('mc_config.ini')
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
select OPID as job_externalid, j.NAME, FORAMT, FORPROB, STARTDATE, CLOSEDDATE, CLOSEBY,STATUS,nullif(trim(UOPCOMMENT),'') as special_amount, UOPCOMMISI as special_com, nullif(trim(UOPPRESEAR),'') as special_descript from OPMGR j
join company com on com.COMPANY = j.COMPANY
left join (select c1.ACCOUNTNO as ACCOUNTNO_2, c1.CONTACT as CONTACT_2, c.COMPANY as COMPANY_2 from CONTACT1 c1
left join company c on c.COMPANY = c1.COMPANY
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')) cont on cont.ACCOUNTNO_2 = j.ACCOUNTNO and cont.COMPANY_2 = j.COMPANY
left join USERS u on u.USERNAME = j.USERID
where RECTYPE = 'O  '
and com.COMPANY is not null
""", engine_mssql)
assert False
# %% start date/end date
stdate = placed_info[['job_externalid', 'STARTDATE']].dropna()
stdate['start_date'] = stdate['STARTDATE']
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
cp1 = vplace.update_startdate_only_for_placement_detail_jobonly(stdate, mylog)

# %% invoice date
# stdate = perm_info[['job_externalid', 'candidate_externalid', 'Invoice_Date']].dropna()
# stdate['invoice_date'] = stdate['Invoice_Date']
# stdate['invoice_date'] = pd.to_datetime(stdate['invoice_date'])
# cp1 = vplace.update_invoice_date(stdate, mylog)

# %% offer date/place date
jobapp_offer_date = placed_info[['job_externalid', 'STARTDATE']].dropna()
jobapp_offer_date['offer_date'] = pd.to_datetime(jobapp_offer_date['STARTDATE'])
vplace.update_offerdate_jobonly(jobapp_offer_date, mylog)

jobapp_placed_date = placed_info[['job_externalid', 'STARTDATE']].dropna()
jobapp_placed_date['placed_date'] = pd.to_datetime(jobapp_placed_date['STARTDATE'])
# jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['jobapp_created_date'])
vplace.update_placeddate_jobonly(jobapp_placed_date, mylog)
# vplace.update_sent_date(jobapp_created_date, mylog)

# %% job type
jt = placed_info[['job_externalid', 'STATUS']].dropna().drop_duplicates()
job_type_map = pd.read_csv('Mindcor_Job _ Status.csv')
jt = jt.merge(job_type_map, left_on='STATUS', right_on='Job > Status', how='left')
# jt['Job_model'].unique()
# jt.loc[jt['Job_model']=='Permanent', 'job_type'] = 'permanent'
# jt.loc[jt['Job_model']=='Contract', 'job_type'] = 'contract'
# jt.loc[jt['Job_model']=='Temporary', 'job_type'] = 'contract'
# jt.loc[jt['Job_model']=='Shift', 'job_type'] = 'contract'
# jt2 = jt[['job_externalid', 'job_type']].dropna()
# tem2 = df[['job_externalid', 'job_type','position_sub_type']]
jt['job_type'] = jt['Job_type']
jt['perm_sub_type'] = jt['Perm_Sub_Stype']
jt = jt.loc[jt['Job_type'].notnull()]

df=jt
tem2 = df[['job_externalid', 'job_type', 'perm_sub_type']]
assert set(tem2['job_type'].value_counts().keys()) \
    .issubset(set(vplace.jobtype['desc'].values)), \
    "There are some invalid job types values"

assert set(tem2['perm_sub_type'].value_counts().keys()) \
    .issubset(set(vplace.perm_sub_type['desc'].values)), \
    "There are some invalid job types values"

tem2 = tem2.merge(vplace.jobtype, left_on='job_type', right_on='desc')
tem2 = tem2.merge(vplace.perm_sub_type, left_on='perm_sub_type', right_on='desc')

# transform data
# tem2 = tem2.merge(self.job, on=['job_externalid'])
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn,['position_type', 'position_sub_type'], ['id', ],'position_description', logger)
# return tem2

# prepare position type values

# tem2 = df[['job_externalid', 'candidate_externalid', 'placement_type']]
# assert set(tem2['placement_type'].value_counts().keys()) \
#     .issubset(set(self.jobtype['desc'].values)), \
#     "There are some invalid job types values"
# tem2 = tem2.merge(self.jobtype, left_on='placement_type', right_on='desc')

# transform data
tem2 = tem2.merge(vplace.position_candidate, on='job_externalid')
tem2['id'] = tem2['offer_id']
tem3 = tem2[['id','position_type', 'position_sub_type' ]].dropna()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem3, vplace.ddbconn, ['position_type', 'position_sub_type' ], ['id', ], 'offer', mylog)

# cp5 = vplace.update_jobtype_job_only(jt, mylog)

# %% salary
tem = placed_info[['job_externalid', 'FORAMT']].dropna().rename(columns={'FORAMT':'annual_salary'})
tem['annual_salary'] = tem['annual_salary'].astype(float)
cp6 = vplace.update_offer_annual_salary_jobonly(tem, mylog)

# %% quick fee
vplace.update_use_quick_fee_forecast_for_permanent_job()

tem = placed_info[['job_externalid','FORPROB']].dropna()
tem['percentage_of_annual_salary'] = tem['FORPROB']
tem['percentage_of_annual_salary'] = tem['percentage_of_annual_salary'].astype(float)
tem.percentage_of_annual_salary = tem.percentage_of_annual_salary.round(2)
cp5, cp6 = vplace.update_percentage_of_annual_salary_jobonly(tem, mylog)

# %% placement note
note = placed_info[['job_externalid', 'special_amount', 'special_com','special_descript']].dropna()
note['special_com'] = note['special_com'].astype(str)
note['note'] = note[['special_amount', 'special_com','special_descript']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Special Amount','Special Com','Special Descript'], x) if e[1]]), axis=1)

cp0 = vplace.update_internal_note_jobonly(note, mylog)

# %% invoice note
# note = perm_info[['job_externalid', 'candidate_externalid', 'Commission_Gross', 'VAT_Amount']].dropna()
# note['Commission_Gross'] = note['Commission_Gross'].astype(str)
# note['VAT_Amount'] = note['VAT_Amount'].astype(str)
# note['invoice_message'] = note[['Commission_Gross', 'VAT_Amount']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Invoice Total', 'VAT'], x) if e[1]]), axis=1)
#
# cp0 = vplace.update_invoice_note(note, mylog)

# %% split
# tem1 = perm_info[['job_externalid', 'candidate_externalid', 'onwer1','AM1percent']].dropna()
# tem1['user_email'] = tem1['onwer1']
# tem1['shared'] = tem1['AM1percent']
# tem1['shared'] = tem1['shared'].astype(float)
# tem1.drop(['onwer1', 'AM1percent'], axis=1, inplace=True)
# tem2 = perm_info[['job_externalid', 'candidate_externalid', 'onwer2','AM2percent']].dropna()
# tem2['user_email'] = tem2['onwer2']
# tem2['shared'] = tem2['AM2percent']
# tem2['shared'] = tem2['shared'].astype(float)
# tem2.drop(['onwer2', 'AM2percent'], axis=1, inplace=True)
# tem = pd.concat([tem1,tem2])
# tem=tem.loc[tem['shared']!=0.0]
# vplace.insert_profit_split_mode_percentage(tem, mylog)

