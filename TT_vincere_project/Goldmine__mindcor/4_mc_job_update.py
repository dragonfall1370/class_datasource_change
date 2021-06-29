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
cf.read('mc_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
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

from common import vincere_job
vjob = vincere_job.Job(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)

# %%
job = pd.read_sql(""" 
select OPID as job_externalid, j.NAME, FORAMT, FORPROB, STARTDATE, CLOSEDDATE, CLOSEBY,STATUS,UOPCOMMENT,UOPCOMMISI from OPMGR j
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
j_prod = pd.read_sql("""select id, name from position_description""", engine_postgre_review)
j_prod['name'] = j_prod['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, vjob.ddbconn, ['name'], ['id'], 'position_description', mylog)
# vjob.set_job_location_by_company_location(mylog)
# %% currency country
tem = job[['job_externalid']]
tem['currency_type'] = 'zar'
tem['country_code'] = 'ZA'
vjob.update_currency_type(tem, mylog)
vjob.update_country_code(tem, mylog)

# %% country
# int = pd.read_sql("""select ac.idassignment as job_externalid, i.value as international
# from assignmentcode ac
# left join International i on i.idInternational = ac.codeid
# where idtablemd = '94b9bb6a-5f20-41bd-bc1d-59d34b2550ac'""", engine_sqlite)
# int['country_code'] = int['international'].map(vcom.get_country_code)
# vjob.update_country_code(int, mylog)

# # %% internal_description
# tem = job[['job_externalid', 'RebatePeriod','VacancyDetails','RegDate']]
# tem['RegDate'] = tem['RegDate'].astype(str)
# tem['internal_description'] = tem[['RebatePeriod','VacancyDetails','RegDate']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Rebate Period', 'Notes','RegDate'], x) if e[1]]), axis=1)
# tem = tem.loc[tem.internal_description != '']
# tem.loc[tem['RebatePeriod'].notnull()]
# cp8 = vjob.update_internal_description2(tem, dest_db, mylog)

# %% public_description
# tem = job[['job_externalid', 'Job_Description']].dropna().drop_duplicates()
# tem['public_description'] = tem['Job_Description']
# cp8 = vjob.update_public_description(tem, mylog)

# # %% rate_type
# employment_type = job[['job_externalid', 'FullPart']].dropna()
# employment_type['FullPart'].unique()
# employment_type.loc[(employment_type['FullPart'] == 'Full-time'), 'employment_type'] = 0
# employment_type.loc[(employment_type['FullPart'] == 'Part-time'), 'employment_type'] = 1
# tem = employment_type[['job_externalid', 'employment_type']].dropna()
# employment_type.loc[employment_type['employment_type'] == 1]
# vjob.update_employment_type(tem, mylog)

# %% head count
# hc = job[['job_externalid', 'No_Required']].dropna().drop_duplicates()
# hc['head_count'] = hc['No_Required']
# # hc['head_count'] = hc['head_count'].astype(float)
# hc['head_count'] = hc['head_count'].astype(int)
# cp1 = vjob.update_head_count(hc, mylog)

# %% job type
jt = job[['job_externalid', 'STATUS']].dropna().drop_duplicates()
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
cp5 = vjob.update_job_type_sub_type(jt, mylog)

# %% start date close date
tem = job[['job_externalid', 'STARTDATE']].dropna().rename(columns={'STARTDATE': 'start_date'})
tem['start_date'] = pd.to_datetime(tem['start_date'])
vjob.update_start_date(tem, mylog)

tem = job[['job_externalid', 'CLOSEBY']].dropna().rename(columns={'CLOSEBY': 'close_date'})
tem['close_date'] = pd.to_datetime(tem['close_date'])
vjob.update_close_date(tem, mylog)
#
# %% salary
sala = job[['job_externalid', 'FORAMT']].dropna()
sala['actual_salary'] = sala['FORAMT']
sala['actual_salary'] = sala['actual_salary'].astype(float)
cp6 = vjob.update_actual_salary(sala, mylog)

# %% on cost
tem = pd.read_sql(""" 
select OPID as job_externalid,trim(UOPCOMMENT) as UOPCOMMENT from OPMGR j
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
and com.COMPANY is not null and nullif (UOPCOMMENT,'') is not null
""", engine_mssql)
tem['on_cost'] = tem['UOPCOMMENT']
tem['on_cost'] = tem['on_cost'].astype(float)
cp6 = vjob.update_on_cost(tem, mylog)

# %% on cost percentage
tem = job[['job_externalid', 'UOPCOMMISI']].dropna()
tem['on_cost_percentage'] = tem['UOPCOMMISI']
tem['on_cost_percentage'] = tem['on_cost_percentage'].astype(float)
cp6 = vjob.update_on_cost_percentage(tem, mylog)

# %% order number
# tem = job[['job_externalid', 'Client_order_no']].dropna().drop_duplicates()
# tem['purchase_order'] = tem['Client_order_no']
# vjob.update_purchase_order(tem, mylog)

# %% quick fee forcast
qikfeefor = job[['job_externalid', 'FORPROB']].dropna()

qikfeefor['use_quick_fee_forecast'] = 1
qikfeefor['percentage_of_annual_salary'] = qikfeefor['FORPROB']
qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% note
note = pd.read_sql("""
select OPID as job_externalid, j.NAME, STAGE, nullif(F3,'') as Unit, nullif(convert(nvarchar(max),NOTES),'') as NOTES
 ,nullif(trim(UOPCOMMENT),'') as special_amount, UOPCOMMISI as special_com, nullif(trim(UOPPRESEAR),'') as special_descript
from OPMGR j
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
note = note.where(note.notnull(),None)
note['special_com'] = note['special_com'].apply(lambda x: str(x) if x else x)
note['note'] = note[['special_amount','special_com','special_descript','STAGE', 'Unit', 'NOTES']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Special Amount','Special Com','Special Descript','STAGE', 'UNIT', 'NOTES'], x) if e[1]]), axis=1)
note.loc[note['special_amount'].notnull()]

note = note.loc[note['note']!='']
vjob.update_note2(note, dest_db, mylog)


# %% reg date
# tem = pd.read_sql("""
# select
#        concat('', Agreement_Reference) as job_externalid
#      , Diary_Date
# from DB_Job_Order_Diary_Details j
# where Event_Description ='Vacancy registered                                '
# --and Diary_Text like '%Vacancy Added%'
# """, engine_mssql)
# tem = tem.drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['Diary_Date'])
# vjob.update_reg_date(tem, mylog)

# %% last activity date
# tem = pd.read_sql("""
# select concat('', Agreement_Reference) as job_externalid
#      , max(Created) as Created
# from DB_Job_Order_Diary_Details
# group by Agreement_Reference
# """, engine_mssql)
# tem = tem.drop_duplicates()
# tem['last_activity_date'] = pd.to_datetime(tem['Created'])
# vjob.update_last_activity_date(tem, mylog)

# %% industry
# sql = """
# select ac.idassignment as job_externalid, ac.codeid as idIndustry
# from assignmentcode ac
# where idtablemd = '6e748cd6-b1cd-4886-bd65-43a5acbb66a1'
# """
# job_industries = pd.read_sql(sql, engine_sqlite)
# job_industries = job_industries.dropna()
# job_industries['idIndustry'] = job_industries['idIndustry'].str.lower()
#
# industries = pd.read_sql("""
# select i1.idIndustry, i2.Value as ind, i1.Value as sind
# from Industry i1
# left join Industry i2 on i1.ParentId = i2.idIndustry
# """, engine_sqlite)
# industries['idIndustry'] = industries['idIndustry'].str.lower()
#
# industry_1 = job_industries.merge(industries, on='idIndustry')
# industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
# industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
#
# industries_csv = pd.read_csv('industries.csv')
# industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# j_industries = industry_1.merge(industries_csv, on='matcher')
#
# j_industries_2 = j_industries[['job_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
# j_industries_2 = j_industries_2.where(j_industries_2.notnull(),None)
#
# tem1 = j_industries_2[['job_externalid','Vincere Industry']].drop_duplicates().dropna()
# tem1['name'] = tem1['Vincere Industry']
# cp10 = vjob.insert_job_industry_subindustry(tem1, mylog, True)
#
# tem2 = j_industries_2[['job_externalid','Sub Industry']].drop_duplicates().dropna()
# tem2['name'] = tem2['Sub Industry']
# cp10 = vjob.insert_job_industry_subindustry(tem2, mylog, False)

