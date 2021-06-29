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
cf.read('sh_config.ini')
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
select CONCAT('',j.job_no) as job_externalid
     , serv_description
     , job_qty
     , job_salary
     , job_fee_percentage
     , job_salary_upper
     , nullif(job_quals,'') as job_quals
     , nullif(job_benefits,'') as job_benefits
     , job_branch
     , job_division
     , job_temp_perm
     , job_status
     , job_reg_date
     , job_start_date
     , job_finish_date
     , nullif(trim(job_desc),'') as duty
     , nullif(trim(job_notes),'') as notes
     , nullif(trim(job_experience),'') as experience
     , nullif(trim(job_hands),'') as health
from jobs j
left join job_services js on j.job_serv_no = js.serv_no
left join job_notes jn on j.job_no = jn.job_no
left join job_desc jd on j.job_no = jd.job_no
left join job_experience je on j.job_no = je.job_no
left join job_hands jh on j.job_no = jh.job_no
""", engine_mssql)
assert False
j_prod = pd.read_sql("""select id, name from position_description""", engine_postgre_review)
j_prod['name'] = j_prod['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, vjob.ddbconn, ['name'], ['id'], 'position_description', mylog)
# vjob.set_job_location_by_company_location(mylog)
# %% currency country
tem = job[['job_externalid']]
tem['currency_type'] = 'pound'
tem['country_code'] = 'GB'
vjob.update_currency_type(tem, mylog)
vjob.update_country_code(tem, mylog)

# %% location
vjob.set_job_location_by_company_location(mylog)

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
hc = job[['job_externalid', 'job_qty']].dropna().drop_duplicates()
hc['head_count'] = hc['job_qty']
hc['head_count'] = hc['job_qty'].astype(int)
cp1 = vjob.update_head_count(hc, mylog)

# %% job type
tem = job[['job_externalid', 'serv_description']].dropna().drop_duplicates()
tem['serv_description'].unique()
tem.loc[(tem['serv_description'] == 'PERM VACANCY'), 'job_type'] = 'permanent'
tem.loc[(tem['serv_description'] == 'DAY BOOKING'), 'job_type'] = 'temporary'
cp5 = vjob.update_job_type(tem, mylog)

# %% start date close date
tem = job[['job_externalid', 'job_start_date']].dropna().rename(columns={'job_start_date': 'start_date'})
tem['start_date'] = tem['start_date'].astype(str)
tem = tem.loc[tem['start_date']!='0']
tem['start_date_1'] = tem['start_date'].apply(lambda x: x[0:4] if x else x)
tem['start_date_2'] = tem['start_date'].apply(lambda x: x[4:6] if x else x)
tem['start_date_3'] = tem['start_date'].apply(lambda x: x[6:9] if x else x)
tem['start_date'] = tem['start_date_1'] +'-'+tem['start_date_2']+'-'+tem['start_date_3']
tem = tem.loc[tem['start_date_2']<'13']
tem['start_date'] = pd.to_datetime(tem['start_date'], format='%Y/%m/%d %H:%M:%S')
vjob.update_start_date(tem, mylog)

tem = job[['job_externalid', 'job_reg_date']].dropna().rename(columns={'job_reg_date': 'reg_date'})
tem['reg_date'] = tem['reg_date'].astype(str)
tem = tem.loc[tem['reg_date']!='0']
tem['reg_date_1'] = tem['reg_date'].apply(lambda x: x[0:4] if x else x)
tem['reg_date_2'] = tem['reg_date'].apply(lambda x: x[4:6] if x else x)
tem['reg_date_3'] = tem['reg_date'].apply(lambda x: x[6:9] if x else x)
tem['reg_date'] = tem['reg_date_1'] +'-'+tem['reg_date_2']+'-'+tem['reg_date_3']
tem['reg_date'] = pd.to_datetime(tem['reg_date'], format='%Y/%m/%d %H:%M:%S')
vjob.update_reg_date(tem, mylog)

tem = job[['job_externalid', 'job_finish_date']].dropna().rename(columns={'job_finish_date': 'close_date'})
tem['close_date'] = tem['close_date'].astype(str)
tem = tem.loc[tem['close_date']!='0']
tem['close_date_1'] = tem['close_date'].apply(lambda x: x[0:4] if x else x)
tem['close_date_2'] = tem['close_date'].apply(lambda x: x[4:6] if x else x)
tem['close_date_3'] = tem['close_date'].apply(lambda x: x[6:9] if x else x)
tem['close_date'] = tem['close_date_1'] +'-'+tem['close_date_2']+'-'+tem['close_date_3']
# tem = tem.loc[tem['close_date']<'13']
tem['close_date'] = pd.to_datetime(tem['close_date'], format='%Y/%m/%d %H:%M:%S')
vjob.update_close_date(tem, mylog)

# %% pay rate
# tem = job[['job_externalid', 'Pay_Rate']].dropna()
# tem['pay_rate'] = tem['Pay_Rate'].astype(float)
# vjob.update_pay_rate(tem, mylog)
#
# # %% charge rate
# tem = job[['job_externalid', 'Charge_Rate']].dropna()
# tem['charge_rate'] = tem['Charge_Rate'].astype(float)
# vjob.update_charge_rate(tem, mylog)

# %% annual salary
actsa = job[['job_externalid', 'job_salary']].dropna()
actsa['actual_salary'] = actsa['job_salary']
actsa['actual_salary'] = actsa['actual_salary'].astype(float)
cp7 = vjob.update_actual_salary(actsa, mylog)

# %% salary from/to
frsala = job[['job_externalid', 'job_salary']].dropna()
frsala['salary_from'] = frsala['job_salary']
frsala = frsala.loc[frsala['salary_from']!=0]
frsala['salary_from'] = frsala['salary_from'].astype(float)
cp8 = vjob.update_salary_from(frsala, mylog)
#
tosala = job[['job_externalid', 'job_salary_upper']].dropna()
tosala['salary_to'] = tosala['job_salary_upper']
tosala = tosala.loc[tosala['salary_to']!=0]
tosala['salary_to'] = tosala['salary_to'].astype(float)
cp9 = vjob.update_salary_to(tosala, mylog)

# %% quick fee forcast
qikfeefor = job[['job_externalid', 'job_fee_percentage']].dropna()
qikfeefor['use_quick_fee_forecast'] = 1
qikfeefor['percentage_of_annual_salary'] = qikfeefor['job_fee_percentage']
qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
qikfeefor = qikfeefor.loc[qikfeefor['percentage_of_annual_salary']!=0.0]
cp10 = vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
cp11 = vjob.update_percentage_of_annual_salary(qikfeefor, mylog)


# %% note
note = job[['job_externalid','job_quals','job_benefits','job_branch','job_division','job_temp_perm','duty','notes','experience','health']]
note['note'] = note[['job_externalid','job_quals','job_benefits','job_branch','job_division','job_temp_perm','duty','notes','experience','health']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Job I.D.','Quals','Benefits','Office','Sector','Both / Perm / Temp','Duties','Notes','Experience','Health and Safety'], x) if e[1]]), axis=1)

note = note.loc[note['notes']!='']
vjob.update_note2(note, dest_db, mylog)

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

# %% close date
# tem = pd.read_sql("""
# with min as (select Role_Id, min(Rate_Id) as Rate_Id from Booking_Role_Rate group by Role_Id)
# select br.Role_Id
#      , br.Status_Code as role_status
#      , s.Description
# from Booking_Role br
# join Booking b on br.Booking_Id = b.Booking_Id
# left join (select code, Description from Lookup where Table_Name in ('BOOKING_ROLE_STATUS')) s on br.Status_Code = s.Code
# where s.Description in ('Filled by other agency','Filled by Client','Cancelled','Placed')
# and br.Role_Id not in (
# 8547
# ,8467
# ,8696
# ,8473
# ,8490
# ,8678
# ,8710
# ,8849
# ,8508
# ,8626
# ,8627
# ,8655
# ,8656
# ,8669
# ,8708
# ,8514
# ,8544
# )
# """, engine_mssql)
# tem['job_externalid'] = 'BK'+tem['Role_Id'].astype(str)
# tem['close_date'] = datetime.datetime.now() - datetime.timedelta(days=1)
# vjob.update_close_date(tem, mylog)

 # %% status list
tem = job[['job_externalid', 'job_status']].dropna()
tem['name'] = tem['job_status']
tem1 = tem[['name']].drop_duplicates()
tem1['owner']  =''
vjob.create_status_list(tem1, mylog)
vjob.add_job_status(tem, mylog)

# %% delete
# tem = job[['job_externalid', 'Created_DTTM']].dropna().rename(columns={'Created_DTTM': 'start_date'})
# tem['start_date_1'] = tem['start_date'].apply(lambda x: x[0:10] if x else x)
# tem['start_date_2'] = tem['start_date'].apply(lambda x: x[11:19] if x else x)
# tem['start_date'] = tem['start_date_1'] +' '+tem['start_date_2']
# tem['start_date'] = tem['start_date'].apply(lambda x: x.replace('.',':') if x else x)
# tem['start_date'] = pd.to_datetime(tem['start_date'])
# tem['reg_date'] = tem['start_date']
#
# tem['today'] = datetime.datetime.now()
# tem['delta'] = tem['today'] - tem['reg_date']
# tem['delta'] = tem['delta'].apply(lambda x: x.days)
# tem = tem.loc[tem['delta']>2190]
# tem = tem.merge(vjob.job, on=['job_externalid'])
# tem['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vjob.ddbconn, ['deleted_timestamp', ], ['id', ], 'position_description', mylog)