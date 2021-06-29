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
cf.read('ca_config.ini')
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
with min as (select Role_Id, min(Rate_Id) as Rate_Id from Booking_Role_Rate group by Role_Id)
select b.Booking_Code
     , b.Notes
     , b.Status_Code
     , br.Role_Id
     , br.Description
     , Role_Code
     , br.Status_Code as role_status
     , Rate_Group
     , rate_type
     , brr.Description as rate_des
     , Pay_Rate
     , Charge_Rate
     , Hours_Per_Unit
     , br.Notes as role_notes, br.Created_DTTM
from Booking_Role br
join Booking b on br.Booking_Id = b.Booking_Id
left join (
select brrate.*, rt.Description as rate_type
from Booking_Role_Rate brrate
join min m on m.Rate_Id = brrate.Rate_Id
left join Rate_Type rt on rt.Type_Id = brrate.Type_Id) brr on br.Role_Id = brr.Role_Id
where br.Role_Id not in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544
)
""", engine_mssql)
job['job_externalid'] = 'BK'+job['Role_Id'].astype(str)
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
addr = pd.read_sql(""" 
select br.Role_Id as job_externalid, Role_Code, a.*
from Booking_Role br
join Booking b on br.Booking_Id = b.Booking_Id
left join (
select Client_Id, Client_Address_Id, addr.* from Client_Address ca
join (select Address_Id, Line_1, Line_2, Line_3, c.Description as county, co.Description as country, t.Description as town, Postcode
from Address a
left join County c on a.County_Id = c.County_Id
left join Country co on a.Country_Id = co.Country_Id
left join Town t on a.Town_Id = t.Town_Id) addr on addr.Address_Id = ca.Address_Id) a on br.Site_Address_Id = a.Client_Address_Id and  b.Client_Id = a.Client_Id
where br.Role_Id not in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544
)
""", engine_mssql)
addr['job_externalid'] = 'BK'+addr['job_externalid'].astype(str)
addr['company_externalid'] = addr['Client_Id'].apply(lambda x: str(x) if x else x)
addr['address'] = addr[['Line_1', 'Line_2','Line_3','town','county','Postcode','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem2 = addr[['job_externalid', 'company_externalid', 'address']].drop_duplicates()
vjob.map_job_location_by_company_location(tem2, mylog)

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
tem = job[['job_externalid']]
tem['job_type'] = 'contract'
cp5 = vjob.update_job_type(tem, mylog)

# %% start date close date
tem = job[['job_externalid', 'Created_DTTM']].dropna().rename(columns={'Created_DTTM': 'start_date'})
tem['start_date_1'] = tem['start_date'].apply(lambda x: x[0:10] if x else x)
tem['start_date_2'] = tem['start_date'].apply(lambda x: x[11:19] if x else x)
tem['start_date'] = tem['start_date_1'] +' '+tem['start_date_2']
tem['start_date'] = tem['start_date'].apply(lambda x: x.replace('.',':') if x else x)
tem['start_date'] = pd.to_datetime(tem['start_date'])
tem['reg_date'] = tem['start_date']
vjob.update_start_date(tem, mylog)
vjob.update_reg_date(tem, mylog)

# tem = job[['job_externalid', 'CLOSEBY']].dropna().rename(columns={'CLOSEBY': 'close_date'})
# tem['close_date'] = pd.to_datetime(tem['close_date'])
# vjob.update_close_date(tem, mylog)

# %% pay rate
tem = job[['job_externalid', 'Pay_Rate']].dropna()
tem['pay_rate'] = tem['Pay_Rate'].astype(float)
vjob.update_pay_rate(tem, mylog)

# %% charge rate
tem = job[['job_externalid', 'Charge_Rate']].dropna()
tem['charge_rate'] = tem['Charge_Rate'].astype(float)
vjob.update_charge_rate(tem, mylog)

# %% note
note = job[['job_externalid','Notes','Status_Code','role_status','Rate_Group','rate_type','rate_des','role_notes','Hours_Per_Unit']]
date = pd.read_sql(""" 
select Role_Id, max(Role_DT) as end_date, min(Role_DT) as start_date
from Booking_Role_Day
where Role_Id not in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544
)
group by Role_Id
""", engine_mssql)
date['job_externalid'] = 'BK'+date['Role_Id'].astype(str)
date['end_date'] = date['end_date'].apply(lambda x: x[0:10] if x else x)
date['start_date'] = date['start_date'].apply(lambda x: x[0:10] if x else x)


tem = note.merge(date, on='job_externalid', how='left')

tem['rate_type'].unique()
tem.loc[(tem['rate_type'] == 'Hourly Rate'), 'WTR'] = 'Yes'
tem.loc[(tem['rate_type'] == 'Hourly Rate'), 'WTR2'] = 'No'
tem.loc[(tem['rate_type'] == 'Bonus Payment'), 'WTR'] = 'No'
tem.loc[(tem['rate_type'] == 'Bonus Payment'), 'WTR2'] = 'No'
tem.loc[(tem['rate_type'] == 'Holiday Pay'), 'WTR'] = 'No'
tem.loc[(tem['rate_type'] == 'Holiday Pay'), 'WTR2'] = 'No'
tem.loc[(tem['rate_type'] == 'Charge Rate'), 'WTR'] = 'No'
tem.loc[(tem['rate_type'] == 'Charge Rate'), 'WTR2'] = 'No'
tem.loc[(tem['rate_type'] == 'ZZ  Pay'), 'WTR'] = 'No'
tem.loc[(tem['rate_type'] == 'ZZ  Pay'), 'WTR2'] = 'No'
tem.loc[(tem['rate_type'] == 'ZZhourly Rate'), 'WTR'] = 'No'
tem.loc[(tem['rate_type'] == 'ZZhourly Rate'), 'WTR2'] = 'No'
tem.loc[(tem['rate_type'] == 'NO WTR Rate'), 'WTR'] = 'No'
tem.loc[(tem['rate_type'] == 'NO WTR Rate'), 'WTR2'] = 'No'
tem.loc[(tem['rate_type'] == 'Overtime Rate'), 'WTR'] = 'No'
tem.loc[(tem['rate_type'] == 'Overtime Rate'), 'WTR2'] = 'No'
tem = tem.where(tem.notnull(),None)
tem['note'] = tem[['Notes','Status_Code','start_date','end_date','role_status','Rate_Group','rate_type','rate_des','role_notes','Hours_Per_Unit','WTR','WTR2']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Vacancy Notes','Vacancy Status','Role Code','Start Date','End Date','Role Status','Rate Group','Rate Type','Rate Description','Role Notes','Hours Per Unit','WTR','WTR 2'], x) if e[1]]), axis=1)

tem = tem.loc[tem['note']!='']
vjob.update_note2(tem, dest_db, mylog)

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

# %% status
status = pd.read_sql(""" 
with min as (select Role_Id, min(Rate_Id) as Rate_Id from Booking_Role_Rate group by Role_Id)
select br.Role_Id
     , br.Status_Code as role_status
     , s.Description
from Booking_Role br
join Booking b on br.Booking_Id = b.Booking_Id
left join (select code, Description from Lookup where Table_Name in ('BOOKING_ROLE_STATUS')) s on br.Status_Code = s.Code
where s.Description in ('Advert Lead','Candidate Lead','Lost Fee','Hot Boss Lead')
and br.Role_Id not in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544
)
""", engine_mssql)
status['job_externalid'] = 'BK'+status['Role_Id'].astype(str)
vjob.update_job_lead(status, mylog)

# %% close date
tem = pd.read_sql(""" 
with min as (select Role_Id, min(Rate_Id) as Rate_Id from Booking_Role_Rate group by Role_Id)
select br.Role_Id
     , br.Status_Code as role_status
     , s.Description
from Booking_Role br
join Booking b on br.Booking_Id = b.Booking_Id
left join (select code, Description from Lookup where Table_Name in ('BOOKING_ROLE_STATUS')) s on br.Status_Code = s.Code
where s.Description in ('Filled by other agency','Filled by Client','Cancelled','Placed')
and br.Role_Id not in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544
)
""", engine_mssql)
tem['job_externalid'] = 'BK'+tem['Role_Id'].astype(str)
tem['close_date'] = datetime.datetime.now() - datetime.timedelta(days=1)
vjob.update_close_date(tem, mylog)

 # %% status list
tem = pd.read_sql(""" 
with min as (select Role_Id, min(Rate_Id) as Rate_Id from Booking_Role_Rate group by Role_Id)
select br.Role_Id
     , br.Status_Code as role_status
     , s.Description
from Booking_Role br
join Booking b on br.Booking_Id = b.Booking_Id
left join (select code, Description from Lookup where Table_Name in ('BOOKING_ROLE_STATUS')) s on br.Status_Code = s.Code
where s.Description in ('Filled by other agency','Filled by Client','Cancelled','Placed','Advert Lead','Candidate Lead','Lost Fee','Hot Boss Lead')
and br.Role_Id not in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544
)
""", engine_mssql)
tem['job_externalid'] = 'BK'+tem['Role_Id'].astype(str)
tem.loc[tem['Description'] == 'Filled by other agency', 'name'] = 'Closed Filled by other Agency'
tem.loc[tem['Description'] == 'Filled by Client', 'name'] = 'Closed Filled by Client'
tem.loc[tem['Description'] == 'Cancelled', 'name'] = 'Closed - Cancelled'
tem.loc[tem['Description'] == 'Placed', 'name'] = 'Closed - Placed'
tem.loc[tem['Description'] == 'Advert Lead', 'name'] = 'Advert Lead'
tem.loc[tem['Description'] == 'Candidate Lead', 'name'] = 'Candidate Lead'
tem.loc[tem['Description'] == 'Lost Fee', 'name'] = 'Lost Fee'
tem.loc[tem['Description'] == 'Hot Boss Lead', 'name'] = 'Hot Boss Lead'
tem1 = tem[['name']]
tem1['owner']  =''
vjob.create_status_list(tem1, mylog)
vjob.add_job_status(tem, mylog)

# %% delete
tem = job[['job_externalid', 'Created_DTTM']].dropna().rename(columns={'Created_DTTM': 'start_date'})
tem['start_date_1'] = tem['start_date'].apply(lambda x: x[0:10] if x else x)
tem['start_date_2'] = tem['start_date'].apply(lambda x: x[11:19] if x else x)
tem['start_date'] = tem['start_date_1'] +' '+tem['start_date_2']
tem['start_date'] = tem['start_date'].apply(lambda x: x.replace('.',':') if x else x)
tem['start_date'] = pd.to_datetime(tem['start_date'])
tem['reg_date'] = tem['start_date']

tem['today'] = datetime.datetime.now()
tem['delta'] = tem['today'] - tem['reg_date']
tem['delta'] = tem['delta'].apply(lambda x: x.days)
tem = tem.loc[tem['delta']>2190]
tem = tem.merge(vjob.job, on=['job_externalid'])
tem['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vjob.ddbconn, ['deleted_timestamp', ], ['id', ], 'position_description', mylog)