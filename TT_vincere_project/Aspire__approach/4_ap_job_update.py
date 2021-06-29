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
cf.read('ap_config.ini')
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
select
       concat('', Agreement_Reference) as job_externalid
     , Title                           as JobTitle
     , nullif(trim(a.Job_Description),'') as Job_Description
     , nullif(trim(a.Job_model),'') as Job_model
     , a.Start_Date
     , a.End_Date
     , No_Required
     , Client_order_no
     , nullif(concat('', Company_Reference),'')   as company_externalid
     , nullif(trim(Salary_Details),'') as Salary_Details
     , nullif(trim(Job_Notes),'') as Job_Notes
     , nullif(trim(Job_Address),'') as Job_Address
     , nullif(trim(Email_Address),'') as Email_Address
     , nullif(trim(Main_Telephone),'') as Main_Telephone
from Agreement_view a
left join ds_job_basic_information_view jiv on jiv.Reference = a.Agreement_Reference
left join Job_Perm_Placement_View p on p.Reference = a.Agreement_Reference
""", engine_mssql)
assert False
j_prod = pd.read_sql("""select id, name from position_description""", engine_postgre_review)
j_prod['name'] = j_prod['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, vjob.ddbconn, ['name'], ['id'], 'position_description', mylog)
vjob.set_job_location_by_company_location(mylog)
# %% currency country
tem = job[['job_externalid']]
tem['currency_type'] = 'pound'
tem['country_code'] = 'GB'
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
tem = job[['job_externalid', 'Job_Description']].dropna().drop_duplicates()
tem['public_description'] = tem['Job_Description']
cp8 = vjob.update_public_description(tem, mylog)

# # %% rate_type
# employment_type = job[['job_externalid', 'FullPart']].dropna()
# employment_type['FullPart'].unique()
# employment_type.loc[(employment_type['FullPart'] == 'Full-time'), 'employment_type'] = 0
# employment_type.loc[(employment_type['FullPart'] == 'Part-time'), 'employment_type'] = 1
# tem = employment_type[['job_externalid', 'employment_type']].dropna()
# employment_type.loc[employment_type['employment_type'] == 1]
# vjob.update_employment_type(tem, mylog)

# %% head count
hc = job[['job_externalid', 'No_Required']].dropna().drop_duplicates()
hc['head_count'] = hc['No_Required']
# hc['head_count'] = hc['head_count'].astype(float)
hc['head_count'] = hc['head_count'].astype(int)
cp1 = vjob.update_head_count(hc, mylog)

# %% job type
jt = job[['job_externalid', 'Job_model']].dropna().drop_duplicates()
jt['Job_model'].unique()
jt.loc[jt['Job_model']=='Permanent', 'job_type'] = 'permanent'
# jt.loc[jt['Job_model']=='Contract', 'job_type'] = 'contract'
jt.loc[jt['Job_model']=='Temporary', 'job_type'] = 'contract'
jt.loc[jt['Job_model']=='Shift', 'job_type'] = 'contract'
jt2 = jt[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(jt2, mylog)

# %% start date close date
tem = job[['job_externalid', 'Start_Date']].dropna().rename(columns={'Start_Date': 'start_date'})
tem['start_date'] = pd.to_datetime(tem['start_date'])
vjob.update_start_date(tem, mylog)

tem = job[['job_externalid', 'End_Date']].dropna().rename(columns={'End_Date': 'close_date'})
tem['close_date'] = pd.to_datetime(tem['close_date'])
vjob.update_close_date(tem, mylog)
#
# # %% salary from/to
# frsala = job[['job_externalid', 'Salary']].dropna()
# frsala['salary_from'] = frsala['Salary']
# frsala['salary_from'] = frsala['salary_from'].astype(float)
# cp6 = vjob.update_salary_from(frsala, mylog)
#
# tosala = job[['job_externalid', 'Salary1']].dropna()
# tosala['salary_to'] = tosala['Salary1']
# tosala['salary_to'] = tosala['salary_to'].astype(float)
# cp7 = vjob.update_salary_to(tosala, mylog)


# %% order number
tem = job[['job_externalid', 'Client_order_no']].dropna().drop_duplicates()
tem['purchase_order'] = tem['Client_order_no']
vjob.update_purchase_order(tem, mylog)

# %% quick fee forcast
# qikfeefor = job[['job_externalid', 'Fee']].dropna()
#
# qikfeefor['use_quick_fee_forecast'] = 1
# qikfeefor['percentage_of_annual_salary'] = qikfeefor['EstimatedFee']
# qikfeefor['percentage_of_annual_salary'] = qikfeefor['percentage_of_annual_salary'].astype(float)
# vjob.update_use_quick_fee_forecast(qikfeefor, mylog)
# vjob.update_percentage_of_annual_salary(qikfeefor, mylog)

# %% note
ref = pd.read_sql("""
select concat('', Entity_Reference) as job_externalid, nullif(trim(Reference),'') as Reference from Reference_Codes where Entity_Type = 6""", engine_mssql)
ref = ref.dropna()
ref = ref.drop_duplicates()
ref = ref.groupby('job_externalid')['Reference'].apply(lambda x: ', '.join(x)).reset_index()
ref = ref.drop_duplicates()

rate = pd.read_sql("""
select concat('', jrv.Reference) as job_externalid
     , trim(Job_Code) as Job_Code
     , trim(jrv.Rate_Type) as Rate_Type
     , trim(jrv.Unit_Type) as Unit_Type
     , Unit_Type_Value
     , jrv.Pay_Rate
     , jrv.Charge_Rate
     , Pay_Rate_PAWR
     , Charge_Rate_PAWR
     , Pay_Rate_LTD
     , Charge_Rate_LTD
     , Pay_Rate_LAWR
     , Charge_Rate_LAWR
from JobRate_View jrv
left join Job_Rate jr on jr.Reference = jrv.Reference"""
, engine_mssql)
rate = rate.applymap(str)
rate['rate'] = rate[['Job_Code', 'Rate_Type', 'Unit_Type', 'Unit_Type_Value','Pay_Rate','Charge_Rate','Pay_Rate_PAWR','Charge_Rate_PAWR','Pay_Rate_LTD','Charge_Rate_LTD','Pay_Rate_LAWR','Charge_Rate_LAWR']]\
    .apply(lambda x: '   '.join([': '.join(e) for e in zip(['Job Code','Rate Type', 'Unit Type', 'Unit Type Value','Pay Rate','Charge Rate','Pay PAYE AWR','Charge PAYE AWR','Pay LTD','Charge LTD','Pay LTD AWR','Charge LTD AWR'], x) if e[1]]), axis=1)
rate = rate.drop_duplicates()
rate = rate.groupby('job_externalid')['rate'].apply(lambda x: '\n'.join(x)).reset_index()
rate = rate.drop_duplicates()
tem = rate.merge(ref, on='job_externalid', how='left')

tem = tem.applymap(str)
tem['note'] = tem[['Reference', 'rate']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Reference', 'Rate'], x) if e[1]]), axis=1)
tem = tem.loc[tem['note']!='']
vjob.update_note2(tem, dest_db, mylog)

# %% salary from
detail = pd.read_sql("""
with Further_detail AS (
Select b.Reference, a.Reference as Detail_Reference, Detail, Value, 'S' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from string_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 1
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'N' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Number_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 2
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, Convert(Char(10), Value, 103) as Value, 'D' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Date_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 3
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'M' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Money_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 4
-- and System = '0'

union

Select b.Reference, a.Reference as Detail_Reference, Detail, Case Value when 1 then 'True' else 'False' end as Value, 'B' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Boolean_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 5
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'F' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Float_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 8
-- and System = '0'
)
select concat('', Reference) as job_externalid
, nullif(trim(Detail),'') as Detail
, nullif(trim(Value),'') as Value
, nullif(trim(FactoidGroup),'') as FactoidGroup, System, Reference_Type
from Further_detail
where Detail = 'Salary from'
-- and System = '0'
""", engine_mssql)
frsala = detail[['job_externalid','Value']]
frsala['salary_from'] = frsala['Value']
frsala['salary_from'] = frsala['salary_from'].astype(float)
cp6 = vjob.update_salary_from(frsala, mylog)

# %% reg date
tem = pd.read_sql("""
select
       concat('', Agreement_Reference) as job_externalid
     , Diary_Date
from DB_Job_Order_Diary_Details j
where Event_Description ='Vacancy registered                                '
--and Diary_Text like '%Vacancy Added%'
""", engine_mssql)
tem = tem.drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['Diary_Date'])
vjob.update_reg_date(tem, mylog)

# %% last activity date
tem = pd.read_sql("""
select concat('', Agreement_Reference) as job_externalid
     , max(Created) as Created
from DB_Job_Order_Diary_Details
group by Agreement_Reference
""", engine_mssql)
tem = tem.drop_duplicates()
tem['last_activity_date'] = pd.to_datetime(tem['Created'])
vjob.update_last_activity_date(tem, mylog)

# %% industry
sql = """
select ac.idassignment as job_externalid, ac.codeid as idIndustry
from assignmentcode ac
where idtablemd = '6e748cd6-b1cd-4886-bd65-43a5acbb66a1'
"""
job_industries = pd.read_sql(sql, engine_sqlite)
job_industries = job_industries.dropna()
job_industries['idIndustry'] = job_industries['idIndustry'].str.lower()

industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()

industry_1 = job_industries.merge(industries, on='idIndustry')
industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
j_industries = industry_1.merge(industries_csv, on='matcher')

j_industries_2 = j_industries[['job_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
j_industries_2 = j_industries_2.where(j_industries_2.notnull(),None)

tem1 = j_industries_2[['job_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
cp10 = vjob.insert_job_industry_subindustry(tem1, mylog, True)

tem2 = j_industries_2[['job_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
cp10 = vjob.insert_job_industry_subindustry(tem2, mylog, False)

