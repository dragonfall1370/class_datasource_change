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
cf.read('tj_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_job
vjob = vincere_job.Job(connection)

# %%
job = pd.read_sql(""" 
select EnquiryID, DateTaken, ClosedDate, Closed, nullif(JobKeyWords,'') as JobKeyWords, es.Status from Enquiries e
left join EnquiryStatus es on e.Status = es.StatusID
""", engine_mssql)
job['job_externalid'] = 'FC'+job['EnquiryID'].astype(str)
assert False
j_prod = pd.read_sql("""select id, name from position_description""", engine_postgre_review)
j_prod['name'] = j_prod['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, vjob.ddbconn, ['name'], ['id'], 'position_description', mylog)
# vjob.set_job_location_by_company_location(mylog)

# %% currency country
tem = job[['job_externalid']]
tem['currency_type'] = 'myr'
tem['country_code'] = 'MY'
vjob.update_currency_type(tem, mylog)
vjob.update_country_code(tem, mylog)

# %% public_description
# tem = job[['job_externalid', 'Job_Description']].dropna().drop_duplicates()
# tem['public_description'] = tem['Job_Description']
# cp8 = vjob.update_public_description(tem, mylog)

# %% rate_type
# employment_type = job[['job_externalid', 'EMPLOYMENT TYPE']].dropna()
# employment_type['EMPLOYMENT TYPE'].unique()
# employment_type.loc[(employment_type['FullPart'] == 'Full-time'), 'employment_type'] = 0
# employment_type.loc[(employment_type['FullPart'] == 'Part-time'), 'employment_type'] = 1
# tem = employment_type[['job_externalid', 'employment_type']].dropna()
# employment_type.loc[employment_type['employment_type'] == 1]
# vjob.update_employment_type(tem, mylog)


# %% job type
tem = job[['job_externalid', 'Status']].dropna()
tem['Status'].unique()
tem.loc[(tem['Status'] == 'Select Temp'), 'job_type'] = 'contract'
tem.loc[(tem['Status'] == 'Admin Temp'), 'job_type'] = 'contract'
tem.loc[(tem['Status'] == 'Selection Permanent'), 'job_type'] = 'permanent'
tem.loc[(tem['Status'] == 'Tender Temp'), 'job_type'] = 'contract'
tem = tem[['job_externalid', 'job_type']].dropna()
cp5 = vjob.update_job_type(tem, mylog)

# %% start date close date reg date
tem = job[['job_externalid', 'DateTaken']].dropna().rename(columns={'DateTaken': 'start_date'})
tem['start_date'] = pd.to_datetime(tem['start_date'])
tem['reg_date'] = tem['start_date']
vjob.update_start_date(tem, mylog)
vjob.update_reg_date(tem, mylog)

tem = job[['job_externalid', 'ClosedDate']].dropna().rename(columns={'ClosedDate': 'close_date'})
tem['close_date'] = pd.to_datetime(tem['close_date'])
vjob.update_close_date(tem, mylog)

# %% note
note = job[['job_externalid']].dropna()
note['note'] = note['job_externalid'].apply(lambda x: x[2:])
note['note'] = 'Fircroft Enquiries ID: '+note['note']
vjob.update_note2(note, dest_db, mylog)

# %% skill no data
tem = job[['job_externalid','JobKeyWords']].dropna()
tem['key_words'] = tem['JobKeyWords']
vjob.update_key_words(tem, mylog)

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

# %% status
status = pd.read_sql(""" 
select vr.Role_Id as job_externalid, s.Description
from Vacancy_Role vr
left join Vacancy v on vr.Vacancy_Id = v.Vacancy_Id
left join (select code, Description from Lookup where Table_Name in ('VACANCY_ROLE_STATUS')) s on vr.Status_Code = s.Code
where s.Description in ('Advert Lead','Candidate Lead','Lost Fee','Hot Boss Lead')
""", engine_mssql)
status['job_externalid'] = 'VC'+status['job_externalid'].astype(str)
vjob.update_job_lead(status, mylog)

# %% close date
tem = pd.read_sql(""" 
select vr.Role_Id as job_externalid, s.Description
from Vacancy_Role vr
left join Vacancy v on vr.Vacancy_Id = v.Vacancy_Id
left join (select code, Description from Lookup where Table_Name in ('VACANCY_ROLE_STATUS')) s on vr.Status_Code = s.Code
where s.Description in ('Filled by other agency','Filled by Client','Cancelled','Placed')
""", engine_mssql)
tem['job_externalid'] = 'VC'+tem['job_externalid'].astype(str)
tem['close_date'] = datetime.datetime.now() - datetime.timedelta(days=1)
vjob.update_close_date(tem, mylog)

 # %% status list
tem = pd.read_sql(""" 
select vr.Role_Id as job_externalid, s.Description
from Vacancy_Role vr
left join Vacancy v on vr.Vacancy_Id = v.Vacancy_Id
left join (select code, Description from Lookup where Table_Name in ('VACANCY_ROLE_STATUS')) s on vr.Status_Code = s.Code
where s.Description in ('Filled by other agency','Filled by Client','Cancelled','Placed')
""", engine_mssql)
tem['job_externalid'] = 'VC'+tem['job_externalid'].astype(str)
tem.loc[tem['Description'] == 'Filled by other agency', 'name'] = 'Closed Filled by other Agency'
tem.loc[tem['Description'] == 'Filled by other Agency', 'name'] = 'Closed Filled by other Agency'
tem.loc[tem['Description'] == 'Filled by Client', 'name'] = 'Closed Filled by Client'
tem.loc[tem['Description'] == 'Cancelled', 'name'] = 'Closed - Cancelled'
tem.loc[tem['Description'] == 'Placed', 'name'] = 'Closed - Placed'
tem1 = tem[['name']]
tem1['owner']  =''
vjob.create_status_list(tem1, mylog)
vjob.add_job_status(tem, mylog)