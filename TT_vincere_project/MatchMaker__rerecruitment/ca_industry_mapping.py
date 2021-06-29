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
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %% candidate
company = pd.read_sql("""
select Client_Id, i.Description as name, Industry_Code
from Client c
left join Industry i on c.Industry_Id = i.Industry_Id
where i.Description is not null
and Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
company['company_externalid'] = company['Client_Id'].astype(str)
# assert False
from common import vincere_company
vcom = vincere_company.Company(connection)
company.loc[company['company_externalid']=='1757']
tem2.loc[tem2['company_externalid']=='1757']
df=company
tem2 = df[['company_externalid', 'name']].dropna().drop_duplicates()
tem2['matcher'] = tem2['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = tem2.merge(vcom.company, on=['company_externalid'])
tem2.rename(columns={'id':'company_id', }, inplace=True)
cols = ['industry_id', 'company_id', 'insert_timestamp', 'seq']
ver = pd.read_sql('select * from vertical', vcom.ddbconn)
ver['matcher'] = ver['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = tem2.merge(ver, on='matcher')
tem2.rename(columns={'id': 'industry_id', }, inplace=True)

tem2 = tem2.merge(pd.read_sql("select industry_id, company_id, 'existed' as note from company_industry", vcom.ddbconn), on=['industry_id', 'company_id'], how='left')
tem2 = tem2.loc[tem2['note'].isnull()]
tem2['seq'] = tem2.groupby('company_id').cumcount()
vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, vcom.ddbconn, cols, 'company_industry', mylog)


cp1 = vcom.insert_company_industry(company, mylog)

company_division = pd.read_sql("""
select Client_Id
 from Client c
left join Division d on c.Division_Id = d.Division_Id
where d.Description = 'GLA-Agri/Food (kept separate for reporting)'
""", engine_mssql)
company_division['company_externalid'] = company_division['Client_Id'].astype(str)
company_division['name'] = 'GLA'
# assert False
from common import vincere_company
vcom = vincere_company.Company(connection)
cp1 = vcom.insert_company_industry(company_division, mylog)

# %% job
job_v = pd.read_sql("""
select Role_Id, i.Description as name
from Vacancy v
left join Vacancy_Role vr on vr.Vacancy_Id = v.Vacancy_Id
left join Industry i on v.Industry_Id = i.Industry_Id
where Role_Id is not null and i.Description is not null
""", engine_mssql)
job_v['Role_Id'] = 'VC'+job_v['Role_Id'].astype(str)

job_b = pd.read_sql("""
select Role_Id, i.Description as name
from Booking b
left join Booking_Role br on br.Booking_Id = b.Booking_Id
left join Industry i on b.Industry_Id = i.Industry_Id
where Role_Id is not null and i.Description is not null
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
job_b['Role_Id'] = 'BK'+job_b['Role_Id'].astype(str)
# job_b['job_externalid'] = job_b['Role_Id']
job = pd.concat([job_v,job_b])
job['job_externalid'] = job['Role_Id']
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
cp2 = vjob.insert_job_industry_subindustry(job, mylog, True)

# %% contact
# skill = pd.read_csv('skill.csv')
# skill['matcher'] = skill['Gel Skill'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
ind = pd.read_csv('industry_mapping.csv', encoding = "ISO-8859-1")
ind['matcher'] = ind['Gel Skill'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont_skill = pd.read_sql("""
select Client_Contact_Id, Description
from Client_Contact_Skill cs
join Skill s on s.Skill_Id = cs.Skill_Id
where Description is not null
""", engine_mssql)
cont_skill['matcher'] = cont_skill['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont_skill = cont_skill.merge(ind, on='matcher')
cont_skill['contact_externalid'] = cont_skill['Client_Contact_Id'].astype(str)

from common import vincere_contact
vcont = vincere_contact.Contact(connection)
tem1 = cont_skill[['contact_externalid','Industry']].dropna()
tem1['name'] = tem1['Industry']
cp1 = vcont.insert_contact_industry_subindustry(tem1, mylog)


tem2 = cont_skill[['contact_externalid','Sub-Industry']].dropna()
tem2['name'] = tem2['Sub-Industry']
cp2 = vcont.insert_contact_industry_subindustry(tem2, mylog)


# %% candidate
# skill = pd.read_csv('skill.csv')
# skill['matcher'] = skill['Gel Skill'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
ind = pd.read_csv('industry_mapping.csv', encoding = "ISO-8859-1")
ind['matcher'] = ind['Gel Skill'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_skill = pd.read_sql("""
select Candidate_Id, Description
from Candidate_Skill cs
join Skill s on s.Skill_Id = cs.Skill_Id
where Description is not null
""", engine_mssql)
cand_skill['matcher'] = cand_skill['Description'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_skill = cand_skill.merge(ind, on='matcher')
cand_skill['candidate_externalid'] = cand_skill['Candidate_Id'].astype(str)

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
tem1 = cand_skill[['candidate_externalid','Industry']].dropna()
tem1['name'] = tem1['Industry']
cp3 = vcand.insert_candidate_industry_subindustry(tem1, mylog)

tem2 = cand_skill[['candidate_externalid','Sub-Industry']].dropna()
tem2['name'] = tem2['Sub-Industry']
cp4 = vcand.insert_candidate_industry_subindustry(tem2, mylog)

# %% candidate
candidate = pd.read_sql("""
select Candidate_Id as candidate_externalid
     , Temporary_YN
from Candidate c
""", engine_mssql)
candidate['candidate_externalid'] = candidate['candidate_externalid'].astype(str)
candidate = candidate.loc[candidate['Temporary_YN']=='Y']
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
candidate['name'] = 'Temp Compliance'
cp3 = vcand.insert_candidate_industry_subindustry(candidate, mylog)