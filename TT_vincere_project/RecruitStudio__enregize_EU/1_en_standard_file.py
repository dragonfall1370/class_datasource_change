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
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
# %% data connections
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %% company
company = pd.read_sql("""
select CompanyId
     , nullif(CompanyName,'') as CompanyName
     , nullif(u.Email,'') as owner
from Companies c
left join Users u on c.Owner = u.UserName
""", engine_mssql)
company['CompanyId'] = 'EUK'+company['CompanyId']
company['CompanyName'] = company['CompanyName']+'_Energize-UK'
company['CompanyName'] = company['CompanyName'].fillna('Energize-UK')
contact = pd.read_sql("""
with cont_tmp as (
select ContactId
     , nullif(c.FirstName,'') as FirstName
     , nullif(c.LastName,'') as LastName
     , nullif(c.Email,'') as Email
     , nullif(u.Email,'') as owner
     , coalesce(com.CompanyId, com2.CompanyId) as companyId
    , ROW_NUMBER() OVER(PARTITION BY c.ContactId ORDER BY ContactId DESC) rn
from Contacts c
left join Users u on c.UserName = u.UserName
left join Companies com on com.CompanyId = c.CompanyId
left join Companies com2 on com2.CompanyName = c.Company
where  Descriptor = 1)
select * from cont_tmp where rn = 1
""", engine_mssql)
contact['ContactId'] = 'EUK'+contact['ContactId']
contact['companyId'] = contact['companyId'].apply(lambda x: 'EUK'+x if x else x)
contact['LastName'] = contact['LastName']+'_Energize-UK'
contact['LastName'] = contact['LastName'].fillna('Energize-UK')
job = pd.read_sql("""
with cont_tmp as (
select ContactId
     , coalesce(com.CompanyId, com2.CompanyId) as companyId
    , ROW_NUMBER() OVER(PARTITION BY c.ContactId ORDER BY ContactId DESC) rn
from Contacts c
left join Companies com on com.CompanyId = c.CompanyId
left join Companies com2 on com2.CompanyName = c.Company
where  Descriptor = 1)
select cont.ContactId
     , nullif(v.JobNumber,'') as JobNumber
     , nullif(v.JobTitle,'') as JobTitle
     , nullif(v.CompanyId,'') as CompanyId
     , nullif(u.Email,'') as owner
from Vacancies v
left join (select * from cont_tmp where rn = 1) cont on cont.ContactId = v.ContactId and cont.companyId = v.CompanyId
left join Users u on v.UserName = u.UserName
""", engine_mssql)
job['JobNumber'] = 'EUK'+job['JobNumber']
job['CompanyId'] = job['CompanyId'].apply(lambda x: 'EUK'+x if x else x)
job['ContactId'] = job['ContactId'].apply(lambda x: 'EUK'+x if x else x)
job['JobTitle'] = job['JobTitle']+'_Energize-UK'
job['JobTitle'] = job['JobTitle'].fillna('Energize-UK')
candidate = pd.read_sql("""
select ContactId
     , nullif(c.FirstName,'') as FirstName
     , nullif(c.LastName,'') as LastName
     , nullif(c.Email,'') as Email
     , nullif(u.Email,'') as owner
from Contacts c
left join Users u on c.UserName = u.UserName
where  Descriptor = 2 or Descriptor is null
""", engine_mssql)
candidate['ContactId'] = 'EUK'+candidate['ContactId']
candidate['LastName'] = candidate['LastName']+'_Energize-UK'
candidate['LastName'] = candidate['LastName'].fillna('Energize-UK')
# assert False
# %% transpose
company.rename(columns={
    'CompanyId': 'company-externalId',
    'CompanyName': 'company-name',
    'owner': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)
contact.rename(columns={
    'ContactId': 'contact-externalId',
    'companyId': 'contact-companyId',
    'FirstName': 'contact-firstName',
    'LastName': 'contact-lastName',
    'owner': 'contact-owners',
    'Email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)
job.rename(columns={
    'JobNumber': 'position-externalId',
    'CompanyId': 'position-companyId',
    'ContactId': 'position-contactId',
    'JobTitle': 'position-title',
    'owner': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)
candidate.rename(columns={
    'ContactId': 'candidate-externalId',
    'FirstName': 'candidate-firstName',
    'LastName': 'candidate-lastName',
    'Email': 'candidate-email',
    'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)
# %% to csv files
candidate.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)
job.to_csv(os.path.join(standard_file_upload, '5_job.csv'), index=False)
contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)
if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts.csv'), index=False)
tem = default_contacts.loc[default_contacts['contact-companyId'] == 'DEFAULT_COMPANY']
if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
elif len(tem):
    default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)