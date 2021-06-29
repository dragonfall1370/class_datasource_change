# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
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
cf.read('rr_config.ini')
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
user = pd.read_csv('user.csv')
user['matcher'] = user['UserId'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# %% data connections
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %% production
company = pd.read_sql("""
select concat('RSS',[Record ID]) as company_id
     , nullif([Company Name],'') as company_name
     , nullif([Managing Consultant],'') as consultants
from Clients
""", engine_mssql)
company['matcher'] = company['consultants'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
company = company.merge(user, on='matcher', how='left')
company = company.where(company.notnull(), None)

contact = pd.read_sql("""
select concat('RSS',[Record ID]) as contact_id
     , nullif(concat('RSS',[Client ID]),'') as company_id
     , nullif(Forenames,'') as Forenames 
     , nullif(Surname,'') as Surname 
     , nullif(Email,'') as Email
from Contacts
""", engine_mssql)

job = pd.read_sql("""
select concat('RSS',v.[Record ID]) as job_id
     , nullif(v.[Job Title],'') as job_title
     , nullif(concat('RSS',v.[Client ID]),'') as company_id
--      , nullif(concat('',_Contact_ID_),'') as contact_id
--    , nullif(concat('RSS',c._Record_ID_),'') as contact_id
     , c.[Record ID] as contact_id
     , nullif([Managing Consultant],'') as consultants
from Vacancies v
left join Contacts c on v.[Contact ID] = c.[Record ID] and v.[Client ID] = c.[Client ID]
""", engine_mssql)
job['matcher'] = job['consultants'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
job = job.merge(user, on='matcher', how='left')
job = job.where(job.notnull(), None)
job['contact_id'] = job['contact_id'].apply(lambda x: 'RSS'+str(x) if x else x)

candidate = pd.read_sql("""
select concat('RSS',[Record ID]) as candidate_id
     , nullif(Forenames,'') as Forenames
     , nullif(Surname,'') as Surname
     , nullif(Email,'') as Email
     , nullif([Managing Consultant],'') as consultants
from Candidates
""", engine_mssql) #59359
candidate['matcher'] = candidate['consultants'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
candidate = candidate.merge(user, on='matcher', how='left')
candidate = candidate.where(candidate.notnull(), None)
assert False
# %% transpose
company.rename(columns={
    'company_id': 'company-externalId',
    'company_name': 'company-name',
    'UserEmail': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'contact_id': 'contact-externalId',
    'company_id': 'contact-companyId',
    'Surname': 'contact-lastName',
    'Forenames': 'contact-firstName',
    # 'peo_middlename': 'contact-middleName',
     'Email': 'contact-email',
     # 'con_email': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'job_id': 'position-externalId',
    'company_id': 'position-companyId',
    'contact_id': 'position-contactId',
    'job_title': 'position-title',
    'UserEmail': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'candidate_id': 'candidate-externalId',
    'Forenames': 'candidate-firstName',
    'Surname': 'candidate-lastName',
    # 'peo_middlename': 'candidate-middleName',
    'Email': 'candidate-email',
    'UserEmail': 'candidate-owners',
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