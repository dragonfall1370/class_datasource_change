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
cf.read('exede_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% clean data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# assert False
# %% company
company = pd.read_sql("""
select
 c.COMPANY_ID as company_externalid
 , c.NAME as company_name
 , u.EmailAddress as company_owner
 , c.CREATED_ON as company_createddate
from Companies c 
left join [tblvwUser] u on c.CREATED_BY = u.Id
where c.DELETED = 0
and c.COMPANY_ID > 'HQ00014630'
;
""", engine_mssql)

contact = pd.read_sql("""
select 
cont.CONTACT_ID as contact_externalid
, com.company_externalid
, cont.FIRST_NAME as contact_firstname
, cont.LAST_NAME as contact_lastname
, cont.MIDDLE_NAME as contact_middlename
, cont.EMAIL as contact_email
, u.EmailAddress as contact_owner
, cont.CREATED_ON as contact_createddate
from Contacts cont
left join [tblvwUser] u on cont.CREATED_BY = u.Id
left join (
	select
		 c.COMPANY_ID as company_externalid
		from Companies c 
		where c.DELETED = 0
) com on cont.COMPANY_ID = com.company_externalid
where cont.DELETED = 0
AND cont.CONTACT_ID > 'HQ00078829'
;
""", engine_mssql)

job = pd.read_sql("""
select 
j.Id as job_externalid
, com.company_externalid
, cont.CONTACT_ID as contact_externalid
, j.JobTitle as job_title
, j.CreatedDateTime as job_createddate
, u.EmailAddress as job_owner
from Job j
left join (
	select
		 c.COMPANY_ID as company_externalid
		from Companies c 
		where c.DELETED = 0
) com on j.CompanyId = com.company_externalid
left join (
		select 
		cont.CONTACT_ID
		, cont.COMPANY_ID
		from Contacts cont
		where cont.DELETED = 0		
) cont on (j.ContactId = cont.CONTACT_ID and j.CompanyId = cont.COMPANY_ID)
left join [tblvwUser] u on j.CreatedUserId = u.Id
where j.Id > 'HQ00000477'
;
""", engine_mssql)

candidate = pd.read_sql("""
select
cand.APP_ID as candidate_externalid
, cand.FIRST_NAME as candidate_firstname
, cand.LAST_NAME as candidate_lastname
, cand.MIDDLE_NAME as candidate_middlename
, cand.EMAIL  as candidate_email
, u.EmailAddress as candidate_owner
from Applicants cand
left join [tblvwUser] u on cand.CREATED_BY = u.Id
where cand.DELETED = 0 and cand.APP_ID > 'HQ00004169'
""", engine_mssql)

# %% transpose
company.rename(columns={
    'company_externalid': 'company-externalId',
    'company_name': 'company-name',
    'company_owner': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'contact_externalid': 'contact-externalId',
    'company_externalid': 'contact-companyId',
    'contact_firstname': 'contact-firstName',
    'contact_middlename': 'contact-middleName',
    'contact_lastname': 'contact-lastName',
    'contact_owner': 'contact-owners',
    'contact_email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'job_externalid': 'position-externalId',
    'company_externalid': 'position-companyId',
    'contact_externalid': 'position-contactId',
    'job_title': 'position-title',
    'job_owner': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'candidate_externalid': 'candidate-externalId',
    'candidate_firstname': 'candidate-firstName',
    'candidate_middlename': 'candidate-middleName',
    'candidate_lastname': 'candidate-lastName',
    'candidate_email': 'candidate-email',
    'candidate_owner': 'candidate-owners',
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

contact.loc[contact['contact-externalId']=='HQ00000019']