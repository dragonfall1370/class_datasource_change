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
cf.read('tt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
#dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connections
engine_sqlite = sqlalchemy.create_engine('sqlite:///thetalent.db', encoding='utf8')

# %% company
company = pd.read_sql("""
select
c.company_id as company_externalid
, c.name as company_name
, u.email as company_owner
from company c
left join user u on c.owner = u.user_id;
""", engine_sqlite)

contact = pd.read_sql("""
select c2.company_id as company_externalid
, c.contact_id as contact_externalid
, c.first_name as contact_firstname
, c.last_name as contact_lastname
, u.email as contact_owner
, c.email1 as contact_email
from contact c
left join user u on u.user_id = c.owner
left join company c2 on c2.company_id = c.company_id
;
""", engine_sqlite)

job = pd.read_sql("""
select j.joborder_id as job_externalid
, c2.company_id as company_externalid
, c.contact_id as contact_externalid
, u1.email as email1
, u2.email as email2
, u3.email as email3
, j.title as job_title
from joborder j
left join user u1 on u1.user_id =j.owner
left join user u2 on u2.user_id = j.recruiter
left join user u3 on u3.user_id = j.sourcer_id
left join contact c on c.company_id = j.company_id and c.contact_id = j.contact_id
left join company c2 on j.company_id = c2.company_id
;
""", engine_sqlite)
job1 = job[['job_externalid', 'email1', 'email2', 'email3']]
job1['job_owner'] = job1.drop('job_externalid', axis=1).apply(lambda x: ','.join(set(e for e in x if e)) ,axis=1)
job = job.merge(job1[['job_externalid', 'job_owner']], on='job_externalid', how='left')

candidate = pd.read_sql("""
select c.candidate_id as candidate_externalid
, u.email as candidate_owner
, c.first_name as candidate_firstname
, c.last_name as candidate_lastname
, c.middle_name as candidate_middlename
, c.email1 as candidate_email
from candidate c
left join user u on c.owner = u.user_id
""", engine_sqlite)
# assert False
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