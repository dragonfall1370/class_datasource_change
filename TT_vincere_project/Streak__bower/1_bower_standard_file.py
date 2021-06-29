# -*- coding: UTF-8 -*-
# import sys
# sys.path.append('D:\Tony\Working\DMvincere')
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
cf.read('bower_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
assert False
# %% company
company = pd.read_sql("""
select "Company ID"
     , Name
     , "Assigned To"
from Company
""", engine_sqlite)
company['matcher'] = company['Name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
company['Assigned To'] = company['Assigned To'].str.replace(' ','')

contact = pd.read_sql("""
select "Client List"
     , ID
     , Name
     , "First Name"
     , Email
     , "Assigned To"
from Contacts
""", engine_sqlite)
contact['matcher'] = contact['Client List'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact = contact.merge(company[['Company ID', 'matcher']], on='matcher', how='left')
contact['contact-lastName'] = [a.replace(b, '').strip() for a, b in zip(contact['Name'], contact['First Name'])]
contact['Assigned To'] = contact['Assigned To'].str.replace(' ','')

job = pd.read_sql("""
select j.ID
     , j."Job Title"
     , j."Assigned To"
     , j."Client List"
from Job j
""", engine_sqlite)
job['matcher'] = job['Client List'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont = pd.read_sql("""
select ID as contact_externalId, "Client List", Name from Contacts
group by "Client List"
order by Name ASC
""", engine_sqlite)
cont['matcher'] = cont['Client List'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cont = cont.merge(company[['Company ID', 'matcher']], on='matcher', how='left')
job = job.merge(cont[['Company ID', 'matcher','contact_externalId']], on='matcher', how='left')
job = job.merge(company[['Company ID', 'matcher']], on='matcher', how='left')
job.drop(['Company ID_x', 'matcher', 'Client List'], axis=1, inplace=True)
job['Assigned To'] = job['Assigned To'].str.replace(' ','')

candidate = pd.read_sql("""
select ID
     , Name
     , "First name"
     , "Assigned To"
     , Email 
     , "Last name"
from Candidate
""", engine_sqlite)
candidate['Assigned To'] = candidate['Assigned To'].str.replace(' ','')

# assert False
# %% transpose
company.rename(columns={
    'Company ID': 'company-externalId',
    'Name': 'company-name',
    'Assigned To': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'ID': 'contact-externalId',
    'Company ID': 'contact-companyId',
    'First Name': 'contact-firstName',
    'Assigned To': 'contact-owners',
    'Email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'ID': 'position-externalId',
    'Company ID_y': 'position-companyId',
    'contact_externalId': 'position-contactId',
    'Job Title': 'position-title',
    'Assigned To': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'ID': 'candidate-externalId',
    'First name': 'candidate-firstName',
    'Last name': 'candidate-lastName',
    'Email': 'candidate-email',
    'Assigned To': 'candidate-owners',
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