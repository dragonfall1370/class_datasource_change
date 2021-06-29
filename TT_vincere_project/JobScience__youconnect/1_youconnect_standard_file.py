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
cf.read('yc_config.ini')
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

# %% company
sql = """
SELECT
	  a.Id,
      a.Name,
      --a.BillingStreet, a.BillingState, a.BillingCity, a.BillingPostalCode, a.BillingCountry,
      u.Email as owner
FROM Account a
LEFT JOIN User u ON a.OwnerId = u.Id
WHERE a.IsDeleted = 0;
"""
company = pd.read_sql(sql, engine_sqlite)
# company['billing_address'] = company[['BillingStreet', 'BillingState', 'BillingCity', 'BillingPostalCode', 'BillingCountry']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company.rename(columns={
    'Id': 'company-externalId',
    'Name': 'company-name',
    'owner': 'company-owners',
    # 'billing_address': 'company-locationAddress',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

# %% contact
sql = """
select
       c.Id,
       c.FirstName,
       c.LastName,
       c.Email,
       com.Id as companyid,
       u.Email as owner
       --c.RecordTypeId
from Contact c
join RecordType r on (c.RecordTypeId || 'AA2') = r.Id
left join Account com on (c.AccountId = com.Id and com.IsDeleted=0)
left join User u on c.OwnerId = u.Id
where c.IsDeleted = 0
and r.Name = 'Contact'
"""
contact = pd.read_sql(sql, engine_sqlite)
contact.rename(columns={
    'Id': 'contact-externalId',
    'FirstName': 'contact-firstName',
    'LastName': 'contact-lastName',
    'Email': 'contact-email',
    'companyid': 'contact-companyId',
    'owner': 'contact-owners',
}, inplace=True)
contact = vincere_standard_migration.process_vincere_contact(contact)
contact['contact-companyId'].fillna('DEFAULT_COMPANY', inplace=True)

contact.loc[contact['contact-companyId'].isnull()]
default_company = contact.loc[contact['contact-companyId'] == 'DEFAULT_COMPANY'][['contact-companyId']].drop_duplicates()
default_company.rename(columns={'contact-companyId': 'company-externalId'}, inplace=True)

# %% job
# close date ts_date_filled
sql = """
SELECT
j.Id,
j.Name,
u.Email AS owner,
j.ts2__Account__c AS companyId,
cont.contactid AS contactId
FROM ts2__Job__c j
LEFT JOIN (
    SELECT
    c.Id as contactid,
    c.AccountId as companyid
    FROM Contact c
    JOIN RecordType r ON (c.RecordTypeId || 'AA2') = r.Id
    WHERE c.IsDeleted = 0 AND r.Name = 'Contact') cont on (j.ts2__Account__c = cont.companyid and j.ts2__Contact__c = cont.contactid)
LEFT JOIN USER u ON j.OwnerId = u.Id
WHERE j.IsDeleted = 0;
"""
job = pd.read_sql(sql, engine_sqlite)
job.rename(columns={
    'Id': 'position-externalId',
    'Name': 'position-title',
    'owner': 'position-owners',
    'contactId': 'position-contactId',
    'companyId': 'position-companyId',
}, inplace=True)
job['position-companyId'].fillna('DEFAULT_COMPANY', inplace=True)
# generate default contact external id base on company id (so that each company has max 1 default contact)
job.loc[job['position-contactId'].isnull(), 'position-contactId'] = 'DEFAULT_CONTACT' + job.loc[job['position-contactId'].isnull(), 'position-companyId']
job.loc[job['position-contactId']=='DEFAULT_CONTACT']
job.loc[job['position-companyId']=='DEFAULT_COMPANY']
job = vincere_standard_migration.process_vincere_job(job, mylog)
default_contacts = job.loc[job['position-contactId'].str.contains('DEFAULT_CONTACT')][['position-contactId', 'position-companyId']].drop_duplicates()
default_contacts.rename(columns={
    'position-contactId': 'contact-externalId',
    'position-companyId': 'contact-companyId',
}, inplace=True)
# check microsoft case
default_contacts.loc[default_contacts['contact-companyId']=='0012400000K6s6iAAB']

# %% candidate
sql = """
select
       c.Id,
       c.FirstName,
       c.LastName,
       c.Email,
       c.Title,
       c.ts2__EmployerOrgName_1__c,
       u.Email as owner
       --c.MailingStreet, c.MailingState, c.MailingCity, c.MailingPostalCode, c.MailingCountry
from Contact c
join RecordType r on (c.RecordTypeId || 'AA2') = r.Id
left join User u on c.OwnerId = u.Id
where c.IsDeleted = 0
and r.Name = 'Candidate'
"""
candidate = pd.read_sql(sql, engine_sqlite)
# candidate['mailing_address'] = candidate[['MailingStreet', 'MailingState', 'MailingCity', 'MailingPostalCode', 'MailingCountry']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate.rename(columns={
    'Id': 'candidate-externalId',
    'FirstName': 'candidate-firstName',
    'LastName': 'candidate-lastName',
    'Email': 'candidate-email',
    'ts2__EmployerOrgName_1__c': 'candidate-company1',
    'Title': 'candidate-jobTitle1',
    'owner': 'candidate-owners',
    # 'mailing_address': 'candidate-address',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% csv
default_company['company-name'] = 'DEFAULT COMPANY'
company = pd.concat([default_company, company], sort=True)
company.to_csv(os.path.join(standard_file_upload, '1_company.csv'), index=False)

default_contacts['contact-firstName'] = 'DEFAULT'
default_contacts['contact-lastName'] = 'CONTACT'
# default_contacts['rn'] = default_contacts.groupby('contact-externalId').cumcount()
default_contacts['rn'] = default_contacts.reset_index().index
default_contacts['rn'] = default_contacts['rn'].astype(str)
default_contacts['contact-email'] = 'default_email_' + default_contacts['rn'] + '@vincere.io'
default_contacts.drop('rn', axis=1, inplace=True)
contact = pd.concat([default_contacts, contact], sort=True)
contact.to_csv(os.path.join(standard_file_upload, '2_contact.csv'), index=False)

job.to_csv(os.path.join(standard_file_upload, '3_job.csv'), index=False)

candidate.to_csv(os.path.join(standard_file_upload, '4_candidate.csv'), index=False)
