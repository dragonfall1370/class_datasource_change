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
cf.read('un_config.ini')
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

# %% contact
sql = """
select c.Id,
       c.FirstName,
       c.LastName,
       c.Email,
       c.AccountId as companyid,
       u.Email as owner
from Contact_new c
left join User_new u on c.OwnerId = u.Id
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
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

# %% job
# close date ts_date_filled
sql = """
SELECT
j.Id,
j.Name,
u.Email AS owner,
j.CreatedDate,
j.ts2__Contact__c AS contactId,
j.ts2__Account__c as companyId
FROM Job_new j
LEFT JOIN User_new u ON j.OwnerId = u.Id
"""
job = pd.read_sql(sql, engine_sqlite)
job = job.drop_duplicates()

job.rename(columns={
    'Id': 'position-externalId',
    'Name': 'position-title',
    'owner': 'position-owners',
    'contactId': 'position-contactId',
    'companyId': 'position-companyId',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

# %% candidate
sql = """
select
       c.Id,
       c.FirstName,
       c.LastName,
       c.Email,
       u.Email as owner
       --c.RecordTypeId
from Cand_new c
left join User_new u on c.OwnerId = u.Id
"""
candidate = pd.read_sql(sql, engine_sqlite)
candidate = candidate.drop_duplicates()
candidate.rename(columns={
    'Id': 'candidate-externalId',
    'FirstName': 'candidate-firstName',
    'LastName': 'candidate-lastName',
    'Email': 'candidate-email',
    'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% csv

job.to_csv(os.path.join(standard_file_upload, 'new_job.csv'), index=False)
contact.to_csv(os.path.join(standard_file_upload, 'new_contact.csv'), index=False)
candidate.to_csv(os.path.join(standard_file_upload, 'new_candidate.csv'), index=False)
if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, 'new_default_contacts.csv'), index=False)

tem = default_contacts.loc[default_contacts['contact-companyId'] == 'DEFAULT_COMPANY']
if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, 'new_default_company.csv'), index=False)
elif len(tem):
    default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
    default_company.to_csv(os.path.join(standard_file_upload, 'new_default_company.csv'), index=False)
