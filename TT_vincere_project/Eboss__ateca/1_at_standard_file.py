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
cf.read('at_config.ini')
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
# assert False
# %%
company = pd.read_sql("""
select concat('AT',company_id) as company_id, company_name, consultant
from client_contact
""", engine_mssql)
company = company.drop_duplicates()
company['consultant'].unique()
company.loc[(company['consultant'] == 'Richard Downes,'), 'owner'] = 'richard.downes@ateca.co.uk'
company.loc[(company['consultant'] == 'Jordan Cooke,'), 'owner'] = 'jordan.cooke@ateca.co.uk'
company.loc[(company['consultant'] == 'Steve Slater,'), 'owner'] = 'steve@ateca.co.uk'
company = company.where(company.notnull(),None)

contact = pd.read_sql("""
select concat('AT',contact_id) as contact_id,
       concat('AT',company_id) as company_id,
       contact_person,
       email,consultant
from client_contact
""", engine_mssql)
contact = contact.drop_duplicates()
contact['contact-lastName'] = contact['contact_person'].apply(lambda x: x.strip().split(' ')[-1])
contact['contact-firstName'] = [a.replace(b, '').strip() for a, b in zip(contact['contact_person'], contact['contact-lastName'])]
contact.loc[(contact['consultant'] == 'Richard Downes,'), 'owner'] = 'richard.downes@ateca.co.uk'
contact.loc[(contact['consultant'] == 'Jordan Cooke,'), 'owner'] = 'jordan.cooke@ateca.co.uk'
contact.loc[(contact['consultant'] == 'Steve Slater,'), 'owner'] = 'steve@ateca.co.uk'
contact = contact.where(contact.notnull(),None)

job = pd.read_sql("""
select concat('AT',JobID) as JobID
, Client
, concat('AT',contact_id) as contact_id
, concat('AT',company_id) as company_id 
, [Job Title]
from jobs j
left join (select company_id, company_name, max(contact_id) as contact_id
from client_contact
group by company_id, company_name) c on c.company_name = j.Client
""", engine_mssql)
job = job.drop_duplicates()

candidate = pd.read_sql("""
select concat('AT',[CD Number]) as candiadte_id
, [First Name]
, nullif(trim(Surname),'') as Surname
, nullif(trim(Email),'') as Email, consultant
 from ateca_cand
""", engine_mssql)
candidate = candidate.drop_duplicates()

tem1 = candidate.loc[candidate['Email'].isnull()]
tem2 = candidate.loc[candidate['Email'].notnull()]
tem3 = tem2.loc[~tem2['Email'].str.contains('@')]
tem3['Email'] = tem3['Email']+'@noemail.com'
tem4 = tem2.loc[tem2['Email'].str.contains('@')]
candidate = pd.concat([tem1,tem3,tem4])
candidate['consultant'].unique()
candidate.loc[(candidate['consultant'] == 'Richard Downes'), 'owner'] = 'richard.downes@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Jordan Cooke'), 'owner'] = 'jordan.cooke@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Steve Slater'), 'owner'] = 'steve@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Steve Slater,Callum Fairweather'), 'owner'] = 'steve@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Steve Slater,Richard Downes'), 'owner'] = 'steve@ateca.co.uk,richard.downes@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Steve Slater,Jordan Cooke'), 'owner'] = 'steve@ateca.co.uk,jordan.cooke@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Steve Slater,Wayne'), 'owner'] = 'steve@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Steve Slater,Zak Collins'), 'owner'] = 'steve@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Steve Slater,James Green'), 'owner'] = 'steve@ateca.co.uk'
candidate.loc[(candidate['consultant'] == 'Richard Downes,Callum Fairweather'), 'owner'] = 'richard.downes@ateca.co.uk'
candidate = candidate.where(candidate.notnull(),None)

assert False
# %% transpose
company.rename(columns={
    'company_id': 'company-externalId',
    'company_name': 'company-name',
    'owner': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'contact_id': 'contact-externalId',
    'company_id': 'contact-companyId',
    # 'LAST NAME': 'contact-lastName',
    # 'FIRST NAME': 'contact-firstName',
    # 'middlename': 'contact-middleName',
     'email': 'contact-email',
     'owner': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'JobID': 'position-externalId',
    'company_id': 'position-companyId',
    'contact_id': 'position-contactId',
    'Job Title': 'position-title',
    # 'Email_Address': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'candiadte_id': 'candidate-externalId',
    'First Name': 'candidate-firstName',
    'Surname': 'candidate-lastName',
    # 'MIDDLE NAME': 'candidate-middleName',
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
