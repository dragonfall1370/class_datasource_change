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
cf.read('lv_config.ini')
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
# assert False
# %% company
company = pd.read_sql("""
with com as (
select c.ID
     , c.Name
     , p.WorkEMail as owner
from Company c
left join Person p on c.LeadConsultantID = p.ID
where LeadConsultantID is not null
and c.IsArchived = 0
UNION
select c.ID
     , c.Name
     , p.WorkEMail as owner
from Company c
left join Person p on c.PermanentConsultantID = p.ID
where PermanentConsultantID is not null
and c.IsArchived = 0
UNION
select c.ID
     , c.Name
     , p.WorkEMail as owner
from Company c
left join Person p on c.ContractConsultantID = p.ID
where ContractConsultantID is not null
and c.IsArchived = 0
UNION
select c.ID
     , c.Name
     , p.WorkEMail as owner
from Company c
left join Person p on c.ContractConsultantID = p.ID
where ContractConsultantID is null
and PermanentConsultantID is null
and LeadConsultantID is null
and c.IsArchived = 0
    )
select
ID, Name, owner
from com
where Id not in (select c.ID as company_externalid
from Company c
join Drop_Down___Sectors rmi on rmi.ID= c.PrimarySectorWSIID
where rmi.Value = 'Hospitality')
""", engine_sqlite)
tem1 = company.loc[company['owner'].notnull()]
tem2 = company.loc[company['owner'].isnull()]
tem1_1 = tem1[['ID','owner']]
tem1_1 = tem1_1.groupby('ID')['owner'].apply(lambda x: ','.join(x)).reset_index()
tem1_2 = tem1[['ID','Name']].drop_duplicates()
tem1 = tem1_2.merge(tem1_1, on ='ID')
company = pd.concat([tem1,tem2])

contact = pd.read_sql("""
select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join (select * from CandidateEmployment where DateTo is null) com on p.ID = com.CandidateID
where c.IsActivated = 0
and p.IsActivated = 1
and p.IsArchived = 0
UNION
select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join (select * from CandidateEmployment where DateTo is null) com on p.ID = com.CandidateID
where c.IsActivated = 1
and p.IsActivated = 1
and p.IsArchived = 0
""", engine_sqlite)
contact.sort_values('DateFrom', inplace=True, ascending=False)
contact['rn'] = contact.groupby('ID').cumcount()
# contact.loc[contact['rn'] > 0]
# contact.loc[contact['ID'] == '19317']
contact = contact.loc[contact['rn'] == 0]

job = pd.read_sql("""
select j.Id
     , Title
     , j.CompanyID as company_externalid
     , cont.ID as cont_externalid
     , p.WorkEMail as owner
from Job j
left join Person p on j.ConsultantID = p.ID
left join (select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join (select * from CandidateEmployment where DateTo is null) com on p.ID = com.CandidateID
where c.IsActivated = 0
and p.IsActivated = 1
and p.IsArchived = 0
UNION
select p.ID
     , p.FirstName
     , p.Surname
     , p.WorkEMail
     , com.CompanyID
     , com.DateFrom
from Person p
join Candidate c on p.ID = c.ID
left join (select * from CandidateEmployment where DateTo is null) com on p.ID = com.CandidateID
where c.IsActivated = 1
and p.IsActivated = 1
and p.IsArchived = 0) cont on cont.ID = j.ContactID and cont.CompanyID = j.CompanyID""", engine_sqlite)

candidate = pd.read_sql("""
select p.ID
     , p.FirstName
     , p.Surname
     , p.MiddleName
     , c.PersonalEMail
     , o.WorkEMail as owner
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
where c.IsActivated = 1
and p.IsActivated = 0
and c.IsArchived = 0
UNION
select p.ID
     , p.FirstName
     , p.Surname
     , p.MiddleName
     , c.PersonalEMail
     , o.WorkEMail as owner
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
where c.IsActivated = 1
and p.IsActivated = 1
and c.IsArchived = 0
""", engine_sqlite)

# %% transpose
company.rename(columns={
    'ID': 'company-externalId',
    'Name': 'company-name',
    'owner': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'ID': 'contact-externalId',
    'CompanyID': 'contact-companyId',
    'FirstName': 'contact-firstName',
    'MiddleName': 'contact-middleName',
    'Surname': 'contact-lastName',
    'WorkEMail': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'ID': 'position-externalId',
    'company_externalid': 'position-companyId',
    'cont_externalid': 'position-contactId',
    'Title': 'position-title',
    'owner': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'ID': 'candidate-externalId',
    'FirstName': 'candidate-firstName',
    'MiddleName': 'candidate-middleName',
    'Surname': 'candidate-lastName',
    'PersonalEMail': 'candidate-email',
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