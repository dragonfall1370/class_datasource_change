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
cf.read('ca_config.ini')
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
user['matcher'] = user['Email in Gel'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# %% data connections
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# assert False
# %% production
company = pd.read_sql("""
select Client_Id, Client_Name from Client
""", engine_mssql)

contact = pd.read_sql("""
select Client_Contact_Id, First_Name, Last_Name, Email_Address,Client_Id from Client_Contact cc
left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
""", engine_mssql)

job = pd.read_sql("""
select Role_Id, vr.Description, v.Client_Id, cc.Client_Contact_Id, Email_Address
from Vacancy v
left join Vacancy_Role vr on vr.Vacancy_Id = v.Vacancy_Id
left join Client_Contact cc on v.Client_Id = cc.Client_Id and v.Main_Contact_Id = cc.Client_Contact_Id
left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = v.Consultant_Id
where Role_Id is not null
""", engine_mssql)
job['Role_Id'] = 'VC'+job['Role_Id'].astype(str)
job['matcher'] = job['Email_Address'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
job = job.merge(user, on='matcher', how='left')

job_b = pd.read_sql("""
select Role_Id, br.Description, b.Client_Id, cc.Client_Contact_Id, Email_Address,b.Booking_Id,b.Booking_Code
from Booking b
left join Booking_Role br on br.Booking_Id = b.Booking_Id
left join Client_Contact cc on b.Client_Id = cc.Client_Id and b.Main_Contact_Id = cc.Client_Contact_Id
left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = b.Consultant_Id
where Role_Id is not null
""", engine_mssql)
job_b['Role_Id'] = 'BK'+job_b['Role_Id'].astype(str)
job_b['matcher'] = job_b['Email_Address'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
job_b = job_b.merge(user, on='matcher', how='left')

candidate = pd.read_sql("""
select Candidate_Id, Candidate_Code, First_Name, Last_Name ,e.Email_Address as email, o.Email_Address as owner
from Candidate c
join (
select Person_Id, Email_Address, First_Name ,Last_Name
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id
left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = c.Consultant_Id
""", engine_mssql) #59359
candidate['matcher'] = candidate['owner'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
candidate = candidate.merge(user, on='matcher', how='left')

# %% pay & bill
com_load = pd.read_sql("""
select Client_Id, Client_Name from Client
where Client_Id in (
select distinct b.Client_Id
from Booking b
left join Booking_Role br on br.Booking_Id = b.Booking_Id
where Role_Id in (
select Booking_Role_Id from Placement where Placement_Code in ('PL41909'
,'PL41910'
,'PL44078'
,'PL43810'
,'PL44075'
,'PL 43186'
,'PL 42798'
,'PL41961'
,'PL44292'
,'PL41958'
,'PL42798'
,'PL42816'
,'PL42826'
,'PL42960'
,'PL43049'
,'PL43343'
,'PL43100'
,'PL43186'
,'PL43641'
)))
""", engine_mssql)

contact_load = pd.read_sql("""
select Client_Contact_Id, First_Name, Last_Name, Email_Address,Client_Id from Client_Contact cc
left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
where Client_Contact_Id in (
select distinct cc.Client_Contact_Id
from Booking b
left join Booking_Role br on br.Booking_Id = b.Booking_Id
left join Client_Contact cc on b.Client_Id = cc.Client_Id and b.Main_Contact_Id = cc.Client_Contact_Id
where Role_Id in (
select Booking_Role_Id from Placement where Placement_Code in ('PL41909'
,'PL41910'
,'PL44078'
,'PL43810'
,'PL44075'
,'PL 43186'
,'PL 42798'
,'PL41961'
,'PL44292'
,'PL41958'
,'PL42798'
,'PL42816'
,'PL42826'
,'PL42960'
,'PL43049'
,'PL43343'
,'PL43100'
,'PL43186'
,'PL43641'
)))
""", engine_mssql)

job_load = pd.read_sql("""
select Role_Id, br.Description, b.Client_Id, cc.Client_Contact_Id, Email_Address,b.Booking_Id,b.Booking_Code
from Booking b
left join Booking_Role br on br.Booking_Id = b.Booking_Id
left join Client_Contact cc on b.Client_Id = cc.Client_Id and b.Main_Contact_Id = cc.Client_Contact_Id
left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = b.Consultant_Id
where Role_Id in (
select Booking_Role_Id from Placement where Placement_Code in ('PL41909'
,'PL41910'
,'PL44078'
,'PL43810'
,'PL44075'
,'PL 43186'
,'PL 42798'
,'PL41961'
,'PL44292'
,'PL41958'
,'PL42798'
,'PL42816'
,'PL42826'
,'PL42960'
,'PL43049'
,'PL43343'
,'PL43100'
,'PL43186'
,'PL43641'
))
""", engine_mssql)
job_load['Role_Id'] = 'BK'+job_load['Role_Id'].astype(str)

candidate_load = pd.read_sql("""
select c.Candidate_Id, Candidate_Code, First_Name, Last_Name ,e.Email_Address as email, o.Email_Address as owner
from Candidate c
join (
select Person_Id, Email_Address, First_Name ,Last_Name
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id
left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = c.Consultant_Id
    where c.Candidate_Id in (
select Candidate_Id from Placement where Placement_Code in ('PL41909'
,'PL41910'
,'PL44078'
,'PL43810'
,'PL44075'
,'PL 43186'
,'PL 42798'
,'PL41961'
,'PL44292'
,'PL41958'
,'PL42798'
,'PL42816'
,'PL42826'
,'PL42960'
,'PL43049'
,'PL43343'
,'PL43100'
,'PL43186'
,'PL43641'
))
""", engine_mssql) #59359
company = company.loc[~company['Client_Id'].isin(com_load['Client_Id'])]
contact = contact.loc[~contact['Client_Contact_Id'].isin(contact_load['Client_Contact_Id'])]
job_b = job_b.loc[~job_b['Role_Id'].isin(job_load['Role_Id'])]
candidate = candidate.loc[~candidate['Candidate_Id'].isin(candidate_load['Candidate_Id'])]
assert False
# %% transpose
company.rename(columns={
    'Client_Id': 'company-externalId',
    'Client_Name': 'company-name',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'Client_Contact_Id': 'contact-externalId',
    'Client_Id': 'contact-companyId',
    'Last_Name': 'contact-lastName',
    'First_Name': 'contact-firstName',
    # 'middlename': 'contact-middleName',
     'Email_Address': 'contact-email',
     # 'EMC_ACC_EMAILS': 'contact-owners',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'Role_Id': 'position-externalId',
    'Client_Id': 'position-companyId',
    'Client_Contact_Id': 'position-contactId',
    'Description': 'position-title',
    'Email for Vincere login': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

job_b.rename(columns={
    'Role_Id': 'position-externalId',
    'Client_Id': 'position-companyId',
    'Client_Contact_Id': 'position-contactId',
    'Description': 'position-title',
    'Email for Vincere login': 'position-owners',
}, inplace=True)
job_b, default_contacts_b = vincere_standard_migration.process_vincere_job_2(job_b, mylog)

candidate.rename(columns={
    'Candidate_Id': 'candidate-externalId',
    'First_Name': 'candidate-firstName',
    'Last_Name': 'candidate-lastName',
    # 'Middle': 'candidate-middleName',
    'email': 'candidate-email',
    'Email for Vincere login': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% to csv files
candidate.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)
job.to_csv(os.path.join(standard_file_upload, '5_job.csv'), index=False)
job_b.to_csv(os.path.join(standard_file_upload, '5_job_2.csv'), index=False)
contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)
if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts.csv'), index=False)

tem = default_contacts.loc[default_contacts['contact-companyId'] == 'DEFAULT_COMPANY']
if len(default_contacts_b):
    default_contacts_b.to_csv(os.path.join(standard_file_upload, '3_default_contacts_2.csv'), index=False)

tem2 = default_contacts_b.loc[default_contacts_b['contact-companyId'] == 'DEFAULT_COMPANY']
if len(default_company):
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
elif len(tem):
    default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
elif len(tem2):
    default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
    default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)

# %% to csv files
# comp = pd.read_csv(os.path.join(standard_file_upload, '2_company.csv'))
# comp['company-externalId'] = 'CASTLEPROD'+comp['company-externalId'].astype(str)
# comp.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)
#
# cont = pd.read_csv(os.path.join(standard_file_upload, '4_contact.csv'))
# cont['contact-externalId'] = 'CASTLEPROD'+cont['contact-externalId'].astype(str)
# cont.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
#
# j1 = pd.read_csv(os.path.join(standard_file_upload, '5_job.csv'))
# j1['position-externalId'] = 'CASTLEPROD'+j1['position-externalId'].astype(str)
# j1.to_csv(os.path.join(standard_file_upload, '5_job.csv'), index=False)
#
# j2 = pd.read_csv(os.path.join(standard_file_upload, '5_job_2.csv'))
# j2['position-externalId'] = 'CASTLEPROD'+j2['position-externalId'].astype(str)
# j2.to_csv(os.path.join(standard_file_upload, '5_job_2.csv'), index=False)
#
# cand = pd.read_csv(os.path.join(standard_file_upload, '6_candidate.csv'))
# cand['candidate-externalId'] = 'CASTLEPROD'+cand['candidate-externalId'].astype(str)
# cand.to_csv(os.path.join(standard_file_upload, '6_candidate.csv'), index=False)