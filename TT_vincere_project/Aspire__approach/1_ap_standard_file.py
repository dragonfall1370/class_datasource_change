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
cf.read('ap_config.ini')
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
select
concat('', Reference) as company_externalid
, Company_Name as company_name
, getdate() as created_date
from Company c
where Active = 1;
""", engine_mssql)

company_owner = pd.read_sql("""
select concat('', Company_Reference) as company_externalid
     , nullif(trim(cv.Email),'') as email

from Client_Search_View c
left join (
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '') AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) cv on cv.Reference = c.Company_Consultant
""", engine_mssql)
company_owner = company_owner.dropna()
company_owner = company_owner.drop_duplicates()
company_owner = company_owner.groupby('company_externalid')['email'].apply(','.join).reset_index()
company = company.merge(company_owner,on='company_externalid',how='left')
company = company.where(company.notnull(),None)

contact = pd.read_sql("""
select distinct
concat('', Reference) as contact_externalid
, nullif(Forename,'') as firstname
, nullif(Surname,'') as lastname
, nullif(middle,'') as middlename
, nullif(Email,'') as email
, getdate() as date_created
, concat('', liaison_Reference) as company_externalid
from Company_Contact_View ccv
where Active = 1
""", engine_mssql)


job = pd.read_sql("""
select
       concat('', Agreement_Reference) as job_externalid
     , Title                           as JobTitle
     , nullif(concat('', Company_Reference),'')   as company_externalid
     , nullif(concat('', ccv1.contact_externalid),'')   as contact_externalid
     , Start_Date                      as created_date
     , o.Email
from Agreement_view av
left join (select distinct
concat('', Reference) as contact_externalid
, nullif(Forename,'') as firstname
, nullif(Surname,'') as lastname
, nullif(middle,'') as middlename
, nullif(Email,'') as email
, getdate() as date_created
, concat('', liaison_Reference) as company_externalid
from Company_Contact_View ccv) ccv1 on av.liaison_Reference = ccv1.contact_externalid and av.Company_Reference = ccv1.company_externalid
left join (select q.Reference,Email
from consultant_lookup q
left join (
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '') AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) cv on cv.Reference = q.Consultant_Reference
where q.reference_type = 6 and nullif(Email, '') is not null) o on o.Reference = av.Agreement_Reference
where active = 1
""", engine_mssql)

candidate = pd.read_sql("""
select concat('', csv.Person_Reference) as externalid
     , nullif(csv.Forename,'') as Forename
     , nullif(csv.Surname,'') as Surname
     , nullif(csv.Middle,'') as Middle
     , nullif(Email_Address,'') as Email_Address
     , nullif(coalesce(o1.Email, o2.Email),'') as Email
from Candidate_Search_View csv
join Candidate_Merge_View cmv on csv.Person_Reference = cmv.Reference
left join (
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '') AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) o1 on o1.Reference = csv.TempConsultantReference
left join (
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '') AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) o2 on o2.Reference = csv.PermConsultantReference
where csv.Active = 1
""", engine_mssql)
assert False
# %% transpose
company.rename(columns={
    'company_externalid': 'company-externalId',
    'company_name': 'company-name',
    'email': 'company-owners',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'contact_externalid': 'contact-externalId',
    'company_externalid': 'contact-companyId',
    'firstname': 'contact-firstName',
    'lastname': 'contact-lastName',
    'middlename': 'contact-middleName',
    'email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'job_externalid': 'position-externalId',
    'company_externalid': 'position-companyId',
    'contact_externalid': 'position-contactId',
    'JobTitle': 'position-title',
    # 'Email': 'position-owners',
}, inplace=True)
job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

candidate.rename(columns={
    'externalid': 'candidate-externalId',
    'Forename': 'candidate-firstName',
    'Surname': 'candidate-lastName',
    'Middle': 'candidate-middleName',
    'Email_Address': 'candidate-email',
    'Email': 'candidate-owners',
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