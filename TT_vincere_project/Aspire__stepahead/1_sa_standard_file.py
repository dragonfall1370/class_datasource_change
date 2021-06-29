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
cf.read('sa_config.ini')
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
# company = pd.read_sql("""
# select
# concat('', Reference) as externalid
# , Company_Name as company_name
# , getdate() as created_date
# from Company c
# where Active = 1;
# """, engine_mssql)
#
# contact = pd.read_sql("""
# select distinct
# concat('', Reference) as contact_externalid
# , nullif(Forename,'') as firstname
# , nullif(Surname,'') as lastname
# , nullif(middle,'') as middlename
# , nullif(Email,'') as email
# , getdate() as date_created
# , com_id.company_externalid
# from Company_Contact_View ccv
# join(
# select
# concat('', Reference) as contact_externalid
# , nullif(concat('', max(liaison_Reference)),'') as company_externalid
# from Company_Contact_View
# group by Reference) com_id on com_id.contact_externalid = ccv.Reference
# where Active = 1
# """, engine_mssql)
#
#
# job = pd.read_sql("""
# select
#        concat('', Agreement_Reference) as job_externalid
#      , Title                           as JobTitle
#      , nullif(concat('', Company_Reference),'')   as company_externalid
#      , nullif(concat('', ccv1.contact_externalid),'')   as contact_externalid
#      , Start_Date                      as created_date
#      , o.Email
# from Agreement_view av
# left join (select distinct
# concat('', Reference) as contact_externalid
# , nullif(Forename,'') as firstname
# , nullif(Surname,'') as lastname
# , nullif(middle,'') as middlename
# , nullif(Email,'') as email
# , getdate() as date_created
# , com_id.company_externalid
# from Company_Contact_View ccv
# join(
# select
# concat('', Reference) as contact_externalid
# , nullif(concat('', max(liaison_Reference)),'') as company_externalid
# from Company_Contact_View
# group by Reference) com_id on com_id.contact_externalid = ccv.Reference) ccv1 on av.liaison_Reference = ccv1.contact_externalid and av.Company_Reference = ccv1.company_externalid
# left join (select q.Reference,Email
# from consultant_lookup q
# left join (
#     SELECT a.Reference,
#            a.Person_Reference,
#            UserName,
#            Password,
#            + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
#                + ISNULL(RTRIM(Surname), '') AS Full_Name,
#            Forename,
#            Surname,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
#            SuperUser,
#            a.active
#     FROM Consultant a
#              INNER JOIN Person b
#                         ON a.person_reference = b.reference) cv on cv.Reference = q.Consultant_Reference
# where q.reference_type = 6 and nullif(Email, '') is not null) o on o.Reference = av.Agreement_Reference
# where active = 1
# """, engine_mssql)
#
# candidate = pd.read_sql("""
# select concat('', csv.Person_Reference) as externalid
#      , nullif(csv.Forename,'') as Forename
#      , nullif(csv.Surname,'') as Surname
#      , nullif(csv.Middle,'') as Middle
#      , nullif(Email_Address,'') as Email_Address
#      , nullif(coalesce(o1.Email, o2.Email),'') as Email
# from Candidate_Search_View csv
# join Candidate_Merge_View cmv on csv.Person_Reference = cmv.Reference
# left join (
#     SELECT a.Reference,
#            a.Person_Reference,
#            UserName,
#            Password,
#            + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
#                + ISNULL(RTRIM(Surname), '') AS Full_Name,
#            Forename,
#            Surname,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
#            SuperUser,
#            a.active
#     FROM Consultant a
#              INNER JOIN Person b
#                         ON a.person_reference = b.reference) o1 on o1.Reference = csv.TempConsultantReference
# left join (
#     SELECT a.Reference,
#            a.Person_Reference,
#            UserName,
#            Password,
#            + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
#                + ISNULL(RTRIM(Surname), '') AS Full_Name,
#            Forename,
#            Surname,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
#            SuperUser,
#            a.active
#     FROM Consultant a
#              INNER JOIN Person b
#                         ON a.person_reference = b.reference) o2 on o2.Reference = csv.PermConsultantReference
# where csv.Active = 1
# """, engine_mssql)
# assert False
company = pd.read_sql("""
select
concat('', Reference) as externalid
, Company_Name
 from (select Company_Name, Company.Reference, Client.Reference as client_ref from Company
    join Client on Client.Company_Reference = Company.Reference) c
join(
select distinct Company_Reference
From Diary_Entry a
Inner join diary_entry_blueprint d
on a.Blueprint_Reference = d.Reference
Inner join type_description e
on d.Diary_type = e.reference
Inner join Diary_Entry_Lookup f
on a.Reference = f.Reference
and f.Entity_Type = 4
Inner join DB_Client_Basic_Details g
on f.Entity_Reference = g.Company_Reference
Where consultant_reference is not null
and e.type = 'Diary'
and Created between '2016-10-01 00:00:00' and '2020-10-01 00:00:00'
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference
""", engine_mssql)

contact = pd.read_sql("""
select distinct
concat('', ccv.Reference) as contact_externalid
, nullif(Forename,'') as firstname
, nullif(Surname,'') as lastname
, nullif(middle,'') as middlename
, nullif(Email,'') as email
, getdate() as date_created
, com.Reference
, concat(ccv.Reference,'_',com.Reference) as new_contact_externalid
from Company_Contact_View ccv
join (select Reference, Company_Name from (select Company_Name, Company.Reference, Client.Reference as client_ref from Company
    join Client on Client.Company_Reference = Company.Reference) c
join(
select distinct Company_Reference
From Diary_Entry a
Inner join diary_entry_blueprint d
on a.Blueprint_Reference = d.Reference
Inner join type_description e
on d.Diary_type = e.reference
Inner join Diary_Entry_Lookup f
on a.Reference = f.Reference
and f.Entity_Type = 4
Inner join DB_Client_Basic_Details g
on f.Entity_Reference = g.Company_Reference
Where consultant_reference is not null
and e.type = 'Diary'
and Created between '2016-10-01 00:00:00' and '2020-10-01 00:00:00'
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference) com on ccv.liaison_Reference = com.Reference
""", engine_mssql)


job = pd.read_sql("""
select concat('', a.Reference) as job_externalid
     , Title                           as JobTitle
     , nullif(concat('', Company_Reference),'') as company_id
     ,coalesce(contact_externalid+'_'+cast(company_externalid as varchar),null) as new_contact_externalid
     , o.Email
FROM dbo.agreement a
INNER JOIN dbo.Client b ON a.Client_Reference = b.reference
INNER JOIN dbo.Company c ON b.Company_Reference = c.reference
INNER JOIN dbo.Job_Description e ON a.Job_reference = e.Reference
inner join dbo.type_description f
on  a.job_model = f.reference
and f.type = 'job_model'

left join (select distinct
concat('', ccv.Reference) as contact_externalid
, nullif(Forename,'') as firstname
, nullif(Surname,'') as lastname
, nullif(middle,'') as middlename
, nullif(Email,'') as email
, getdate() as date_created
, com.Reference as company_externalid
, concat(ccv.Reference,'_',com.Reference) as new_contact_externalid
from Company_Contact_View ccv
join (select Reference, Company_Name from (select Company_Name, Company.Reference, Client.Reference as client_ref from Company
    join Client on Client.Company_Reference = Company.Reference) c
join(
select distinct Company_Reference
From Diary_Entry a
Inner join diary_entry_blueprint d
on a.Blueprint_Reference = d.Reference
Inner join type_description e
on d.Diary_type = e.reference
Inner join Diary_Entry_Lookup f
on a.Reference = f.Reference
and f.Entity_Type = 4
Inner join DB_Client_Basic_Details g
on f.Entity_Reference = g.Company_Reference
Where consultant_reference is not null
and e.type = 'Diary'
and Created between '2016-10-01 00:00:00' and '2020-10-01 00:00:00'
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference) com on ccv.liaison_Reference = com.Reference) ccv1 on a.liaison_Reference = ccv1.contact_externalid and Company_Reference = ccv1.company_externalid

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
where q.reference_type = 6 and nullif(Email, '') is not null) o on o.Reference = a.Reference

where a.Active = 1 and Start_Date > '2016-10-01 00:00:00'
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
    'externalid': 'company-externalId',
    'company_name': 'company-name',
}, inplace=True)
company = vincere_standard_migration.process_vincere_comp(company, mylog)

contact.rename(columns={
    'new_contact_externalid': 'contact-externalId',
    'Reference': 'contact-companyId',
    'firstname': 'contact-firstName',
    'lastname': 'contact-lastName',
    'middlename': 'contact-middleName',
    'email': 'contact-email',
}, inplace=True)
contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)

job.rename(columns={
    'job_externalid': 'position-externalId',
    'company_id': 'position-companyId',
    'new_contact_externalid': 'position-contactId',
    'JobTitle': 'position-title',
    'Email': 'position-owners',
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