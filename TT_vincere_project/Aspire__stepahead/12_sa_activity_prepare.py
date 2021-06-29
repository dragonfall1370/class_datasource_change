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
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

def html_to_text(html):
    from bs4 import BeautifulSoup
    # url = "http://news.bbc.co.uk/2/hi/health/2284783.stm"
    # html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html)

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text

# %% company
company_log = pd.read_sql("""
select concat('', Company_Reference) as company_external_id
     , nullif(trim(Event_Description),'') as Event_Description
     , Status
     , Priority
     , DateAndTime
     , Diary_Text
     , nullif(trim(Email),'') as owner
     , Created
from DB_Client_Diary_Details c
join (select concat('', Reference) as externalid, Company_Name
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
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference) com on c.Company_Reference = com.externalid
left join(
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '')                              AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) o on o.Full_Name = Consultant_Name
""", engine_mssql)
company_log = company_log.drop_duplicates()

# %% contact
contact_log = pd.read_sql("""
select new_contact_externalid as contact_external_id
     , nullif(trim(Event_Description),'') as Event_Description
     , Status
     , Priority
     , DateAndTime
     , Diary_Text
     , concat('', Company_Reference) as company_external_id
     , nullif(trim(o.Email),'') as owner
     , Created
from DB_Client_Contact_Diary_Details c
join (select distinct
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
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference) com on ccv.liaison_Reference = com.Reference) cont on cont.contact_externalid = c.Contact_Person_Reference
left join(
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '')                              AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) o on o.Full_Name = Consultant_Name
""", engine_mssql)
contact_log = contact_log.drop_duplicates()

# %% job
job_log = pd.read_sql("""
select
       concat('', Agreement_Reference) as position_external_id
     , concat('', Contact_Person_Reference) as contact_external_id
     , nullif(trim(Event_Description),'') as Event_Description
     , Status
     , Priority
     , Diary_Date, Time
     , Diary_Text
     , concat('', Company_Reference) as company_external_id
     , nullif(trim(Email),'') as owner
     , Created
from DB_Job_Order_Diary_Details j
join (select concat('', a.Reference) as job_externalid
     , Title                           as JobTitle
     , nullif(concat('', Company_Reference),'') as company_id
FROM dbo.agreement a
INNER JOIN dbo.Client b ON a.Client_Reference = b.reference
INNER JOIN dbo.Company c ON b.Company_Reference = c.reference
INNER JOIN dbo.Job_Description e ON a.Job_reference = e.Reference
inner join dbo.type_description f
on  a.job_model = f.reference
and f.type = 'job_model') j2 on j.Agreement_Reference = j2.job_externalid
left join(
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '')                              AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) o on o.Full_Name = Consultant_Name
where try_parse(CONCAT_WS('-'
                ,substring(Created,7,4)
                ,substring(Created,4,2)
                ,substring(Created,1,2)
        )as datetime) > '2016-10-01 00:00:00'
""", engine_mssql)
job_log = job_log.drop_duplicates()

# %% candidate
candidate_log = pd.read_sql("""
select concat('', c.Person_Reference) as candidate_external_id
     , nullif(trim(Event_Description),'') as Event_Description
     , Status
     , Priority
     , DateAndTime
     , Diary_Text
     , nullif(trim(o.Email),'') as owner
     , Created
from DB_Candidate_Diary_Details c
join(select concat('', csv.Person_Reference) as externalid
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
where csv.Active = 1) cand on cand.externalid = c.Person_Reference
left join(
    SELECT a.Reference,
           a.Person_Reference,
           UserName,
           Password,
           + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
               + ISNULL(RTRIM(Surname), '')                              AS Full_Name,
           Forename,
           Surname,
           dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
           dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
           SuperUser,
           a.active
    FROM Consultant a
             INNER JOIN Person b
                        ON a.person_reference = b.reference) o on o.Full_Name = Consultant_Name
""", engine_mssql)
candidate_log = candidate_log.drop_duplicates()


candidate_activity = pd.read_sql("""
select concat('', Person_Reference) as candidate_external_id
     , DateOfWork
     , ActivityDesc
     , nullif(trim(RoleDesc),'') as RoleDesc 
     , nullif(trim(Hirer_Name),'') as Hirer_Name
     , nullif(trim(Location_Name),'') as Location_Name
     , Timesheet
     , Archived
     , PayRate
     , concat('', Company_Reference) as company_external_id
from AWR_History_View a
join(select concat('', csv.Person_Reference) as externalid
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
where csv.Active = 1) cand on cand.externalid = a.Person_Reference
""", engine_mssql)
candidate_activity = candidate_activity.drop_duplicates()
candidate_activity['owner'] = ''

candidate_qualified = pd.read_sql("""
select concat('', Person_Reference) as candidate_external_id
     , date
     , nullif(trim(RoleDesc),'') as RoleDesc 
     , nullif(trim(Company_Name),'') as Company_Name
     , UnQualDate
     , AWRVerify
     , concat('', Company_Reference) as company_external_id
from AWR_Qualified_View a 
join(select concat('', csv.Person_Reference) as externalid
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
where csv.Active = 1) cand on cand.externalid = a.Person_Reference
""", engine_mssql)
candidate_qualified = candidate_qualified.drop_duplicates()
candidate_qualified['owner'] = ''

assert False
# %%
company_log['Created'] = company_log['Created'].apply(lambda x: x.strip())
company_log['insert_timestamp'] = pd.to_datetime(company_log['Created'])
company_log['insert_timestamp_2'] = pd.to_datetime(company_log['Created'], format='%d/%m/%Y %H:%M:%S')
company_log['DateAndTime'] = company_log['DateAndTime'].astype(str)
company_log['content'] = company_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

contact_log['Created'] = contact_log['Created'].apply(lambda x: x.strip())
contact_log['insert_timestamp'] = pd.to_datetime(contact_log['Created'])
contact_log['insert_timestamp_2'] = pd.to_datetime(contact_log['Created'], format='%d/%m/%Y %H:%M:%S')
contact_log['DateAndTime'] = contact_log['DateAndTime'].astype(str)
contact_log['content'] = contact_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

job_log['Created'] = job_log['Created'].apply(lambda x: x.strip())
job_log['insert_timestamp'] = pd.to_datetime(job_log['Created'])
job_log['insert_timestamp_2'] = pd.to_datetime(job_log['Created'], format='%d/%m/%Y %H:%M:%S')
# job_log['position_external_id']=job_log['job_external_id']
job_log['DateAndTime'] = job_log['Diary_Date'].astype(str)
job_log['content'] = job_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

candidate_log['Created'] = candidate_log['Created'].apply(lambda x: x.strip())
candidate_log['insert_timestamp'] = pd.to_datetime(candidate_log['Created'])
candidate_log['insert_timestamp_2'] = pd.to_datetime(candidate_log['Created'], format='%d/%m/%Y %H:%M:%S')
candidate_log['DateAndTime'] = candidate_log['DateAndTime'].astype(str)
candidate_log['content'] = candidate_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

candidate_activity['insert_timestamp'] = pd.to_datetime(candidate_activity['DateOfWork'])
candidate_activity['insert_timestamp_2'] = pd.to_datetime(candidate_activity['DateOfWork'], format='%Y/%m/%d')
candidate_activity['PayRate'] = candidate_activity['PayRate'].astype(str)
candidate_activity['Archived'] = candidate_activity['Archived'].astype(str)
candidate_activity['content'] = candidate_activity[['ActivityDesc','RoleDesc','Hirer_Name', 'Location_Name','Timesheet','Archived','PayRate']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Activity','Role','Hirer', 'Location','Timesheet','Archived','Pay Rate'], x) if e[1]]), axis=1)

candidate_qualified['insert_timestamp'] = pd.to_datetime(candidate_qualified['date'])
candidate_qualified['insert_timestamp_2'] = pd.to_datetime(candidate_qualified['date'], format='%Y/%m/%d')
candidate_qualified['UnQualDate'] = candidate_qualified['UnQualDate'].astype(str)
candidate_qualified['UnQualDate'] = candidate_qualified['UnQualDate'].apply(lambda x: x.replace('NaT',''))
candidate_qualified['content'] = candidate_qualified[['RoleDesc','Company_Name', 'UnQualDate','AWRVerify']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Role','Company', 'Un-Qual. Date','Verify'], x) if e[1]]), axis=1)

# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(company_log, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(contact_log, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(job_log, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(candidate_log, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)
re5 = vincere_activity.transform_activities_temp(candidate_activity, conn_str_ddb, mylog)
re5 = re5.where(re5.notnull(), None)
re6 = vincere_activity.transform_activities_temp(candidate_qualified, conn_str_ddb, mylog)
re6 = re6.where(re6.notnull(), None)

dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'insert_timestamp_2': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR,
    'type': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity_2', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity_2', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity_2', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity_2', if_exists='append', dtype=dtype, index=False)
re5.to_sql(con=engine_sqlite, name='vincere_activity_2', if_exists='append', dtype=dtype, index=False)
re6.to_sql(con=engine_sqlite, name='vincere_activity_2', if_exists='append', dtype=dtype, index=False)

# %% activity
# activity = pd.read_sql("""
# select id, company_id, contact_id, content, insert_timestamp, user_account_id from activity where company_id is not null and contact_id is not null
# """, engine_postgre_review)
# activity['matcher'] = activity['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# re3['matcher'] = re3['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem = activity.merge(re3, on=['company_id','contact_id','matcher','insert_timestamp'])
# tem2 = tem[['id','position_id']].drop_duplicates()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['position_id', ], ['id', ], 'activity', mylog)