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
assert False
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
      )as datetime) > '2019-10-01 00:00:00'
""", engine_mssql)
company_log = company_log.drop_duplicates()

# %% contact
contact_log = pd.read_sql("""
select concat('', Contact_Person_Reference) as contact_external_id
     , nullif(trim(Event_Description),'') as Event_Description
     , Status
     , Priority
     , DateAndTime
     , Diary_Text
     , concat('', Company_Reference) as company_external_id
     , nullif(trim(o.Email),'') as owner
     , Created
from DB_Client_Contact_Diary_Details c
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
      )as datetime) > '2019-10-01 00:00:00'
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
       )as datetime) > '2019-10-01 00:00:00'
""", engine_mssql)
job_log = job_log.drop_duplicates()

# %% candidate
candidate_log = pd.read_sql("""
select top 500000
    concat('', c.Person_Reference) as candidate_external_id
     , nullif(trim(Event_Description),'') as Event_Description
     , Status
     , Priority
     , DateAndTime
     , Diary_Text
     , nullif(trim(o.Email),'') as owner
     , Created
from DB_Candidate_Diary_Details c
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
--where try_parse(CONCAT_WS('-'
--                ,substring(Created,7,4)
--                 ,substring(Created,4,2)
--                ,substring(Created,1,2)
--      )as datetime) > '2019-10-01 00:00:00'
""", engine_mssql)
candidate_log = candidate_log.drop_duplicates()
assert False
# %%
company_log['Created'] = company_log['Created'].apply(lambda x: x.strip())
# company_log['insert_timestamp'] = pd.to_datetime(company_log['Created'])
company_log['insert_timestamp'] = pd.to_datetime(company_log['Created'], format='%d/%m/%Y %H:%M:%S')
company_log['DateAndTime'] = company_log['DateAndTime'].astype(str)
company_log['content'] = company_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

contact_log['Created'] = contact_log['Created'].apply(lambda x: x.strip())
# contact_log['insert_timestamp'] = pd.to_datetime(contact_log['Created'])
contact_log['insert_timestamp'] = pd.to_datetime(contact_log['Created'], format='%d/%m/%Y %H:%M:%S')
contact_log['DateAndTime'] = contact_log['DateAndTime'].astype(str)
contact_log['content'] = contact_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

job_log['Created'] = job_log['Created'].apply(lambda x: x.strip())
# job_log['insert_timestamp'] = pd.to_datetime(job_log['Created'])
job_log['insert_timestamp'] = pd.to_datetime(job_log['Created'], format='%d/%m/%Y %H:%M:%S')
# job_log['position_external_id']=job_log['job_external_id']
job_log['DateAndTime'] = job_log['Diary_Date'].astype(str)
job_log['content'] = job_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

candidate_log['Created'] = candidate_log['Created'].apply(lambda x: x.strip())
# candidate_log['insert_timestamp'] = pd.to_datetime(candidate_log['Created'])
candidate_log['insert_timestamp'] = pd.to_datetime(candidate_log['Created'], format='%d/%m/%Y %H:%M:%S')
candidate_log['DateAndTime'] = candidate_log['DateAndTime'].astype(str)
candidate_log['content'] = candidate_log[['Event_Description','Status', 'Priority','Diary_Text','DateAndTime']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Event','Status', 'Priority', 'Notes', 'Date'], x) if e[1]]), axis=1)

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

dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    # 'insert_timestamp_2': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR,
    'type': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity_review', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity_review', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity_review', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity_review', if_exists='append', dtype=dtype, index=False)