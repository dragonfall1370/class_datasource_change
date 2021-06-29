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

user = pd.read_csv('user.csv')
user['matcher'] = user['Email in Gel'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

# assert False
# %% pending
client_note = pd.read_sql("""
select Client_Id, Notes, Created_DTTM, Email_Address from Client_Note ce
    left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = ce.Created_By_Id
""", engine_mssql)
client_note['company_external_id'] = client_note['Client_Id'].astype(str) #candidate exclude by client

cand_ex_client = pd.read_sql("""
select ce.Client_Id, Candidate_Id, ce.Notes, ce.Created_DTTM
from Client_Exclude ce --candidate exclude by client
""", engine_mssql)
cand_ex_client['company_external_id'] = cand_ex_client['Client_Id'].astype(str) #candidate exclude by client
cand_ex_client['candidate_external_id'] = cand_ex_client['Candidate_Id'].astype(str) #candidate exclude by client

client_ex_cand = pd.read_sql("""
select ce.Client_Id, Candidate_Id, ce.Notes, ce.Created_DTTM
from Candidate_Exclude ce --client exclude by candidate
""", engine_mssql)
client_ex_cand['company_external_id'] = client_ex_cand['Client_Id'].astype(str) #candidate exclude by client
client_ex_cand['candidate_external_id'] = client_ex_cand['Candidate_Id'].astype(str) #candidate exclude by client

client_contact = pd.read_sql("""
select Client_Name, Client_Id,full_name, Contact_Position,ar.*,act.*, art.Description as act_type
from Activity_Recipient_List ar
left join Activity_Recipient_Type art on art.Recipient_Type_Id= ar.Recipient_Type_Id
left join (select concat(First_Name,' ',Last_Name) as full_name, Contact_Position, cc.Client_Id,Client_Name, cc.Client_Contact_Id
from Client_Contact cc
left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
left join Client com on cc.Client_Id = com.Client_Id) c on c.Client_Contact_Id = ar.Recipient_Id
left join (select Activity_Id
     , Start_DTTM
     , End_DTTM
     , AllDay_YN
     , NoTime_YN
     , a.Diary_YN
     , Alarm_YN
     , a.Completed_YN
     , Signature
     , Updated_DTTM
     , at.Description as type
     , a.Description
     , amt.Description as medium
     , Notes
     , Created_DTTM, Email_Address, arc.*
from Activity a
left join Activity_Type at on a.Type_Id = at.Type_Id
left join Activity_Medium_Type amt on amt.Type_Id = a.Medium_Type_Id
left join Activity_Recurrence arc on arc.Recurrence_Id = a.Recurrence_Id
    left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = a.Consultant_Id) act on act. Activity_Id = ar.Activity_Id
where art.Description = 'Client Contact'
""", engine_mssql)
client_contact = client_contact.drop_duplicates()

candidate = pd.read_sql("""
select full_name,ar.*,act.*, art.Description as act_type
from Activity_Recipient_List ar
left join Activity_Recipient_Type art on art.Recipient_Type_Id= ar.Recipient_Type_Id
left join (select Candidate_Id, concat(First_Name,' ',Last_Name) as full_name
from Candidate c
join (
select Person_Id, Email_Address, First_Name ,Last_Name
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) c on c.Candidate_Id = ar.Recipient_Id
left join (select Activity_Id
     , Start_DTTM
     , End_DTTM
     , AllDay_YN
     , NoTime_YN
     , a.Diary_YN
     , Alarm_YN
     , a.Completed_YN
     , Signature
     , Updated_DTTM
     , at.Description as type
     , a.Description
     , amt.Description as medium
     , Notes
     , Created_DTTM, Email_Address, arc.*
from Activity a
left join Activity_Type at on a.Type_Id = at.Type_Id
left join Activity_Medium_Type amt on amt.Type_Id = a.Medium_Type_Id
left join Activity_Recurrence arc on arc.Recurrence_Id = a.Recurrence_Id
    left join (
select Consultant_Id, Email_Address from Consultant c
join (
select Person_Id, Email_Address
from Person p join Contact_Info ci on ci.Contact_Info_Id = p.Contact_Info_Id) e on c.Person_Id = e.Person_Id) o on o.Consultant_Id = a.Consultant_Id) act on act. Activity_Id = ar.Activity_Id
where art.Description = 'Candidate'
""", engine_mssql)
candidate = candidate.drop_duplicates()
assert False
# %%
client_note['Created_DTTM_1'] = client_note['Created_DTTM'].apply(lambda x: x[0:10] if x else x)
client_note['Created_DTTM_2'] = client_note['Created_DTTM'].apply(lambda x: x[11:19] if x else x)
client_note['Created_DTTM'] = client_note['Created_DTTM_1'] +' '+client_note['Created_DTTM_2']
client_note['Created_DTTM'] = client_note['Created_DTTM'].apply(lambda x: x.replace('.',':') if x else x)
client_note['insert_timestamp'] = pd.to_datetime(client_note['Created_DTTM'], format='%Y/%m/%d %H:%M:%S')
client_note['matcher'] = client_note['Email_Address'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower() if x else x)
client_note = client_note.merge(user,on='matcher',how='left')
client_note['owner'] = client_note['Email for Vincere login']
client_note['owner'] = client_note['owner'].fillna('')
client_note['content'] = '---Client Genetal Notes---\n'+client_note['Notes']
client_note = client_note.loc[client_note['insert_timestamp']>'2017-01-01 00:00:00']

cand_ex_client['Created_DTTM_1'] = cand_ex_client['Created_DTTM'].apply(lambda x: x[0:10] if x else x)
cand_ex_client['Created_DTTM_2'] = cand_ex_client['Created_DTTM'].apply(lambda x: x[11:19] if x else x)
cand_ex_client['Created_DTTM'] = cand_ex_client['Created_DTTM_1'] +' '+cand_ex_client['Created_DTTM_2']
cand_ex_client['Created_DTTM'] = cand_ex_client['Created_DTTM'].apply(lambda x: x.replace('.',':') if x else x)
cand_ex_client['insert_timestamp'] = pd.to_datetime(cand_ex_client['Created_DTTM'], format='%Y/%m/%d %H:%M:%S')
cand_ex_client['Notes'] = cand_ex_client['Notes'].fillna('')
cand_ex_client['content'] = '---Candidates excluded by Client---\n'+cand_ex_client['Notes']
cand_ex_client = cand_ex_client.loc[cand_ex_client['insert_timestamp']>'2017-01-01 00:00:00']
cand_ex_client['owner'] = ''

client_ex_cand['Created_DTTM_1'] = client_ex_cand['Created_DTTM'].apply(lambda x: x[0:10] if x else x)
client_ex_cand['Created_DTTM_2'] = client_ex_cand['Created_DTTM'].apply(lambda x: x[11:19] if x else x)
client_ex_cand['Created_DTTM'] = client_ex_cand['Created_DTTM_1'] +' '+client_ex_cand['Created_DTTM_2']
client_ex_cand['Created_DTTM'] = client_ex_cand['Created_DTTM'].apply(lambda x: x.replace('.',':') if x else x)
client_ex_cand['insert_timestamp'] = pd.to_datetime(client_ex_cand['Created_DTTM'], format='%Y/%m/%d %H:%M:%S')
client_ex_cand['Notes'] = client_ex_cand['Notes'].fillna('')
client_ex_cand['content'] = '---Client excluded by Candidates---\n'+client_ex_cand['Notes']
client_ex_cand = client_ex_cand.loc[client_ex_cand['insert_timestamp']>'2017-01-01 00:00:00']
client_ex_cand['owner'] = ''

client_contact = client_contact.loc[:,~client_contact.columns.duplicated()]
candidate = candidate.loc[:,~candidate.columns.duplicated()]

client_contact['Created_DTTM_1'] = client_contact['Created_DTTM'].apply(lambda x: x[0:10] if x else x)
client_contact['Created_DTTM_2'] = client_contact['Created_DTTM'].apply(lambda x: x[11:19] if x else x)
client_contact['Created_DTTM'] = client_contact['Created_DTTM_1'] +' '+client_contact['Created_DTTM_2']
client_contact['Created_DTTM'] = client_contact['Created_DTTM'].apply(lambda x: x.replace('.',':') if x else x)
client_contact['insert_timestamp'] = pd.to_datetime(client_contact['Created_DTTM'], format='%Y/%m/%d %H:%M:%S')
client_contact = client_contact.loc[client_contact['insert_timestamp']>'2017-01-01 00:00:00']

client_contact['Email_Address'] = client_contact['Email_Address'].fillna('')
client_contact['matcher'] = client_contact['Email_Address'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
client_contact = client_contact.merge(user,on='matcher',how='left')
client_contact['owner'] = client_contact['Email for Vincere login']
client_contact['owner'] = client_contact['owner'].fillna('')
client_contact = client_contact.where(client_contact.notnull(),None)
# client_contact['content'] = client_contact[['Start_DTTM','End_DTTM','AllDay_YN','NoTime_YN','Diary_YN','Alarm_YN','Completed_YN','type','Description','medium','Notes','Recurrence_Frequency','reci']]\
#     .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Start','End','All Day','No Time','Diary','Alarm','Completed','Type','Description','Medium','Notes','Recurrence',''], x) if e[1]]), axis=1)
client_contact['content'] = client_contact[['type','Description','medium','Notes']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Description','Medium','Notes'], x) if e[1]]), axis=1)
client_contact['contact_external_id'] = client_contact['Recipient_Id'].astype(str)
client_contact['company_external_id'] = client_contact['Client_Id'].astype(str)


candidate['Created_DTTM_1'] = candidate['Created_DTTM'].apply(lambda x: x[0:10] if x else x)
candidate['Created_DTTM_2'] = candidate['Created_DTTM'].apply(lambda x: x[11:19] if x else x)
candidate['Created_DTTM'] = candidate['Created_DTTM_1'] +' '+candidate['Created_DTTM_2']
candidate['Created_DTTM'] = candidate['Created_DTTM'].apply(lambda x: x.replace('.',':') if x else x)
candidate['insert_timestamp'] = pd.to_datetime(candidate['Created_DTTM'], format='%Y/%m/%d %H:%M:%S')
candidate = candidate.loc[candidate['insert_timestamp']>'2017-01-01 00:00:00']

candidate['Email_Address'] = candidate['Email_Address'].fillna('')
candidate['matcher'] = candidate['Email_Address'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
candidate = candidate.merge(user,on='matcher',how='left')
candidate['owner'] = candidate['Email for Vincere login']
candidate['owner'] = candidate['owner'].fillna('')

candidate = candidate.where(candidate.notnull(),None)
candidate['content'] = candidate[['type','Description','medium','Notes']]\
    .apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Type','Description','Medium','Notes'], x) if e[1]]), axis=1)
candidate['candidate_external_id'] = candidate['Recipient_Id'].astype(str)


# %% load db
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(client_contact, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(candidate, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_activities_temp(client_ex_cand, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re4 = vincere_activity.transform_activities_temp(cand_ex_client, conn_str_ddb, mylog)
re4 = re4.where(re4.notnull(), None)
re5 = vincere_activity.transform_activities_temp(client_note, conn_str_ddb, mylog)
re5 = re5.where(re5.notnull(), None)

dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR,
    'type': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity_prod', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity_prod', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity_prod', if_exists='append', dtype=dtype, index=False)
re4.to_sql(con=engine_sqlite, name='vincere_activity_prod', if_exists='append', dtype=dtype, index=False)
re5.to_sql(con=engine_sqlite, name='vincere_activity_prod', if_exists='append', dtype=dtype, index=False)

# %% activity
# activity = pd.read_sql("""
# select id, company_id, contact_id, content, insert_timestamp, user_account_id from activity where company_id is not null and contact_id is not null
# """, engine_postgre_review)
# activity['matcher'] = activity['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# re3['matcher'] = re3['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem = activity.merge(re3, on=['company_id','contact_id','matcher','insert_timestamp'])
# tem2 = tem[['id','position_id']].drop_duplicates()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['position_id', ], ['id', ], 'activity', mylog)