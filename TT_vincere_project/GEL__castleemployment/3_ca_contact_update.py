# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
from functools import reduce
from common import vincere_job_application
import pandas as pd

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
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
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

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# %% info
contact = pd.read_sql("""
select Client_Contact_Id as contact_externalid
, Phone_Code_1
, Phone_Number_1
, Extension_1
, Phone_Code_2
, Phone_Number_2
, Extension_2
, Mobile_Code
, Mobile_Number
, Contact_Position
, Title
, Notes
from Client_Contact cc
left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
where Client_Contact_Id not in (
294
,3474
,5152
,7907
,10677
,10949
,15334
,16218
,18982
,20476
,21028
,22062)
""", engine_mssql)
contact['contact_externalid'] = contact['contact_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% location name/address
addr = pd.read_sql("""
select Client_Contact_Id as contact_externalid, a.*
from Client_Contact cc
left join (
select Client_Id, Client_Address_Id, addr.* from Client_Address ca
join (select Address_Id, Line_1, Line_2, Line_3, c.Description as county, co.Description as country, t.Description as town, Postcode
from Address a
left join County c on a.County_Id = c.County_Id
left join Country co on a.Country_Id = co.Country_Id
left join Town t on a.Town_Id = t.Town_Id) addr on addr.Address_Id = ca.Address_Id) a on cc.Client_Address_Id = a.Client_Address_Id and cc.Client_Id = a.Client_Id
where Client_Contact_Id not in (
294
,3474
,5152
,7907
,10677
,10949
,15334
,16218
,18982
,20476
,21028
,22062
)
""", engine_mssql)
addr['contact_externalid'] = addr['contact_externalid'].apply(lambda x: str(x) if x else x)
addr['company_externalid'] = addr['Client_Id'].apply(lambda x: str(x) if x else x)
addr['address'] = addr[['Line_1', 'Line_2','Line_3','town','county','Postcode','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem2 = addr[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'Contact_Position']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['Contact_Position']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'Phone_Code_1','Phone_Number_1','Extension_1']]
tem['primary_phone'] = tem[['Phone_Code_1','Phone_Number_1','Extension_1']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['primary_phone']!='']
vcont.update_primary_phone(tem, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'Mobile_Code','Mobile_Number']]
mobile_phone['mobile_phone'] = mobile_phone[['Mobile_Code','Mobile_Number']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
mobile_phone = mobile_phone.loc[mobile_phone['mobile_phone']!='']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
tem = contact[['contact_externalid','Phone_Code_2','Phone_Number_2','Extension_2']]
tem['home_phone'] = tem[['Phone_Code_2','Phone_Number_2','Extension_2']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['home_phone']!='']
vcont.update_home_phone(tem, mylog)

# %% reg date
# tem = contact[['contact_externalid', 'PersonCreateDate']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['PersonCreateDate'])
# vcont.update_reg_date(tem, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'Title']].dropna().drop_duplicates()
tem['gender_title'] = tem['Title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
cp = vcont.update_gender_title(tem2, mylog)

# %% note
note = contact[['contact_externalid','Notes']].dropna()
note['note'] = note['Notes']
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% unsub
tem = pd.read_sql("""
select cc.Client_Contact_Id, Email_Address as email from Client_Contact cc
left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
join (select Client_Contact_Id, Description
from Client_Contact_Skill cs
join Skill s on s.Skill_Id = cs.Skill_Id
where Description =' 1. UNSUBSCRIBED') s on cc.Client_Contact_Id = s.Client_Contact_Id
where Email_Address is not null
and cc.Client_Contact_Id not in (
294
,3474
,5152
,7907
,10677
,10949
,15334
,16218
,18982
,20476
,21028
,22062
)
""", engine_mssql)
tem = tem.loc[tem['email']!='UNSUBSCRIBED']
tem = tem.loc[tem['email'].str.contains('@')]
tem['subscribed']=0
vcont.email_subscribe(tem, mylog)

# %% delete
tem = pd.read_sql("""
select Client_Contact_Id from Client_Contact where Active_YN = 'N'
and Client_Contact_Id not in (
294
,3474
,5152
,7907
,10677
,10949
,15334
,16218
,18982
,20476
,21028
,22062
)
""", engine_mssql)
tem['contact_externalid'] = tem['Client_Contact_Id'].astype(str)
tem = tem.merge(vcont.contact, on=['contact_externalid'])
tem['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcont.ddbconn, ['deleted_timestamp', ], ['id', ], 'contact', mylog)

con_del = pd.read_sql("""select id, deleted_timestamp from contact c2 where company_id in (
select id from company c1  where c1.deleted_timestamp is not null)
and c2.deleted_timestamp is null""", connection)
con_del['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(con_del, vcont.ddbconn, ['deleted_timestamp', ], ['id', ], 'contact', mylog)


job_del = pd.read_sql("""select id, deleted_timestamp from position_description pd where contact_id in (
    select id from contact c where c.deleted_timestamp is not null)
and pd.deleted_timestamp is null""", connection)
job_del['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(job_del, vcont.ddbconn, ['deleted_timestamp', ], ['id', ], 'position_description', mylog)