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
cf.read('en_config.ini')
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

# conn_str_ddb_bkup = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format('tony', 'Yz0t@d4Ta', '35.158.17.10', '25432', 'sociumrecruitment.vincere.io')
# engine_postgre_bkup = sqlalchemy.create_engine(conn_str_ddb_bkup, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# connection2 = engine_postgre_bkup.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)

# from common import vincere_contact
# vcont2 = vincere_contact.Contact(connection2)
# %% info
contact = pd.read_sql("""
with cont_tmp as (
select ContactId as contact_externalid
     , nullif(c.FirstName,'') as FirstName
     , nullif(c.LastName,'') as LastName
     , nullif(c.AlsoKnownAs,'') as AlsoKnownAs
      , nullif(Address1,'') as Address1
     , nullif(Address2,'') as Address2
     , nullif(Address3,'') as Address3
     , nullif(City,'') as City
     , nullif(c.Postcode,'') as Add_Postcode 
     , nullif(County,'') as County
     , nullif(c.Country,'') as Country
     , nullif(c.EMail,'') as Email
     , nullif(c.EMail2,'') as Email2
     , nullif(c.DirectTel,'') as DirectTel
     , nullif(c.WorkTel,'') as WorkTel
     , nullif(c.MobileTel,'') as MobileTel
     , nullif(c.HomeTel,'') as HomeTel
     , nullif(c.Department,'') as Department
     , nullif(c.LinkedInConnected,'') as LinkedInConnected
     , nullif(c.Location,'') as Location
     , nullif(c.SubLocation,'') as SubLocation
     , nullif(u.Email,'') as owner
      , nullif(c.WebSite,'') as WebSite
     , nullif(c.IsMaleGender,'') as IsMaleGender
     , nullif(c.Title,'') as Title
     , nullif(c.DateEmailed,'') as DateEmailed
     , u.UserName as LastUser
     , nullif(c.CallStatus,'') as CallStatus
     , nullif(c.HomeCity,'') as HomeCity
     , nullif(c.ContactSource,'') as Source
     , nullif(c.Sector,'') as Sector
     , nullif(c.JobTitle,'') as JobTitle
     , nullif(AltContact,'') as AltContact
     , nullif(c.Latitude,'') as Latitude
     , nullif(c.Longitude,'') as Longitude
--      , nullif(c.Des,'') as GDPRStatus
--      , nullif(c.DateEmailed,'') as DateEmailed
     , nullif(c.GDPRStatus,'') as GDPRStatus
--      , nullif(c.Da,'') as GDPRStatus
     , coalesce(com.CompanyId, com2.CompanyId) as company_externalid
    , ROW_NUMBER() OVER(PARTITION BY c.ContactId ORDER BY ContactId DESC) rn
from Contacts c
left join Companies com on com.CompanyId = c.CompanyId
left join Companies com2 on com2.CompanyName = c.Company
left join Users u on u.UserId = c.LastUser
where  Descriptor = 1)
select * from cont_tmp where rn = 1
""", engine_mssql.raw_connection())
contact['contact_externalid'] = 'EUK'+contact['contact_externalid']
contact['company_externalid'] = contact['company_externalid'].apply(lambda x: 'EUK'+x if x else x)
assert False
contact.loc[contact['MobileTel'].notnull()]
# vcont2.set_work_location_by_company_location(mylog)
# %% location name/address
contact['address'] = contact[['Address1', 'Address2', 'Address3', 'City', 'Add_Postcode','County','Country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['location_name'] = contact[['Add_Postcode', 'SubLocation', 'Location']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# contact.loc[contact['contact_externalid']=='496623-2782-1358']
# contact.loc[contact['location_name']=='', 'location_name'] = contact['address']
# vcom.update_location_name_2(contact,dest_db,mylog)
# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address','location_name']].drop_duplicates()
comaddr = comaddr.loc[comaddr['company_externalid'].notnull()]
cp1 = vcom.insert_company_location(comaddr, mylog)

# %%
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% city
comaddr = contact[['company_externalid', 'address', 'City', 'Add_Postcode', 'County', 'Country']].drop_duplicates()
comaddr['city'] = comaddr.City
cp3 = vcom.update_location_city_2(comaddr, dest_db, mylog)

# %% postcode
comaddr['post_code'] = comaddr.Add_Postcode
cp4 = vcom.update_location_post_code_2(comaddr, dest_db, mylog)

# %% state
# comaddr['state'] = comaddr.County
comaddr['district'] = comaddr.County
# cp5 = vcom.update_location_state_2(comaddr, dest_db, mylog)
cp5 = vcom.update_location_district_2(comaddr, dest_db, mylog)

# %% country
comaddr['country_code'] = comaddr.Country.map(vcom.get_country_code)
comaddr['country'] = comaddr.Country
cp6 = vcom.update_location_country_2(comaddr, dest_db, mylog)

# %% latitude longitude
tem = contact[['company_externalid', 'address', 'Latitude', 'Longitude']]
tem = tem.loc[tem['Latitude'].notnull()]
tem = tem.loc[tem['Longitude'].notnull()]
tem['latitude'] = tem['Latitude'].apply(lambda x: str(x).strip() if x else x)
tem['longitude'] = tem['Longitude'].apply(lambda x: str(x).strip() if x else x)
tem['latitude'] = tem['latitude'].astype(float)
tem['longitude'] = tem['longitude'].astype(float)
tem['longitude'].unique()
cp6 = vcom.update_location_latlong(tem, dest_db, mylog)

# %% source
tem = contact[['contact_externalid', 'Source']].dropna().drop_duplicates()
tem['Source'].unique()
tem.loc[tem['Source']=='Xing']
tem.loc[tem['Source']=='Connect (Odro)', 'source'] = 'Connect (Odro)'
tem.loc[tem['Source']=='Energize Mailshot', 'source'] = 'Energize Mailshot'
tem.loc[tem['Source']=='Energize Website', 'source'] = 'Energize Website'
tem.loc[tem['Source']=='Headhunt', 'source'] = 'Headhunt'
tem.loc[tem['Source']=='Indeed', 'source'] = 'Indeed'
tem.loc[tem['Source']=='JobServe - Ad Response', 'source'] = 'JobServe - Ad Response'
tem.loc[tem['Source']=='LinkedIn', 'source'] = 'LinkedIn'
tem.loc[tem['Source']=='LinkedIn - Ad Response', 'source'] = 'LinkedIn - Ad Response'
tem.loc[tem['Source']=='LinkedIn Recruiter', 'source'] = 'LinkedIn Recruiter'
tem.loc[tem['Source']=='Recruit Studio', 'source'] = 'Recruit Studio'
tem.loc[tem['Source']=='Reed - Ad Response', 'source'] = 'Reed - Ad Response'
tem.loc[tem['Source']=='Reed - Search', 'source'] = 'Reed - Search'
tem.loc[tem['Source']=='Referral', 'source'] = 'Referral'
tem.loc[tem['Source']=='Totaljobs - Ad Response', 'source'] = 'Totaljobs - Ad Response'
tem.loc[tem['Source']=='Totaljobs - Search', 'source'] = 'Totaljobs - Search'
tem.loc[tem['Source']=='Xing', 'source'] = 'Xing'
tem2 = tem[['contact_externalid', 'source']].dropna().drop_duplicates()
cp = vcont.insert_source(tem2)

# %% insert department company
# tem = contact[['company_externalid', 'Department']].drop_duplicates().dropna()
# tem['department_name'] = tem['Department']
# cp7 = vcom.insert_department(tem, mylog)

# %% insert department contact
# assign the new addesses to contacts work location
# tem2 = contact[['contact_externalid', 'company_externalid', 'Department']].drop_duplicates().dropna()
# tem2['department_name'] = tem2['Department']
# cp8 = vcont.insert_contact_department(tem2, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'Title']].dropna().drop_duplicates()
tem['gender_title'] = tem['Title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
cp = vcont.update_gender_title(tem, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'JobTitle']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['JobTitle']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'DirectTel']].dropna().drop_duplicates()
tem['primary_phone'] = tem['DirectTel']
vcont.update_primary_phone(tem, mylog)

# %% switchboard
tem = contact[['contact_externalid', 'WorkTel']].dropna().drop_duplicates()
tem['switchboard_phone'] = tem['WorkTel']
vcont.update_switchboard_phone(tem, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'MobileTel']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['MobileTel']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
tem = contact[['contact_externalid', 'HomeTel']].dropna().drop_duplicates()
tem['home_phone'] = tem['HomeTel']
vcont.update_home_phone(tem, mylog)

# %% work email
tem = contact[['contact_externalid', 'Email2']].dropna().drop_duplicates()
tem['personal_email'] = tem['Email2']
vcont.update_personal_email(tem, mylog)

# %% primary email
email = contact[['contact_externalid', 'Email', 'Department']]
email.loc[email['Email'].isnull(),'Email1'] = email['Department']
tem1 = email.loc[email['Email1'].notnull()]
tem1 = tem1.loc[tem1['Email1'].str.contains('@')]
tem1['email'] = tem1['Email1']
vcont.update_email(tem1, mylog)

# %% social
tem = contact[['contact_externalid', 'WebSite']].dropna().drop_duplicates()
tem['linkedin'] = tem['WebSite']
vcont.update_linkedin(tem, mylog)

# %% department
# tem = contact[['contact_externalid', 'Department']].dropna().drop_duplicates()
# tem['department'] = tem['Department']
# vcont.update_department(tem, mylog)

# %% reg date
# tem = contact[['contact_externalid', 'RegDate']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['RegDate'])
# vcont.update_reg_date(tem, mylog)

# %% skills
skill = pd.read_sql("""
select c.ContactId as contact_externalid, Skill as skills
from Contacts c
left join (select ObjectId, Skill
from SkillInstances si
left join Skills s on s.SkillId = si.SkillId) sk on sk.ObjectId = c.ContactId
where Descriptor = 1
and Skill is not null
""", engine_mssql)
skill['contact_externalid'] = 'EUK'+skill['contact_externalid']
vcont.update_skills(skill, mylog)
# %% dob
# tem = contact[['contact_externalid', 'DateOfBirth']].dropna().drop_duplicates()
# # tem.to_csv('dob.csv')
# tem = tem.loc[tem['DateOfBirth'] != '0001-01-01']
# tem['date_of_birth'] = pd.to_datetime(tem['DateOfBirth'])
# vcont.update_dob(tem, mylog)
#
# # %% middle name
# tem = contact[['contact_externalid', 'MiddleName']].dropna().drop_duplicates()
# tem['middle_name'] = tem['MiddleName']
# vcont.update_middle_name(tem, mylog)
#
# # %% preferred name
# tem = contact[['contact_externalid', 'KnownAs']].dropna().drop_duplicates()
# tem['preferred_name'] = tem['KnownAs']
# vcont.update_preferred_name(tem, mylog)

# %% note
contact.loc[contact['IsMaleGender']==True, 'IsMaleGender'] = 'Male'
# contact.loc[contact['IsMaleGender'].isnull(), 'IsMaleGender'] = 'Female'

contact['note'] = contact[['IsMaleGender', 'AltContact'
    , 'LastUser', 'HomeCity', 'CallStatus','GDPRStatus']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Gender', 'Alt Cont.', 'Last User', 'Home City', 'Select how often you want to call this contact', 'GDPR Status'], x) if e[1]]), axis=1)
contact.loc[contact['HomeCity'].notnull()]
contact.loc[contact['Sector'].notnull()]
cp7 = vcont.update_note_2(contact, dest_db, mylog)

# %% industry
sql ="""select skc.* from
(select sc.ObjectId, s.Sector from SectorInstances sc
left join Sectors s on s.SectorId = sc.SectorId) skc
join (select ContactId from Contacts where Descriptor = 1) c on c.ContactId = skc.ObjectId
where skc.Sector is not null"""
industry = pd.read_sql(sql, engine_mssql)
industry['contact_externalid'] = 'EUK'+industry['ObjectId']
industry['name'] = industry['Sector']
cp10 = vcont.insert_contact_industry_subindustry(industry, mylog)

# %% create distribution list
d_list = pd.read_sql("""
select
u.Email as owner, AccessModifier,ListName
from Collections_3 col
join (select ContactId from Contacts where Descriptor = 1) c on c.ContactId = col.ObjectId
left join Users u on col.Username = u.UserName
where charindex('^',ListName) > 0 or charindex('*',ListName) > 0
""", engine_mssql)
d_list = d_list.drop_duplicates()

d_list['name'] = d_list['ListName']
d_list['share_permission'] = d_list['AccessModifier'].apply(lambda x: x.lower() if x else x)
d_list.loc[d_list['share_permission']=='team']

d_list.loc[d_list['name']=='Candidates Aug - Dec 13']
tem2.loc[tem2['name']=='Candidates Aug - Dec 13']
vcont.create_distribution_list(d_list, mylog)

d_list_cont = pd.read_sql("""
select
ObjectId as contact_externalid
,ListName as name,u.Email as owner
from Collections_3 col
join (select ContactId from Contacts where Descriptor = 1) c on c.ContactId = col.ObjectId
left join Users u on col.Username = u.UserName
""", engine_mssql)
d_list_cont = d_list_cont.drop_duplicates()
d_list_cont['contact_externalid'] = 'EUK'+d_list_cont['contact_externalid']
# d_list_cont.loc[d_list_cont['name']=='Clients - Suspects - North West']
d_list_cont['owner'] = d_list_cont['owner'].fillna('')

df = d_list_cont
tem2 = df[['contact_externalid', 'name','owner']].drop_duplicates()
if 'insert_timestamp' not in tem2.columns:
    tem2['insert_timestamp'] = datetime.datetime.now()
tem2 = tem2.merge(vcont.contact, on=['contact_externalid'])

df_owner = pd.read_sql("select id as owner_id, email from user_account", vcont.ddbconn)
tem2 = tem2.merge(df_owner, left_on='owner', right_on='email', how='left')
tem2 = tem2.where(tem2.notnull(), None)
tem2['owner_id'] = tem2['owner_id'].map(lambda x: x if x else -10)
tem2.loc[tem2['owner']=='']
tem2.loc[tem2['owner_id'] == -10]

df_group = pd.read_sql("select id as contact_group_id,owner_id, name from contact_group", vcont.ddbconn)
tem2 = tem2.merge(df_group, on=['name','owner_id'])
tem2 = tem2.drop_duplicates()
tem2['contact_id'] = tem2['id']
tem = tem2[['contact_group_id', 'contact_id', 'insert_timestamp']].drop_duplicates()
tem = tem.drop_duplicates()

df_group_cont = pd.read_sql("select id, contact_group_id,contact_id from contact_group_contact", vcont.ddbconn)
tem = tem.merge(df_group_cont, on=['contact_group_id', 'contact_id'], how='left')
tem = tem.query("id.isnull()")
vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, vcont.ddbconn, ['contact_group_id', 'contact_id', 'insert_timestamp'], 'contact_group_contact', mylog)




d_list_team = pd.read_sql("""
select ListName as name, u.Email as owner
from Collections_3 col
join (select ContactId from Contacts where Descriptor = 1) c on c.ContactId = col.ObjectId
left join Users u on u.TeamId = col.TeamId
where AccessModifier = 'Team'
and (charindex('^',ListName) > 0 or charindex('*',ListName) > 0)
""", engine_mssql)
d_list_team = d_list_team.dropna()
df_owner = pd.read_sql("select id as owner_id, email from user_account", vcont.ddbconn)
tem2 = d_list_team.merge(df_owner, left_on='owner', right_on='email', how='left')

df_group = pd.read_sql("select id as contact_group_id, name from contact_group where share_permission = 3", vcont.ddbconn)
tem3 = tem2.merge(df_group, on='name', how='left')
tem3['insert_timestamp'] = datetime.datetime.now()
tem3['user_account_id'] = tem3['owner_id']
tem3 = tem3.loc[tem3['user_account_id'].notnull()]
tem3.loc[tem3['contact_group_id']==22406]
tem3=tem3.drop_duplicates()
vincere_custom_migration.psycopg2_bulk_insert_tracking(tem3, vcont.ddbconn, ['contact_group_id', 'user_account_id', 'insert_timestamp'], 'contact_group_user_account', mylog)


# %%
d_list = pd.read_sql("""
select
u.Email as owner, AccessModifier,ListName as name
from Collections col
join (select ContactId from Contacts where Descriptor = 1) c on c.ContactId = col.ObjectId
left join Users u on col.Username = u.UserName
""", engine_mssql)
d_list = d_list.drop_duplicates()
tem2 = d_list
tem = tem2.merge(pd.read_sql("select * from contact_group", vcont.ddbconn), on=['name'], how='left')
tem3 = tem.query("id.isnull()")
tem3 = tem3.drop_duplicates()

tem4 = tem.query("id.notnull()")
tem4 = tem4.drop_duplicates()
tem4.to_csv('contact_group.csv')


#%%
sk = pd.read_sql("""select skills, id from contact where external_id is not null and skills is not null""",connection)
sk['skills'] = sk['skills'].apply(lambda x: x.replace('\n',','))
vincere_custom_migration.psycopg2_bulk_update_tracking(sk, vcont.ddbconn, ['skills', ], ['id', ], 'contact', mylog)
