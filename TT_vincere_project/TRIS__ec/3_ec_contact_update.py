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
cf.read('ec_config.ini')
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
select c.PersonID as contact_externalid,
       c.PersonFirstName,
       c.PersonSurname,
       c.PersonWorkEMail,
       c.ClientID as company_externalid,
       tT.TName,
       nullif(convert(varchar,c.PersonHomeTelephone),'') as PersonHomeTelephone,
       nullif(convert(varchar,c.PersonWorkTelephone),'') as PersonWorkTelephone,
       nullif(convert(varchar,c.PersonOtherPhone),'') as PersonOtherPhone,
       nullif(convert(varchar,c.PersonMobileTelephone),'') as PersonMobileTelephone,
       nullif(convert(varchar,c.PersonCreateDate),'') as PersonCreateDate,
       nullif(convert(varchar,c.PersonLastUpdate),'') as PersonLastUpdate,
       nullif(convert(varchar,c.PersonHomeEMail),'') as PersonHomeEMail,
       nullif(convert(varchar,c.PersonKnownAs),'') as PersonKnownAs,
       nullif(convert(varchar,DateOfBirth),'') as DateOfBirth,
       nullif(convert(varchar,SkypeAccount),'') as SkypeAccount,
       nullif(convert(varchar,LinkedInUrl),'') as LinkedInUrl,
       nullif(convert(varchar,FacebookAccount),'') as FacebookAccount,
       nullif(convert(varchar,TwitterAccount),'') as TwitterAccount,
       nullif(convert(varchar,c.ContactJobTitle),'') as ContactJobTitle,
       nullif(convert(varchar,c.ContactDepartment),'') as ContactDepartment,
       nullif(convert(varchar,c.ContactNotes),'') as ContactNotes,
       nullif(convert(varchar,ContactOnHold),'') as ContactOnHold,
       nullif(convert(varchar,ContactReasonOnHold),'') as ContactReasonOnHold
from Contact c
left join tblTitle tT on c.TitleID = tT.TitleID
left join tblPerson p on p.PersonID = c.PersonID
left join tblContact tc on tc.ContactID = c.PersonID
""", engine_mssql)
contact['contact_externalid'] =contact['contact_externalid'].apply(lambda x: str(x) if x else x)
contact['company_externalid'] =contact['company_externalid'].apply(lambda x: str(x).split('.')[0] if x else x)
assert False
# # %% location name/address
# contact['address'] = contact[['Address1', 'Address2', 'Address3', 'City', 'Add_Postcode','County','Country']] \
#     .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# contact['location_name'] = contact[['Add_Postcode', 'SubLocation', 'Location']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# # contact.loc[contact['contact_externalid']=='496623-2782-1358']
# # contact.loc[contact['location_name']=='', 'location_name'] = contact['address']
# # vcom.update_location_name_2(contact,dest_db,mylog)
# # %%
# # assign contacts's addresses to their companies
# comaddr = contact[['company_externalid', 'address','location_name']].drop_duplicates()
# cp1 = vcom.insert_company_location(comaddr, mylog)
#
# # %%
# # assign the new addesses to contacts work location
# tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
# cp2 = vcont.insert_contact_work_location(tem2, mylog)
#
# # %% city
# comaddr = contact[['company_externalid', 'address', 'City', 'Add_Postcode', 'County', 'Country']].drop_duplicates()
# comaddr['city'] = comaddr.City
# cp3 = vcom.update_location_city_2(comaddr, dest_db, mylog)
#
# # %% postcode
# comaddr['post_code'] = comaddr.Add_Postcode
# cp4 = vcom.update_location_post_code_2(comaddr, dest_db, mylog)
#
# # %% state
# comaddr['state'] = comaddr.County
# comaddr['district'] = comaddr.County
# cp5 = vcom.update_location_state_2(comaddr, dest_db, mylog)
# cp5 = vcom.update_location_district_2(comaddr, dest_db, mylog)
#
# # %% country
# comaddr['country_code'] = comaddr.Country.map(vcom.get_country_code)
# comaddr['country'] = comaddr.Country
# cp6 = vcom.update_location_country_2(comaddr, dest_db, mylog)

# %% insert department company
tem = contact[['company_externalid', 'ContactDepartment']].drop_duplicates().dropna()
tem['department_name'] = tem['ContactDepartment']
cp7 = vcom.insert_department(tem, mylog)

# %% insert department contact
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'ContactDepartment']].drop_duplicates().dropna()
tem2['department_name'] = tem2['ContactDepartment']
cp8 = vcont.insert_contact_department(tem2, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'TName']].dropna().drop_duplicates()
tem['gender_title'] = tem['TName']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
cp = vcont.update_gender_title(tem, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'ContactJobTitle']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['ContactJobTitle']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'PersonWorkTelephone']].dropna().drop_duplicates()
tem['primary_phone'] = tem['PersonWorkTelephone']
vcont.update_primary_phone(tem, mylog)

# %% switchboard
# tem = contact[['contact_externalid', 'WorkTel']].dropna().drop_duplicates()
# tem['switchboard_phone'] = tem['WorkTel']
# vcont.update_switchboard_phone(tem, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'PersonMobileTelephone']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['PersonMobileTelephone']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
tem = contact[['contact_externalid', 'PersonHomeTelephone']].dropna().drop_duplicates()
tem['home_phone'] = tem['PersonHomeTelephone']
vcont.update_home_phone(tem, mylog)

# %% personal email
tem = contact[['contact_externalid', 'PersonHomeEMail']].dropna().drop_duplicates()
tem['personal_email'] = tem['PersonHomeEMail']
vcont.update_personal_email(tem, mylog)

# %% primary email
# email = contact[['contact_externalid', 'EmailWork']].dropna().drop_duplicates()
# email['email'] = email[['EmailWork']]
# vcont.update_email(email, mylog)

# %% linkedin
tem = contact[['contact_externalid', 'LinkedInUrl']].dropna().drop_duplicates()
tem['linkedin'] = tem['LinkedInUrl']
vcont.update_linkedin(tem, mylog)

# %% skype
tem = contact[['contact_externalid', 'SkypeAccount']].dropna().drop_duplicates()
tem['skype'] = tem['SkypeAccount']
vcont.update_skype(tem, mylog)

# %% fb
tem = contact[['contact_externalid', 'FacebookAccount']].dropna().drop_duplicates()
tem['facebook'] = tem['FacebookAccount']
vcont.update_facebook(tem, mylog)

# %% twitter
tem = contact[['contact_externalid', 'TwitterAccount']].dropna().drop_duplicates()
tem['twitter'] = tem['TwitterAccount']
vcont.update_twitter(tem, mylog)

# %% reg date
tem = contact[['contact_externalid', 'PersonCreateDate']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['PersonCreateDate'])
vcont.update_reg_date(tem, mylog)

# %% skills
# skill = pd.read_sql("""
# select c.ContactId as contact_externalid, Skill as skills
# from Contacts c
# left join (select ObjectId, Skill
# from SkillInstances si
# left join Skills s on s.SkillId = si.SkillId) sk on sk.ObjectId = c.ContactId
# where Descriptor = 1
# and Skill is not null
# """, engine_mssql)
# vcont.update_skills(skill, mylog)
# %% dob
tem = contact[['contact_externalid', 'DateOfBirth']].dropna().drop_duplicates()
tem['date_of_birth'] = pd.to_datetime(tem['DateOfBirth'])
vcont.update_dob(tem, mylog)

# %% last activity date
tem = contact[['contact_externalid', 'PersonLastUpdate']].dropna().drop_duplicates()
tem['last_activity_date'] = pd.to_datetime(tem['PersonLastUpdate'])
vcont.update_last_activity_date(tem, mylog)

# %% active
tem = contact[['contact_externalid', 'ContactOnHold']].dropna().drop_duplicates()
tem.loc[tem['ContactOnHold']=='1', 'active'] = 0
tem2 = tem[['contact_externalid','active']].dropna()
vcont.update_active(tem, mylog)

# # %% middle name
# tem = contact[['contact_externalid', 'MiddleName']].dropna().drop_duplicates()
# tem['middle_name'] = tem['MiddleName']
# vcont.update_middle_name(tem, mylog)

# %% preferred name
tem = contact[['contact_externalid', 'PersonKnownAs']].dropna().drop_duplicates()
tem['preferred_name'] = tem['PersonKnownAs']
vcont.update_preferred_name(tem, mylog)

# %% note
contact['note'] = contact[['contact_externalid', 'PersonOtherPhone'
    , 'ContactReasonOnHold', 'ContactNotes']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID', 'Other Phone', 'Reason On Hold', 'Information'], x) if e[1]]), axis=1)
cp7 = vcont.update_note_2(contact, dest_db, mylog)

# %% industry
sql = """
select P.idperson as contact_externalid, idIndustry_String_List
               from personx P
where isdeleted = '0'
"""
contact_industries = pd.read_sql(sql, engine_sqlite)
contact_industries = contact_industries.dropna()

industry = contact_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_industries[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='idIndustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idIndustry'] = industry['idIndustry'].str.lower()

industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()

industry_1 = industry.merge(industries, on='idIndustry')
industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact_industries = industry_1.merge(industries_csv, on='matcher')

contact_industries_2 = contact_industries[['contact_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
contact_industries_2 = contact_industries_2.where(contact_industries_2.notnull(),None)
tem1 = contact_industries_2[['contact_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
cp10 = vcont.insert_contact_industry_subindustry(tem1, mylog)

tem2 = contact_industries_2[['contact_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
cp10 = vcont.insert_contact_industry_subindustry(tem2, mylog)


