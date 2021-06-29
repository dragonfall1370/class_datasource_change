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
cf.read('tj_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
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
select ContactID, ClientID
    , nullif(MiddleName,'') as MiddleName
         , nullif(KnownAs,'') as KnownAs
         , s.Description as title
         , nullif(Phone,'') as Phone
         , nullif(Fax,'') as Fax
         , nullif(Mobile,'') as Mobile
         , nullif(LinkedInURL,'') as LinkedInURL
     , nullif(SkypeID,'') as SkypeID
         , nullif(Department,'') as Department
         , nullif(HomePhone,'') as HomePhone
         , nullif(RealTitle,'') as RealTitle
         , nullif(OtherPhone,'') as OtherPhone
from ClientContacts cc
left join Salutations s on cc.Title = s.SalutationID
""", engine_mssql)
contact['contact_externalid'] = contact['ContactID'].apply(lambda x: str(x) if x else x)
contact['company_externalid'] = contact['ClientID'].apply(lambda x: str(x) if x else x)
contact['company_externalid'] = 'FC'+contact['company_externalid']
contact['contact_externalid'] = 'FC'+contact['contact_externalid']
assert False
# %% location name/address
contact_location = pd.read_sql("""
    select  ca.ContactID, ClientID
         , nullif(Address1,'') as Address1
         , nullif(Address2,'') as Address2
         , nullif(Address3,'') as Address3
         , nullif(PostCode,'') as PostCode
         , nullif(Town,'') as Town
         , nullif(County,'') as County
         , nullif(Country,'') as Country
    from ContactAddresses ca
join ClientContacts CC2 on ca.ContactID = CC2.ContactID
""", engine_mssql)
contact_location['contact_externalid'] = contact_location['ContactID'].apply(lambda x: str(x) if x else x)
contact_location['company_externalid'] = contact_location['ClientID'].apply(lambda x: str(x) if x else x)
contact_location['company_externalid'] = 'FC'+contact_location['company_externalid']
contact_location['contact_externalid'] = 'FC'+contact_location['contact_externalid']
contact_location['address'] = contact_location[['Address1', 'Address2','Address3','Town','PostCode','County','Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact_location['location_name'] = contact_location['address']

# %%
# assign contacts's addresses to their companies
comaddr = contact_location[['company_externalid', 'address','location_name','contact_externalid','Address1', 'Address2','Address3','Town','PostCode','County','Country']].drop_duplicates()
comaddr = comaddr.loc[comaddr['company_externalid'].notnull()]
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'Address1']].dropna().drop_duplicates()
tem['address_line1'] = tem.Address1
tem['rn'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['rn']<100]
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 2
tem = comaddr[['company_externalid', 'address', 'Address2','Address3']]
tem['address_line2'] = tem[[ 'Address2','Address3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['address_line2']!='']
tem['rn'] = tem['address_line2'].apply(lambda x: len(x))
tem = tem.loc[tem['rn']<100]
cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'Town']].dropna().drop_duplicates()
tem['city'] = tem.Town
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'PostCode']].dropna().drop_duplicates()
tem['post_code'] = tem['PostCode']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'County']].dropna().drop_duplicates()
tem['state'] = tem.County
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'Country']].dropna().drop_duplicates()
tem['country_code'] = tem.Country.map(vcom.get_country_code)
tem['country'] = tem.Country
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% insert department company
tem = contact[['company_externalid', 'Department']].drop_duplicates().dropna()
tem['department_name'] = tem['Department']
cp7 = vcom.insert_department(tem, mylog)

# %% insert department contact
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'Department']].drop_duplicates().dropna()
tem2['department_name'] = tem2['Department']
cp8 = vcont.insert_contact_department(tem2, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'RealTitle']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['RealTitle']
jobtitle['job_title'] = jobtitle['job_title'].apply(lambda x: x.replace('\r\n',''))
jobtitle['rn'] = jobtitle['job_title'].apply(lambda x: len(x))
jobtitle.loc[jobtitle['rn']>100]
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'Phone']].drop_duplicates().dropna()
tem['primary_phone'] = tem['Phone']
vcont.update_primary_phone2(tem, dest_db, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'Mobile']].drop_duplicates().dropna()
mobile_phone['mobile_phone'] = mobile_phone['Mobile']
vcont.update_mobile_phone2(mobile_phone, dest_db, mylog)

# %% reg date
# tem = contact[['contact_externalid', 'PersonCreateDate']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['PersonCreateDate'])
# vcont.update_reg_date(tem, mylog)

# %% preferred name
tem = contact[['contact_externalid', 'KnownAs']].drop_duplicates().dropna()
tem['preferred_name'] = tem['KnownAs']
vcont.update_preferred_name(tem, mylog)

# %% linkedin
tem = contact[['contact_externalid', 'LinkedInURL']].drop_duplicates().dropna()
tem['linkedin'] = tem['LinkedInURL']
vcont.update_linkedin(tem, mylog)

# %% skype
tem = contact[['contact_externalid', 'SkypeID']].drop_duplicates().dropna()
tem['skype'] = tem['SkypeID']
vcont.update_skype(tem, mylog)

# %% personal phone
tem = contact[['contact_externalid', 'HomePhone','OtherPhone']]
tem['home_phone'] = tem[['HomePhone','OtherPhone']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem2 = tem.loc[tem['home_phone']!='']
vcont.update_home_phone2(tem2, dest_db, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'title']].dropna().drop_duplicates()
tem['gender_title'] = tem['title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
cp = vcont.update_gender_title(tem2, mylog)

# %% note
note = contact[['contact_externalid','Fax']]
comment = pd.read_sql("""
    select ContactID, Comment from ContactComments
""", engine_mssql)
comment['contact_externalid'] = comment['ContactID'].apply(lambda x: str(x) if x else x)
comment['contact_externalid'] = 'FC'+comment['contact_externalid']
comment = comment.groupby('contact_externalid')['Comment'].apply(lambda x: '\n\n'.join(x)).reset_index()
comment['Comment'] = '---Comments---\n' + comment['Comment']
note = note.merge(comment, on='contact_externalid', how='left')
note = note.where(note.notnull(),None)
note['note'] = note[['contact_externalid','Fax','Comment']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Fircroft Contact ID','Work Fax',''], x) if e[1]]), axis=1)
note['note'] = note['note'].apply(lambda x: x.replace('Fircroft Contact ID: FC','Fircroft Contact ID: '))
# note['note'] = note['Notes']
cp7 = vcont.update_note_2(note, dest_db, mylog)
