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
cf.read('at_config.ini')
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
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# %% info
contact = pd.read_sql("""
select concat('AT',company_id) as company_externalid
     , concat('AT',contact_id) as contact_externalid
     , nullif(trim(contact_phone),'') as contact_phone
     , nullif(trim(mobile_phone),'') as mobile_phone
     , nullif(trim(contact_position),'') as contact_position
     , nullif(trim(contact_jobtitle),'') as contact_jobtitle
     , nullif(trim(contact_address1),'') as contact_address1
     , nullif(trim(contact_address2),'') as contact_address2
     , nullif(trim(contact_address3),'') as contact_address3
     , nullif(trim(contact_city),'') as contact_city
     , nullif(trim(contact_postcode),'') as contact_postcode
     , nullif(trim(contact_region),'') as contact_region
from client_contact
""", engine_mssql)
assert False
# %% location name/address
contact['address'] = contact[['contact_address1', 'contact_address2','contact_address3','contact_city','contact_region','contact_postcode']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['location_name'] = contact['address']

# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address','location_name','contact_address1', 'contact_address2','contact_address3','contact_city','contact_region','contact_postcode']].drop_duplicates()
comaddr = comaddr.loc[comaddr['company_externalid'].notnull()]
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %%
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
tem2 = tem2.loc[tem2['address']!='']
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% addr 1
# tem = comaddr[['company_externalid', 'address', 'ADDRESS']].dropna().drop_duplicates()
# tem['address_line1'] = tem.ADDRESS
# cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 1
# tem = comaddr[['company_externalid', 'address', 'ADDRESS2']].dropna().drop_duplicates()
# tem['address_line2'] = tem.ADDRESS2
# cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'contact_city']].dropna().drop_duplicates()
tem['city'] = tem.contact_city
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'contact_postcode']].dropna().drop_duplicates()
tem['post_code'] = tem['contact_postcode']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'contact_region']].dropna().drop_duplicates()
tem['state'] = tem.contact_region
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
# tem = comaddr[['company_externalid', 'address', 'COUNTRY']].dropna().drop_duplicates()
# tem['country_code'] = tem.COUNTRY.map(vcom.get_country_code)
# tem['country'] = tem.COUNTRY
# tem.loc[tem['country_code']=='']
# cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% insert department company
# tem = contact[['company_externalid', 'DEPARTMENT']].drop_duplicates().dropna()
# tem['department_name'] = tem['DEPARTMENT']
# cp7 = vcom.insert_department(tem, mylog)

# %% insert department contact
# assign the new addesses to contacts work location
# tem2 = contact[['contact_externalid', 'company_externalid', 'DEPARTMENT']].drop_duplicates().dropna()
# tem2['department_name'] = tem2['DEPARTMENT']
# cp8 = vcont.insert_contact_department(tem2, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'contact_position']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['contact_position']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'contact_phone']].drop_duplicates().dropna()
tem['primary_phone'] = tem['contact_phone']
tem['primary_phone'] = tem['primary_phone'].apply(lambda x: x.replace('=','').replace('"',''))
tem = tem.loc[tem['primary_phone']!='']
vcont.update_primary_phone(tem, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'mobile_phone']].drop_duplicates().dropna()
mobile_phone['mobile_phone'] = mobile_phone['mobile_phone']
mobile_phone['mobile_phone'] = mobile_phone['mobile_phone'].apply(lambda x: x.replace('=','').replace('"',''))
mobile_phone = mobile_phone.loc[mobile_phone['mobile_phone']!='']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% reg date
# tem = contact[['contact_externalid', 'PersonCreateDate']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['PersonCreateDate'])
# vcont.update_reg_date(tem, mylog)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = contact[['contact_externalid', 'TITLE']].dropna().drop_duplicates()
# tem['gender_title'] = tem['TITLE']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
# cp = vcont.update_gender_title(tem2, mylog)

# %% note
note = contact[['contact_externalid','contact_jobtitle']].dropna().drop_duplicates()
note['note'] = note[['contact_jobtitle']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['contact jobtitle'], x) if e[1]]), axis=1)
# note['note'] = note['Notes']
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% status
# tem = contact[['contact_externalid', 'STATUS']].dropna().drop_duplicates()
# tem['name'] = tem['STATUS']
# vcont.add_contact_status(tem, mylog)
