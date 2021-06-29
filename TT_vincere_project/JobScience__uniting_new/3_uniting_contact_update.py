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
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_contact
vcont = vincere_contact.Contact(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)

#%% mailing address
contact = pd.read_sql("""
select c.Id as contact_externalid,
       c.Phone,
       c.MobilePhone,
       c.Employee_Timesheet_Email__c,
       c.Current_JobTitle__c,
       c.Department,
       c.MailingStreet,
       c.MailingCity,
       c.MailingState,
       c.MailingPostalCode,
       c.MailingCountry,
       c.OtherCity,
       c.OtherPostalCode,
       c.OtherState,
       c.AccountId as company_externalid,
       u.Email as owner,
       Email_Marketing__c, Phone_Marketing__c, SMS_Marketing__c
from Contact_new c
left join User_new u on c.OwnerId = u.Id
;
""", engine_sqlite)
assert False
# %% location name/address
contact['location_name'] = contact[['MailingStreet', 'MailingCity', 'MailingState', 'MailingPostalCode', 'MailingCountry']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['address'] = contact.location_name

# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address']].drop_duplicates()
tem1 = comaddr.loc[comaddr.address != '']
cp1 = vcom.insert_company_location(tem1, mylog)

# %%
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% city
comaddr = contact[['company_externalid', 'MailingStreet', 'MailingCity', 'MailingPostalCode', 'MailingState', 'MailingCountry', 'address']].drop_duplicates()
comaddr['city'] = comaddr['MailingCity']
cp3 = vcom.update_location_city_2(comaddr, dest_db, mylog)

# %% postcode
comaddr['post_code'] = comaddr['MailingPostalCode']
cp4 = vcom.update_location_post_code_2(comaddr, dest_db, mylog)

# %% state
comaddr['state'] = comaddr['MailingState']
cp5 = vcom.update_location_state_2(comaddr, dest_db, mylog)

# %% country
comaddr['country_code'] = comaddr.MailingCountry.map(vcom.get_country_code)
comaddr['country'] = comaddr['MailingCountry']
cp6 = vcom.update_location_country_2(comaddr, dest_db, mylog)

# %% insert current location
curraddr = contact[['contact_externalid','OtherCity', 'OtherPostalCode', 'OtherState']]
curraddr['address'] = contact[['OtherCity', 'OtherPostalCode', 'OtherState']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem3 = curraddr.loc[curraddr.address != '']
vcont.insert_current_location(tem3, mylog)

# %% update current location post code
curraddr['post_code'] = curraddr['OtherPostalCode']

df = curraddr
logger = mylog
tem2 = df[['contact_externalid', 'post_code']].dropna()
tem2 = tem2.merge(pd.read_sql("select id, external_id as contact_externalid, contact_owners, current_location_id from contact", vcont.ddbconn), on=['contact_externalid'])
tem2 = tem2.loc[tem2['current_location_id'].notnull()]
tem2['current_location_id'] = tem2['current_location_id'].astype(int)
tem2['id'] = tem2['current_location_id']

tem2.info()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcont.ddbconn, ['post_code', ], ['id', ], 'common_location', logger)
# vcont.update_current_location_post_code(curraddr, mylog)

# %% update current location state
curraddr['state'] = curraddr['OtherState']

df = curraddr
logger = mylog
tem2 = df[['contact_externalid', 'state']].dropna()
tem2 = tem2.merge(pd.read_sql("select id, external_id as contact_externalid, contact_owners, current_location_id from contact", vcont.ddbconn), on=['contact_externalid'])
tem2 = tem2.loc[tem2['current_location_id'].notnull()]
tem2['current_location_id'] = tem2['current_location_id'].astype(int)
tem2['id'] = tem2['current_location_id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcont.ddbconn, ['state', ], ['id', ], 'common_location', logger)
# vcont.update_current_location_state(curraddr, mylog)

# %% job title
tem = contact[['contact_externalid', 'Current_JobTitle__c']].dropna()
tem['job_title'] = tem['Current_JobTitle__c']
vcont.update_job_title(tem, mylog)

# %% department
tem = contact[['contact_externalid', 'Department']].dropna()
tem['department'] = tem['Department']
vcont.update_department(tem, mylog)

# %% primary phone
contact['primary_phone'] = contact['Phone']
vcont.update_primary_phone(contact, mylog)

# %% mobile phone
contact['mobile_phone'] = contact['MobilePhone']
vcont.update_mobile_phone(contact, mylog)

# %% email
contact['personal_email'] = contact['Employee_Timesheet_Email__c']
vcont.update_personal_email(contact, mylog)

# %% note
contact.loc[contact['Email_Marketing__c']=='0', 'Email_Marketing__c'] = 'No'
contact.loc[contact['Email_Marketing__c']=='1', 'Email_Marketing__c'] = 'Yes'
contact.loc[contact['Phone_Marketing__c']=='0', 'Phone_Marketing__c'] = 'No'
contact.loc[contact['Phone_Marketing__c']=='1', 'Phone_Marketing__c'] = 'Yes'
contact.loc[contact['SMS_Marketing__c']=='0', 'SMS_Marketing__c'] = 'No'
contact.loc[contact['SMS_Marketing__c']=='1', 'SMS_Marketing__c'] = 'Yes'
note = contact[[
    'contact_externalid',
'Email_Marketing__c',
'Phone_Marketing__c',
'SMS_Marketing__c']]

prefixes = [
'UA ID',
    'Email Opt In',
    'Phone Opt In',
    'SMS Opt In'
]
note['note'] = note.apply(lambda x: '\n■ '.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
vcont.update_note_2(note, dest_db, mylog)
