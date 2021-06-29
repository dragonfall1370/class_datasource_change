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
cf.read('yc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
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
# from common import parse_gender_title

#%% mailing address
contact = pd.read_sql("""
select cont.Id as contact_externalid
, cont.Salutation
, cont.Birthdate
, cont.Department
, r.Name as record_type 
, cont.Title
, cont.MailingStreet
, cont.MailingState
, cont.MailingCity
, cont.MailingPostalCode
, cont.MailingCountry
, com.Id as company_externalid
, cont.Phone
, cont.MobilePhone
, cont.E_mail_2__c
, m.FirstName || ' ' || m.LastName as modifiedby
, u.LastModifiedDate
, cont.CreatedDate
from Contact cont
join RecordType r on (cont.RecordTypeId || 'AA2') = r.Id
left join Account com on (cont.AccountId = com.Id and com.IsDeleted=0)
left join "User" u on cont.CreatedById = u.Id
left join "User" m on cont.LastModifiedById = m.Id
where cont.IsDeleted = 0
and r.Name = 'Contact'
;
""", engine_sqlite)
assert False
# %% location name/address
contact['location_name'] = contact[['MailingStreet', 'MailingState', 'MailingCity', 'MailingPostalCode', 'MailingCountry']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['address'] = contact.location_name

# %% assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address']].drop_duplicates()
cp1 = vcom.insert_company_location(comaddr, mylog)

# %% assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% city
comaddr = contact[['company_externalid', 'MailingStreet', 'MailingState', 'MailingCity', 'MailingPostalCode', 'MailingCountry', 'address']].drop_duplicates()
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
comaddr['country'] = comaddr.MailingCountry
cp6 = vcom.update_location_country_2(comaddr, dest_db, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'Salutation']].dropna().drop_duplicates()
tem['gender_title'] = tem['Salutation']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
cp = vcont.update_gender_title(tem, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'Title']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['Title']
vcont.update_job_title(jobtitle, mylog)

# %% department
dep = contact[['contact_externalid', 'Department']].dropna().drop_duplicates()
dep['department'] = dep['Department']
vcont.update_department(dep, mylog)

# %% personel email
personal_email = contact[['contact_externalid', 'E_mail_2__c']].dropna().drop_duplicates()
personal_email['personal_email'] = personal_email['E_mail_2__c']
personal_email = personal_email.loc[personal_email['personal_email'] != '']
vcont.update_personal_email(personal_email, mylog)

# %% primary phone
primary_phone = contact[['contact_externalid', 'Phone']].dropna().drop_duplicates()
primary_phone['primary_phone'] = primary_phone['Phone']
vcont.update_primary_phone(primary_phone, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'MobilePhone']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['MobilePhone']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% note
note = contact[[
    'contact_externalid',
    'Birthdate',
    'record_type',
    'modifiedby',
    'LastModifiedDate',
                ]]

prefixes = [
'YC ID',
'Birthdate',
'Contact Record Type',
'Last Modified By',
'Last Modified Date'
]

note['LastModifiedDate'] = pd.to_datetime(note['LastModifiedDate'])
note['LastModifiedDate'] = note['LastModifiedDate'].apply(lambda x: datetime.datetime.strftime(x, '%d/%m/%Y %H:%M'))

note = note.where(note.notnull(), None)
note['note'] = note.apply(lambda x: '\n'.join([': '.join(e) for e in zip(prefixes, x) if e[1]]), axis='columns')
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% reg date
reg_date = contact[['contact_externalid', 'CreatedDate']].dropna().drop_duplicates()
reg_date['CreatedDate'] = pd.to_datetime(reg_date['CreatedDate'])
reg_date['CreatedDate'] = reg_date['CreatedDate'].apply(lambda x: datetime.datetime.strftime(x, '%m/%d/%Y %H:%M'))
reg_date['reg_date'] = pd.to_datetime(reg_date['CreatedDate'])
vcont.update_reg_date(reg_date, mylog)
