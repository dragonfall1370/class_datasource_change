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
cf.read('bower_config.ini')
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
select ID as contact_externalid
     , Telephone
     , "Job Title"
     , Recruitment
     , Training
     , ibLE
     , Notes
     , "Company Size"
     , Location
     , Source
     , "Also Feeder?"
     , "Old Company/Connections"
     , "Met?"
     , Stage, "Business Type", "Date Created"
from Contacts
""", engine_sqlite)
contact['contact_externalid'] = contact['contact_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% job title
jobtitle = contact[['contact_externalid', 'Job Title']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['Job Title']
vcont.update_job_title(jobtitle, mylog)

# %% primary phone
primary_phone = contact[['contact_externalid', 'Telephone']].dropna().drop_duplicates()
primary_phone['primary_phone'] = primary_phone['Telephone']
vcont.update_primary_phone(primary_phone, mylog)

# %% note
note = contact[[
    'contact_externalid',
    'Recruitment',
    'Training',
    'ibLE',
    'Notes',
    'Company Size',
    'Location',
    'Source',
    'Also Feeder?',
    'Old Company/Connections',
    'Met?', 'Stage'
                ]]

note['note'] = note[[
    'Recruitment',
    'Training',
    'ibLE',
    'Notes',
    'Company Size',
    'Location',
    'Source',
    'Also Feeder?',
    'Old Company/Connections',
    'Met?', 'Stage']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip([
    'Recruitment',
    'Training',
    'Ible',
    'Notes',
    'Company Size',
    'Location',
    'Source',
    'Also Feeder?',
    'Old Company/Connections',
    'Met?', 'Stage'], x) if e[1]]), axis=1)

cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% industries
industry = contact[['contact_externalid', 'Business Type']].dropna().drop_duplicates()
industry['name'] = industry['Business Type']
industry = industry.drop_duplicates().dropna()
industry['name'] = industry['name'].str.strip()
cp10 = vcont.insert_contact_industry(industry, mylog)

# %% reg date
reg_date = contact[['contact_externalid', 'Date Created']]
reg_date['reg_date'] = pd.to_datetime(reg_date['Date Created'])
vcont.update_reg_date(reg_date, mylog)