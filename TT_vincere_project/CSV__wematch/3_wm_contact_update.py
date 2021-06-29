# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
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
cf.read('wm_config.ini')
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
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %%
from common import vincere_contact
vcon = vincere_contact.Contact(connection)
contact = pd.read_csv(r'contact_import.csv')
contact['contact_externalid'] = contact['Contact ID']
contact = contact.where(contact.notnull(),None)
assert False
# %% primary mail
tem = contact[['contact_externalid','Primary Email']].dropna().drop_duplicates()
tem['email'] = tem['Primary Email']
vcon.update_email(tem,mylog)

# %% personal mail
tem = contact[['contact_externalid','Personal Email']].dropna().drop_duplicates()
tem['personal_email'] = tem['Personal Email']
vcon.update_personal_email(tem,mylog)

# %% mobile
tem = contact[['contact_externalid','Mobile']].dropna().drop_duplicates()
tem['mobile_phone'] = tem['Mobile']
vcon.update_mobile_phone(tem,mylog)

# %% home phone
tem = contact[['contact_externalid','Home Phone']].dropna().drop_duplicates()
tem['home_phone'] = tem['Home Phone']
vcon.update_home_phone(tem,mylog)

# %% linkedin
tem = contact[['contact_externalid','LinkedIn']].dropna().drop_duplicates()
tem['linkedin'] = tem['LinkedIn']
vcon.update_linkedin(tem,mylog)

# %% skill
tem = contact[['contact_externalid','Xing']].dropna().drop_duplicates()
tem['xing'] = tem['Xing']
vcon.update_xing(tem,mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'Title']].dropna().drop_duplicates()
tem['gender_title'] = tem['Title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
cp = vcon.update_gender_title(tem2, mylog)

# %% sub
tem = contact[['Primary Email','Subscribed Yes/No']].dropna().drop_duplicates()
tem['email'] = tem['Primary Email']
tem.loc[(tem['Subscribed Yes/No'] == 'Yes'), 'subscribed'] = 1
tem.loc[(tem['Subscribed Yes/No'] == 'No'), 'subscribed'] = 0
tem['rn'] = tem.groupby('email').cumcount()
tem = tem.loc[tem['Subscribed Yes/No'] == 0]
tem = tem.drop_duplicates()
vcon.email_subscribe(tem,mylog)