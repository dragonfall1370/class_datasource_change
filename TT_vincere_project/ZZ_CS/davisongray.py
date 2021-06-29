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
from pandas.io.json import json_normalize
import json

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('dn_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

# company = pd.read_sql("""select id as company_id, name, external_id from company""", connection)
# company['matcher'] = company['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

contact = pd.read_csv('RPA Data Master Copy.csv')
# contact['matcher'] = contact['Company Name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# tem = contact.merge(company, left_on='Company Name', right_on='name', how='left')
# tem.loc[tem['contact-externalId']==42874]
# contact.loc[contact['ID']==40480]
# tem.rename(columns={
#     'ID': 'contact-externalId',
#     'external_id': 'contact-companyId',
#     'Last Name': 'contact-lastName',
#     'First Name': 'contact-firstName',
#     'Job Title': 'contact-jobTitle',
#      'Email': 'contact-email',
#      # 'EMC_ACC_EMAILS': 'contact-owners',
# }, inplace=True)
# tem[['contact-externalId','contact-companyId','contact-lastName','contact-firstName','contact-jobTitle', 'contact-email']].to_csv('contact_import.csv',index=False)


from common import vincere_contact
vcont = vincere_contact.Contact(connection)
assert False
contact['id'] = contact['ID']
tem = contact[['id','Email']].dropna()
tem['email'] = tem['Email'].apply(lambda x: x.strip())
# vcont.update_email(tem,mylog)
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcont.ddbconn, ['email', ], ['id', ], 'contact', mylog)

tem = contact[['id','Phone Number']].dropna()
tem['mobile_phone'] = tem['Phone Number'].apply(lambda x: x.replace('\n',','))
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcont.ddbconn, ['mobile_phone', ], ['id', ], 'contact', mylog)

tem['phone'] = tem['Phone Number'].apply(lambda x: x.split('\n')[0])
tem['phone'] = tem['phone'].apply(lambda x: x.replace('(Direct)',''))
tem['phone'] = tem['phone'].apply(lambda x: x.replace('(HQ)',''))
tem['phone'] = tem['phone'].apply(lambda x: x.replace('(Mobile)',''))
tem['phone'] = tem['phone'].apply(lambda x: x.strip())
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcont.ddbconn, ['phone', ], ['id', ], 'contact', mylog)