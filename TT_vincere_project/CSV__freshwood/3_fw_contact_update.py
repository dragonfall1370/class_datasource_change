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
cf.read('fw_config.ini')
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
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_contact
vcont = vincere_contact.Contact(connection)

#%% mailing address
contact = pd.read_csv(os.path.join(standard_file_upload, '4_contact.csv'))
contact['contact_externalid'] = contact['Contact ID']
assert False
# %% personal email
tem = contact[['contact_externalid','Personal Email']].dropna()
tem['personal_email'] = tem['Personal Email']
vcont.update_personal_email(tem, mylog)

# %% email
tem = contact[['contact_externalid','Primary Email']].dropna()
tem['email'] = tem['Primary Email']
vcont.update_email(tem, mylog)

# %% skill
tem = contact[['contact_externalid','Skills (Comma Delimited)']].dropna()
tem['skills'] = tem['Skills (Comma Delimited)']
vcont.update_skills(tem, mylog)


cont = pd.read_sql("""select id, email from contact where external_id is not null """,connection)
cont = cont.dropna()
company['name'] = company['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(company, connection, ['name'], ['id'], 'company', mylog)