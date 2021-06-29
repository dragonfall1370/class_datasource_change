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
import phonenumbers
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
data_folder = cf['default'].get('data_folder')
# data_folder = '/Users/truongtung/Desktop'
sqlite_path = cf['default'].get('sqlite_path')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()


def get_correct_phone(phone):
    arr = []
    for match in phonenumbers.PhoneNumberMatcher(phone, "GB"):
        correct_phone = phonenumbers.format_number(match.number, phonenumbers.PhoneNumberFormat.INTERNATIONAL)
        arr.append(correct_phone)
    return ', '.join(arr)
assert False
# %% contact p phone
contact_pphone = pd.read_sql("""select id, phone as primary_phone from contact where nullif(phone,'') is not null""", connection)
contact_pphone['phone'] = contact_pphone['primary_phone'].apply(lambda x: get_correct_phone(x))
contact_pphone = contact_pphone.loc[contact_pphone['phone']!='']
vincere_custom_migration.psycopg2_bulk_update_tracking(contact_pphone, connection, ['phone', ], ['id', ], 'contact', mylog)

# %% contact mobile phone
contact_mphone = pd.read_sql("""select id, mobile_phone as mobile from contact where nullif(mobile_phone,'') is not null""", connection)
contact_mphone['mobile_phone'] = contact_mphone['mobile'].apply(lambda x: get_correct_phone(x))
contact_mphone = contact_mphone.loc[contact_mphone['mobile_phone']!='']
vincere_custom_migration.psycopg2_bulk_update_tracking(contact_mphone, connection, ['mobile_phone', ], ['id', ], 'contact', mylog)

# %% candidate p phone
candidate_pphone = pd.read_sql("""select id, phone as primary_phone from candidate where nullif(phone,'') is not null""", connection)
candidate_pphone['phone'] = candidate_pphone['primary_phone'].apply(lambda x: get_correct_phone(x))
candidate_pphone = candidate_pphone.loc[candidate_pphone['phone']!='']
vincere_custom_migration.psycopg2_bulk_update_tracking(candidate_pphone, connection, ['phone', ], ['id', ], 'candidate', mylog)

# %% candidate mobile phone
candidate_mphone = pd.read_sql("""select id, phone2 as mobile from candidate where nullif(phone2,'') is not null""", connection)
candidate_mphone['phone2'] = candidate_mphone['mobile'].apply(lambda x: get_correct_phone(x))
candidate_mphone = candidate_mphone.loc[candidate_mphone['phone2']!='']
vincere_custom_migration.psycopg2_bulk_update_tracking(candidate_mphone, connection, ['phone2', ], ['id', ], 'candidate', mylog)



# %%



