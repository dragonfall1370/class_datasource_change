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
cf.read('dj_config.ini')
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

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
gdpr = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     , UGDPR
from CONTACT1 c1
left join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'Placed'
,'Do not Use'
,'Candidate') and UGDPR is not null
""", engine_mssql)
assert False
gdpr['external_id'] = gdpr['candidate_externalid']
gdpr['portal_status'] = 1
gdpr['explicit_consent'] = 1
gdpr['exercise_right'] = 3  # 3: Other [Person informed how to exercise their rights]

gdpr['request_through'] = 1 # 6: Other
gdpr['obtained_through'] = 1 # 6: Other
gdpr['obtained_through_date'] = pd.to_datetime(gdpr['UGDPR'])
gdpr['request_through_date'] = pd.to_datetime(gdpr['UGDPR'])
gdpr['expire'] = 0

cols = ['candidate_id',
        'request_through_date',  # 3: Other [Person informed how to exercise their rights]
        'request_through',  # 6: Other
        'obtained_through',  # 6: Other
        'obtained_through_date',
        'explicit_consent',  # 0: No
        'portal_status',  # 1:Consent given / 2:Pending [Consent to keep]
        'exercise_right',  # [Notes | Journal]
        'expire']
# assert False
vincere_custom_migration.insert_candidate_gdpr_compliance(gdpr, connection, cols)