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
cf.read('ls_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# assert False
# %% international
sql = """
select ac.idassignment as job_externalid, i.value as international
from assignmentcode ac
left join International i on i.idInternational = ac.codeid
where idtablemd = '94b9bb6a-5f20-41bd-bc1d-59d34b2550ac'
"""
job = pd.read_sql(sql, engine_sqlite)

job['matcher'] = job['international'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('international.csv')
tem2['matcher'] = tem2['Document Category field in File Finder'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = job.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
# Guatemala Central America
tem3 = tem3.loc[tem3['Vincere Value'].notnull()]
tem3 = tem3.drop_duplicates()
cand_international_api = '5dd6a073f0ca6d9abd4a324e21316fe9'
cand = tem3[['job_externalid', 'Vincere Value']]
cand = cand.drop_duplicates()
cand = cand.where(cand.notnull(), None)
vincere_custom_migration.insert_job_muti_selection_checkbox(cand, 'job_externalid', 'Vincere Value', cand_international_api, connection)


