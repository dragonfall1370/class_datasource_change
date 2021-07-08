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
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
j_prod = pd.read_sql("""select id, name from position_description""", engine_postgre_review)
j_prod['check'] = j_prod['name'].apply(lambda x: re.findall(r'\(\d+\)',x))
j_prod['lenght'] = j_prod['check'].apply(lambda x: len(x))
j_prod = j_prod.loc[j_prod['lenght']>0]
j_prod['name'] = j_prod['name'].apply(lambda x: x.split('(')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, connection, ['name'], ['id'], 'position_description', mylog)

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
candidate = pd.read_sql("""
 select
 concat('', p.ItemId) as candidate_externalid,
        DateModified
 from People p
 left join Users u on p.OwnerId = u.ItemId
 where 
 p.TypeCandidateChecked = 1
 or (p.TypeClientChecked = 0 and p.TypeCandidateChecked = 0)
 or (p.TypeClientChecked is null and p.TypeCandidateChecked is null)
 or (p.TypeClientChecked = 0 and p.TypeCandidateChecked is null)
 or (p.TypeClientChecked is null and p.TypeCandidateChecked = 0)
""", engine_mssql)
candidate['last_activity_date'] = pd.to_datetime(candidate['DateModified'])
vcand.update_last_activity_date(candidate, mylog)