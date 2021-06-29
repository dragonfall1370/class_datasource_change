# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

jobapp = pd.read_sql("""select job, person
from activities
where job is not null  and person is not null
and type in(
'Client Rejected / Rejected'
,'Client Rejected / Rejected / By Recruiter'
,'Client Rejected'
)""",engine_sqlite)
assert False
# load placement
jobapp['jobextid'] = jobapp['job'].apply(lambda x: str(x).split('.')[0])
jobapp['candidateextid'] = jobapp['person'].apply(lambda x: str(x).split('.')[0])

# %% job application is marked as rejected
vjobapp = pd.read_sql("""
select pc.status,rejected_date, pc.id, pd.external_id as jobextid, c.external_id as candidateextid, associated_date
from position_candidate pc
  join position_description pd on pc.position_description_id = pd.id
  join candidate c on pc.candidate_id = c.id
  """, connection)
# assert False
tem = jobapp.merge(vjobapp, on=['jobextid', 'candidateextid'])
tem['status'].unique()
# tem.loc[tem['status']==]
tem.loc[tem['rejected_date'].isnull()]
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, ddbconn, ['rejected_date'], ['id'], 'position_candidate', mylog)