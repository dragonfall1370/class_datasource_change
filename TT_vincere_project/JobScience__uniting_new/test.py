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
import json

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# cand = pd.read_csv(os.path.join(standard_file_upload, 'skip_file_cand.csv'))
#
# cand_db = pd.read_sql("""
# select external_id,email from candidate
# """, engine_postgre)
# assert False
# c = cand.merge(cand_db, left_on='candidate-externalId', right_on='external_id')
# c_2 = cand.loc[~cand['candidate-externalId'].isin(c['candidate-externalId'])]
# c_3 = c_2.merge(cand_db, left_on='candidate-email', right_on='email')
# c_3['candidate-email'] = c_3['candidate-externalId']+'_'+c_3['candidate-email']
#
# c_3.to_csv(os.path.join(standard_file_upload, 'cand_to_add_new.csv'))
#
#
# cand = pd.read_csv(os.path.join(standard_file_upload, 'skip_file_cand.csv'))
job_app = pd.read_csv(os.path.join(standard_file_upload, 'skip_file_place.csv'))
cand_db = pd.read_sql("""
select external_id as candidate_externalid ,id as cand_id from candidate
""", engine_postgre)

job_db = pd.read_sql("""
select external_id as job_externalid,id as job_id from position_description
""", engine_postgre)

pos_cand = pd.read_sql("""
select status,candidate_id, position_description_id, id from position_candidate
""", engine_postgre)
assert False
job_app = job_app.merge(cand_db, left_on='application-candidateExternalId', right_on='candidate_externalid', how='left')
job_app = job_app.merge(job_db, left_on='application-positionExternalId', right_on='job_externalid', how='left')
job_app = job_app.merge(pos_cand, left_on=['cand_id','job_id'], right_on=['candidate_id','position_description_id'], how='left')
job_app['status'].unique()
job_app.loc[~job_app['id'].notnull()]
job_app.loc[job_app['status']==104.0]