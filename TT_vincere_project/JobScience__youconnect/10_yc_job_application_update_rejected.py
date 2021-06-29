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
cf.read('yc_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()
# %%
sql = """
select
c.CreatedDate
, c.ts2__Job__c as job_externalid
, c.ts2__Candidate_Contact__c as candidate_externalid
, c.ts2extams__Substatus__c as application_stage
, c.ts2__Application_Status__c
from ts2__Application__c c where c.IsDeleted=0
and application_stage is not null
and c.ts2__Stage__c = 'Application'
and c.ts2__Application_Status__c = 'Rejected'
"""
job_app_contains_rejected = pd.read_sql(sql, engine_sqlite)

# load placement
job_app_contains_rejected['application-positionExternalId'] = job_app_contains_rejected['job_externalid'].astype(str)
job_app_contains_rejected['application-candidateExternalId'] = job_app_contains_rejected['candidate_externalid'].astype(str)
job_app_contains_rejected['application-positionExternalId'] = job_app_contains_rejected['application-positionExternalId'].str.strip()
job_app_contains_rejected['application-candidateExternalId'] = job_app_contains_rejected['application-candidateExternalId'].str.strip()
assert False
# %% job application is marked as rejected
vjobapp = pd.read_sql("""
select rejected_date, pc.id, pd.external_id as jobextid, c.external_id as candidateextid
from position_candidate pc
  join position_description pd on pc.position_description_id = pd.id
  join candidate c on pc.candidate_id = c.id
  """, ddbconn)

job_app_contains_rejected['application-positionExternalId'] = job_app_contains_rejected['application-positionExternalId'].astype(str)
job_app_contains_rejected['application-candidateExternalId'] = job_app_contains_rejected['application-candidateExternalId'].astype(str)
job_app_contains_rejected = job_app_contains_rejected.merge(vjobapp, right_on=['jobextid', 'candidateextid'], left_on=['application-positionExternalId', 'application-candidateExternalId'])
job_app_contains_rejected['rejected_date'] = pd.to_datetime(job_app_contains_rejected['CreatedDate'])

vincere_custom_migration.psycopg2_bulk_update_tracking(job_app_contains_rejected, ddbconn, ['rejected_date'], ['id'], 'position_candidate', mylog)


