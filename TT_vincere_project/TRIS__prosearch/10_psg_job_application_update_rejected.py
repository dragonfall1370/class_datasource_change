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
cf.read('psg_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
# src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')

# load placement
job_app_contains_rejected = pd.read_csv(os.path.join(standard_file_upload, '7_jobapplication_other.csv'))
job_app_contains_rejected['application-positionExternalId'] = job_app_contains_rejected['application-positionExternalId'].astype(str)
job_app_contains_rejected['application-candidateExternalId'] = job_app_contains_rejected['application-candidateExternalId'].astype(str)
job_app_contains_rejected['application-positionExternalId'] = job_app_contains_rejected['application-positionExternalId'].str.strip()
job_app_contains_rejected['application-candidateExternalId'] = job_app_contains_rejected['application-candidateExternalId'].map(lambda x: str(x).strip())

job_app_contains_rejected = job_app_contains_rejected.loc[job_app_contains_rejected['application-stage-note'] == 'rejected']

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
job_app_contains_rejected['rejected_date'] = pd.to_datetime(job_app_contains_rejected['application-actionedDate'])

vincere_custom_migration.psycopg2_bulk_update_tracking(job_app_contains_rejected, ddbconn, ['rejected_date'], ['id'], 'position_candidate', mylog)


