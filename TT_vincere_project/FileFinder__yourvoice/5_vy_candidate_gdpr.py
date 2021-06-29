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
cf.read('yv_config.ini')
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
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()


from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
gdpr = pd.read_sql("""
select idPerson
, ps.Value as status
, ProcessingReasonValue
, Email
, Result
, ErrorCode
, ErrorDescription, ModifiedOn, pct.Value as type, CreatedOn
from ComplianceLog cl
-- left join ProcessingReason pr on cl.idProcessingReason = pr.idProcessingReason
left join ProcessingStatus ps on cl.idProcessingStatus = ps.idProcessingStatus
left join PersonCommunicationType pct on cl.idPersonCommunicationType = pct.idPersonCommunicationType
where IsCurrent = 1
""", engine_sqlite)

gdpr['external_id'] = gdpr['idPerson']
# gdpr['exercise_right'] = gdpr['Person informed how to exercise their rights']  # 3: Other [Person informed how to exercise their rights]
gdpr['request_through'] = 1 # 6: Other
gdpr['obtained_through'] = 1 # 6: Other
gdpr['obtained_through_date'] = pd.to_datetime(gdpr['ModifiedOn'])
gdpr['request_through_date'] = pd.to_datetime(gdpr['ModifiedOn'])
# gdpr['expire'] = None
# gdpr.loc[gdpr['Expires'] == 1.0, 'expire'] = 1
# gdpr['expire'] = gdpr['expire'].apply(lambda x: int(x) if x else x)
# gdpr['expire_date'] = pd.to_datetime(gdpr['ToDate'])

gdpr.loc[gdpr['ProcessingReasonValue'] == 'Consent', 'portal_status'] = 1
gdpr.loc[gdpr['ProcessingReasonValue'] == 'Legitimate Interests', 'portal_status'] = 5

gdpr.loc[gdpr['status'] == 'Consent Email - In Progress', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Not Started', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Pending', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Refusal By Expiry', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Refusal', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Consent Email - Not Right Now', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Accepted', 'explicit_consent'] = 1
gdpr.loc[gdpr['status'] == 'Email Not Sent', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Privacy Notice Email - Pending', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Objected', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Consent Email - Queued', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Consent Email - Submitted to GT', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Privacy Notice Email - Queued', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Privacy Notice Email - Submitted to GT', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Understood', 'explicit_consent'] = 1
gdpr.loc[gdpr['status'] == 'Privacy Notice Email - In Progress', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Privacy Notice Email - Not Right Now', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Consent Email - Failed', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Privacy Notice Email - Failed', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Privacy Notice Email - Not Right Now Expired', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Consent Email - Not Right Now Expired', 'explicit_consent'] = 0
gdpr.loc[gdpr['status'] == 'Email Sent', 'explicit_consent'] = 0
# gdpr['portal_status'] = 1  # 1:Consent given / 2:Pending [Consent to keep] / 3:To be forgotten / 4:Contract / 5:Legitimate interest
gdpr['notes'] = gdpr[['type', 'Result', 'ErrorCode', 'ErrorDescription']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Email Type', 'Result', 'GT Error Code', 'GT Error Description'], x) if e[1]]), axis=1)  # [Notes | Journal]*
gdpr['insert_timestamp'] = pd.to_datetime(gdpr['CreatedOn'])
cols = ['candidate_id',
        'request_through_date',  # 3: Other [Person informed how to exercise their rights]
        'request_through',  # 6: Other
        'obtained_through',  # 6: Other
        'obtained_through_date',
        'explicit_consent',  # 0: No
        'portal_status',  # 1:Consent given / 2:Pending [Consent to keep]
        'notes',  # [Notes | Journal]
        'insert_timestamp']
# assert False
vincere_custom_migration.insert_candidate_gdpr_compliance(gdpr, connection, cols)