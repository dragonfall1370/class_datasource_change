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
import datetime
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('tj_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
from common import vincere_job
vjob = vincere_job.Job(ddbconn)
assert False
# close_job = pd.read_csv('C:\\Users\\tony\\Desktop\\youconnect\\tanjong_job_not_malaysia.csv')
# close_job['external_id'] = 'FC'+close_job['Job ID'].astype(str)
#
# j_prod = pd.read_sql("""select id, name, external_id from position_description""", ddbconn)
# j_prod = j_prod.loc[j_prod['external_id'].isin(close_job['external_id'])]
# j_prod['head_count_close_date'] = datetime.datetime.now() - datetime.timedelta(days=3)
# vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod, ddbconn, ['head_count_close_date', ], ['id', ], 'position_description', mylog)
#
# j_prod['active'] = 2
# vincere_custom_migration.psycopg2_bulk_update_tracking(j_prod,ddbconn, ['active'], ['id'], 'position_description',mylog)
job = pd.read_sql(""" 
select EnquiryID, DateTaken, ClosedDate, Closed, nullif(JobKeyWords,'') as JobKeyWords, es.Status from Enquiries e
left join EnquiryStatus es on e.Status = es.StatusID
""", engine_mssql)
job['job_externalid'] = 'FC'+job['EnquiryID'].astype(str)
job['close_date'] = datetime.datetime.now() - datetime.timedelta(days=4)
vjob.update_close_date(job, mylog)
job['name'] = 'Closed'
vjob.add_job_status(job, mylog)