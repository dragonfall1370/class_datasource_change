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
cf.read('ca_config.ini')
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
assert False
cont_place = pd.read_sql("""select contact_id from position_description where id in (
select  position_description_id from position_candidate where status > 300)
 and position_category = 1""", connection)
cont_place['board'] = 4
cont_place['status'] = 1
cont_place['id'] = cont_place['contact_id']
cont_place.loc[cont_place['id']==61431]
vincere_custom_migration.psycopg2_bulk_update_tracking(cont_place, connection, ['board', 'status'], ['id', ],'contact', mylog)

cont_no_place = pd.read_sql("""select contact_id from position_description where id in (
select  position_description_id from position_candidate where status < 300)
 and position_category = 1""", connection)
cont_no_place['board'] = 3
cont_no_place['status'] = 1
cont_no_place['id'] = cont_no_place['contact_id']
cont_no_place.loc[cont_no_place['id']==61431]
vincere_custom_migration.psycopg2_bulk_update_tracking(cont_no_place, connection, ['board', 'status'], ['id', ],'contact', mylog)

cont_no_place2 = pd.read_sql("""select contact_id, position_category from position_description
where id not in (select  position_description_id from position_candidate)
 and position_category = 1""", connection)
cont_no_place2['board'] = 3
cont_no_place2['status'] = 1
cont_no_place2['id'] = cont_no_place2['contact_id']
cont_no_place2.loc[cont_no_place2['id']==61431]
vincere_custom_migration.psycopg2_bulk_update_tracking(cont_no_place2, connection, ['board', 'status'], ['id', ],'contact', mylog)

cont_no_job = pd.read_sql("""select id from contact where id not in (select distinct contact_id from position_description where position_category = 1)""", connection)
cont_no_job['board'] = 2
cont_no_job['status'] = 1
cont_no_job.loc[cont_no_job['id']==61431]
vincere_custom_migration.psycopg2_bulk_update_tracking(cont_no_job, connection, ['board', 'status'], ['id', ],'contact', mylog)
