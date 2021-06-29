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
cf.read('ca_config.ini')
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
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_placement_detail
vplace = vincere_placement_detail.PlacementDetail(connection)


# %% permanent
placed_info = pd.read_sql("""
select Candidate_Id, Role_Id, Start_DT from Vacancy_Role_Candidate where Start_DT is not null 
""", engine_mssql)
placed_info['job_externalid'] = 'VC'+placed_info['Role_Id'].astype(str)
placed_info['candidate_externalid'] = placed_info['Candidate_Id'].astype(str)
placed_info['start_date'] = placed_info['Start_DT'].apply(lambda x: x[0:10] if x else x)
placed_info['start_date'] = pd.to_datetime(placed_info['start_date'])
assert False
# %% start date/end date
stdate = placed_info[['job_externalid', 'candidate_externalid', 'start_date']].dropna()
stdate['start_date'] = pd.to_datetime(stdate['start_date'])
cp1 = vplace.update_startdate_only_for_placement_detail(stdate, mylog)

# %% offer date/place date
jobapp_created_date = placed_info[['job_externalid', 'candidate_externalid', 'start_date']].dropna()
jobapp_created_date['offer_date'] = pd.to_datetime(jobapp_created_date['start_date'])
jobapp_created_date['placed_date'] = pd.to_datetime(jobapp_created_date['start_date'])
jobapp_created_date['sent_date'] = pd.to_datetime(jobapp_created_date['start_date'])
vplace.update_offerdate(jobapp_created_date, mylog)
vplace.update_placeddate(jobapp_created_date, mylog)
vplace.update_sent_date(jobapp_created_date, mylog)




