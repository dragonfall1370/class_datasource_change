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

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %% candidate
# job_del = pd.read_csv('C:\\Users\\tony\\Desktop\\New folder\\job_to_rmv.csv')



# candidate = pd.read_sql("""
# select c.Id as candidate_externalid
# from Contacts c
# --and c.CreatedDate >= '2018-01-01'
# """, engine_sqlite)
#
# japp = pd.read_sql("""
# select distinct c.ts2__Candidate_Contact__c as candidate_externalid
# from ts2__Application__c c where c.IsDeleted=0 and c.LastModifiedDate >= '2017-10-31'
# and c.ts2__Stage__c in ('Submittal', 'Offer', 'Placement', 'Application',
#                          'Second Client Interview', 'UA Interview',
#                         'Final Client Interview', 'Client Interview')
# """, engine_sqlite)
#
# match = candidate.merge(japp, on='candidate_externalid')

# jobapp = pd.read_sql("""
# select pc.id, pc.insert_timestamp, pd.external_id as jobid, c.external_id as candid
# from position_candidate pc
# left join position_description pd on pd.id = pc.position_description_id
# left join candidate c on pc.candidate_id = c.id
# where pd.external_id is not null and c.external_id is not null
# """, engine_postgre)
# assert False
# jobapp['matcher'] = jobapp['jobid'].apply(lambda x: x[:-3])
# jobapp = jobapp.merge(job_del, left_on='matcher', right_on='Vancacy Id')
# tem1 = pd.DataFrame(jobapp['candid'].value_counts().keys(), columns=['candidate_id'])
#
# cand = pd.read_sql("""
# select id, external_id, insert_timestamp, deleted_timestamp from candidate where external_id is not null and insert_timestamp >= '2019-11-20' and deleted_timestamp is null
# """, engine_postgre)
# # cand = cand.loc[~cand['external_id'].isin(match['candidate_externalid'])]
# cand = cand.merge(tem1, left_on='external_id', right_on='candidate_id')
# cand['deleted_timestamp'] = datetime.datetime.now()
# # # vincere_custom_migration.load_data_to_vincere(cand, dest_db, 'update', 'candidate', ['deleted_timestamp'], ['id'], mylog)
# vincere_custom_migration.psycopg2_bulk_update_tracking(cand, connection, ['deleted_timestamp', ], ['id', ], 'candidate', mylog)



cand_src = pd.read_sql("""
select * from candidate_source
""", engine_postgre)

cand_src['hidden_timestamp'] = datetime.datetime.now()
vincere_custom_migration.load_data_to_vincere(cand_src, dest_db, 'update', 'candidate_source', ['hidden_timestamp'], ['id'], mylog)