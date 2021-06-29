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
cf.read('rt_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% dest db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()

# %% backup db
conn_str_ddb_bkup = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format('postgres', '123456', 'dmpfra.vinceredev.com', '5432', 'rytons_backup_restored')
engine_postgre_bkup = sqlalchemy.create_engine(conn_str_ddb_bkup, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn_bkup = engine_postgre_bkup.raw_connection()

cand = pd.read_csv(r'C:\Users\tony\Desktop\rytons\files\candidate.csv')
cand.drop(['Last activity date', 'Activity content'], axis=1, inplace=True)
cand = cand.drop_duplicates()
# assert False
cand_bkup = pd.read_sql("""select * from candidate """, engine_postgre_bkup)
cand_bkup = cand_bkup.merge(cand[['ID','Email']], left_on='id',right_on='ID')
cand_bkup.drop(['ID','Email'], axis=1, inplace=True)

cand_wex = cand_bkup.loc[cand_bkup['external_id'].notnull()]


cand_prod = pd.read_sql("""select * from candidate""", engine_postgre_review)
cand_not_dup = cand_wex.merge(cand_prod, on='id')
cand_dup = cand_wex.loc[~cand_wex['id'].isin(cand_not_dup['id'])]
cand_map = cand_prod.merge(cand_dup[['id','external_id']], on='external_id')
cand_map_id = cand_map[['id_x','id_y']].rename(columns={'id_x': 'new_id','id_y':'old_id'})
# cand_dup.to_csv('cand_dup.csv')


activity_cand = pd.read_sql("""select * from activity_candidate""", engine_postgre_bkup)
activity_cand = activity_cand.merge(cand_dup[['id']], left_on='candidate_id', right_on='id')
activity_cand = activity_cand.merge(cand_map_id, left_on='candidate_id', right_on='old_id', how='left')

activity = pd.read_sql("""select * from activity""", engine_postgre_bkup)
activity = activity.merge(activity_cand[['activity_id','candidate_id','new_id','old_id']], left_on='id', right_on='activity_id')

activity_prod = pd.read_sql("""select * from activity""", engine_postgre_review)
activity = activity.loc[~activity['id'].isin(activity_prod['id'])]
a = activity[['candidate_id_x','candidate_id_y','new_id','old_id']]
# activity = activity.merge(activity_cand, left_on='id', right_on='activity_id')
activity.drop(['old_id','candidate_id_y','candidate_id_x','activity_id','external_map'], axis=1, inplace=True)

activity.rename(columns={'new_id': 'candidate_id'}, inplace=True)
col_activity = list(activity.columns)
# col_activity.pop()
# col_activity.remove('contact_id')
# col_activity.remove('candidate_id')
activity_cand.drop(['id','candidate_id','old_id'], axis=1, inplace=True)
activity_cand.rename(columns={'new_id': 'candidate_id'}, inplace=True)

col_activity_comp = list(activity_cand.columns)
col_activity_comp.pop()

vincere_custom_migration.psycopg2_bulk_insert_tracking(activity, ddbconn,col_activity, 'activity', mylog)

vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_cand, ddbconn,col_activity_comp, 'activity_candidate', mylog)
