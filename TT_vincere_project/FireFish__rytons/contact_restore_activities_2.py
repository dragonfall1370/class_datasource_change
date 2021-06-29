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
assert False

# cont = pd.read_sql("""select * from contact""", engine_postgre_review)
# activity_cont = pd.read_sql("""select * from activity_contact""", engine_postgre_bkup)
# activity_cont = activity_cont.merge(cont[['id']], left_on='contact_id', right_on='id')
#
# activity = pd.read_sql("""select * from activity""", engine_postgre_bkup)
# activity = activity.merge(activity_cont[['activity_id']], left_on='id', right_on='activity_id')
#
# activity_prod = pd.read_sql("""select * from activity""", engine_postgre_review)
# activity = activity.loc[~activity['id'].isin(activity_prod['id'])]
#
# activity_cont_prod = pd.read_sql("""select * from activity_contact""", engine_postgre_review)
# activity_cont = activity_cont.loc[~activity_cont['id'].isin(activity_cont_prod['id'])]
#
# activity.rename(columns={'new_id': 'candidate_id'}, inplace=True)
# col_activity = list(activity.columns)
# # col_activity.pop()
# # col_activity.remove('contact_id')
# # col_activity.remove('candidate_id')
# activity_cand.drop(['id','candidate_id','old_id'], axis=1, inplace=True)
# activity_cand.rename(columns={'new_id': 'candidate_id'}, inplace=True)
#
# col_activity_cand = list(activity_cand.columns)
# col_activity_cand.pop()
#
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity, ddbconn,col_activity, 'activity', mylog)
#
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_cand, ddbconn,col_activity_cand, 'activity_candidate', mylog)


# cont = pd.read_sql("""select * from contact where external_id is null""", engine_postgre_review)
# activity_cont = pd.read_sql("""select * from activity_contact""", engine_postgre_bkup)
# activity_cont = activity_cont.merge(cont[['id']], left_on='contact_id', right_on='id')
# activity_cont['matcher'] = activity_cont['activity_id'].astype(str)+'_'+activity_cont['contact_id'].astype(str)
#
# activity_cont_prod = pd.read_sql("""select * from activity_contact""", engine_postgre_review)
# activity_cont_prod['matcher'] = activity_cont_prod['activity_id'].astype(str)+'_'+activity_cont_prod['contact_id'].astype(str)
# activity_cont = activity_cont.loc[~activity_cont['matcher'].isin(activity_cont_prod['matcher'])]
#
# col_activity_cont = list(activity_cont.columns)
# col_activity_cont.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_cont, ddbconn,col_activity_cont, 'activity_contact', mylog)


# comp = pd.read_sql("""select * from company where external_id is null""", engine_postgre_review)
# activity_comp = pd.read_sql("""select * from activity_company""", engine_postgre_bkup)
# activity_comp = activity_comp.merge(comp[['id']], left_on='company_id', right_on='id')
# activity_comp['matcher'] = activity_comp['activity_id'].astype(str)+'_'+activity_comp['company_id'].astype(str)
#
# activity_comp_prod = pd.read_sql("""select * from activity_company""", engine_postgre_review)
# activity_comp_prod['matcher'] = activity_comp_prod['activity_id'].astype(str)+'_'+activity_comp_prod['company_id'].astype(str)
# activity_comp = activity_comp.loc[~activity_comp['matcher'].isin(activity_comp_prod['matcher'])]
#
# col_activity_comp = list(activity_comp.columns)
# col_activity_comp.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_comp, ddbconn,col_activity_comp, 'activity_company', mylog)


cand = pd.read_sql("""select * from candidate where external_id is null""", engine_postgre_review)
activity_cand = pd.read_sql("""select * from activity_candidate""", engine_postgre_bkup)
activity_cand = activity_cand.merge(comp[['id']], left_on='candidate_id', right_on='id')
activity_cand['matcher'] = activity_cand['activity_id'].astype(str)+'_'+activity_cand['candidate_id'].astype(str)

activity_cand_prod = pd.read_sql("""select * from activity_candidate""", engine_postgre_review)
activity_cand_prod['matcher'] = activity_cand_prod['activity_id'].astype(str)+'_'+activity_cand_prod['candidate_id'].astype(str)
activity_cand = activity_cand.loc[~activity_cand['matcher'].isin(activity_cand_prod['matcher'])]

col_activity_cand = list(activity_comp.columns)
col_activity_cand.pop()
vincere_custom_migration.psycopg2_bulk_insert_tracking(activity_cand, ddbconn,col_activity_cand, 'activity_candidate', mylog)