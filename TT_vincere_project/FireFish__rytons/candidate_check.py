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
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% dest db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# %% dest db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn = engine_postgre_review.raw_connection()

# %% backup db
conn_str_ddb_bkup = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format('postgres', '123456', 'dmpfra.vinceredev.com', '5432', 'rytons_backup_restored')
engine_postgre_bkup = sqlalchemy.create_engine(conn_str_ddb_bkup, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
ddbconn_bkup = engine_postgre_bkup.raw_connection()
assert False
# cand_bkup = pd.read_sql("""select * from candidate """, engine_postgre_bkup)
# cand_prod = pd.read_sql("""select * from candidate""", engine_postgre_review)
#
# cand_dup = cand_bkup.merge(cand_prod, on=['first_name','last_name','email','external_id'])
# cand_bkup2 = cand_bkup.loc[~cand_bkup['id'].isin(cand_dup['id_x'])]
#
# cand_dup2 = cand_bkup2.merge(cand_prod, on=['first_name','last_name','email'])
# cand_bkup3 = cand_bkup2.loc[~cand_bkup2['id'].isin(cand_dup2['id_x'])]
# cand_bkup3.to_csv('cand_bkup_to_restored.csv',index=False)
#
# col_cand= list(cand_bkup3.columns)
# col_cand.pop()
# col_cand.remove('current_location_id')
# col_cand.remove('highest_pcid')
#
# vincere_custom_migration.psycopg2_bulk_insert_tracking(cand_bkup3, ddbconn, col_cand, 'candidate', mylog)




# cand = pd.read_csv('cand_bkup_to_restored.csv')
# activity_cand = pd.read_sql("""select * from activity_candidate""", engine_postgre_bkup)
# activity_cand = activity_cand.merge(cand[['id']], left_on='candidate_id', right_on='id')
#
# activity = pd.read_sql("""select * from activity""", engine_postgre_bkup)
# activity = activity.merge(activity_cand[['activity_id']], left_on='id', right_on='activity_id')
#
# activity_prod = pd.read_sql("""select * from activity""", engine_postgre_review)
# activity = activity.loc[~activity['id'].isin(activity_prod['id'])]
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


cand = pd.read_csv('cand_bkup_to_restored.csv')
pos_cand = pd.read_sql("""select * from position_candidate""", engine_postgre_bkup)
pos_cand = pos_cand.merge(cand[['id']], left_on='candidate_id', right_on='id')
pos_cand.to_csv('pos_cand_10102020.csv')

job = pd.read_sql("""select * from position_description""", engine_postgre_bkup)
job = job.merge(pos_cand[['position_description_id']], left_on='id', right_on='position_description_id')
job = job.drop_duplicates()
job['external_id'].value_counts()

job_prod = pd.read_sql("""select * from position_description""", engine_postgre_review)
job_add = job.loc[~job['id'].isin(job_prod['id'])]
job_add.to_csv('job_add_10102020.csv')


cont = pd.read_sql("""select * from contact where deleted_timestamp is null""", engine_postgre_bkup)
cont = cont.merge(job_add[['contact_id']], left_on='id', right_on='contact_id')
tem1 = pd.DataFrame(cont['external_id'].value_counts().keys(), columns=['external_id'])
cont = cont.merge(tem1, on='external_id')
cont = cont.drop_duplicates()
cont['external_id']

cont_prod = pd.read_sql("""select * from contact""", engine_postgre_review)
cont_prod = cont_prod.loc[cont_prod['external_id'].isin(cont['external_id'])]
cont_match = cont_prod.merge(cont, on='external_id')
cont_new_id = cont_prod[['id','company_id']]
cont_map_id = cont_match[['id_x','company_id_x','id_y','company_id_y']]\
    .rename(columns={'id_x': 'new_cont_id', 'company_id_x': 'new_comp_id', 'id_y': 'old_cont_id', 'company_id_y': 'old_comp_id'})

job_add = job_add.merge(cont_map_id, left_on=['contact_id', 'company_id'], right_on=['old_cont_id', 'old_comp_id'])
job_add.drop(['old_cont_id','old_comp_id','position_description_id','company_location_id','company_id','contact_id'], axis=1, inplace=True)
job_add.rename(columns={'new_cont_id': 'contact_id', 'new_comp_id': 'company_id'}, inplace=True)

col_job= list(job_add.columns)
# col_cand.pop()
# col_cand.remove('current_location_id')
# col_cand.remove('highest_pcid')
job_add['id']
vincere_custom_migration.psycopg2_bulk_insert_tracking(job_add, ddbconn, col_job, 'position_description', mylog)

# compensation_bkup = pd.read_sql("""select * from compensation where position_id in (32180
# ,32098
# ,32110
# ,32099
# ,32152
# ,32153
# ,32154
# ,32155
# ,32156
# ,32157
# ,32111)""", engine_postgre_bkup)
#
# col_compensation = list(compensation_bkup.columns)
# # col_compensation.remove('company_location_id')
# # col_job.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(compensation_bkup, ddbconn,col_compensation, 'compensation', mylog)
#
#
#
#
# position_agency_consultant_bkup = pd.read_sql("""select * from position_agency_consultant where position_id in (32180
# ,32098
# ,32110
# ,32099
# ,32152
# ,32153
# ,32154
# ,32155
# ,32156
# ,32157
# ,32111)""", engine_postgre_bkup)
#
# col_position_agency_consultant = list(position_agency_consultant_bkup.columns)
# col_position_agency_consultant.remove('company_location_id')
# # col_job.pop()
# vincere_custom_migration.psycopg2_bulk_insert_tracking(position_agency_consultant_bkup, ddbconn,col_position_agency_consultant, 'position_agency_consultant', mylog)


pos_cand.drop('id_y', axis=1, inplace=True)
pos_cand.rename(columns={'id_x':'id'}, inplace=True)
col_pos_cand = list(pos_cand.columns)
col_pos_cand.remove('company_location_id')
# col_job.pop()
vincere_custom_migration.psycopg2_bulk_insert_tracking(pos_cand, ddbconn, col_pos_cand, 'position_candidate', mylog)