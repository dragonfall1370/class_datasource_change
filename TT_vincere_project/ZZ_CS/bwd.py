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
cf.read('dn_config.ini')
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
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %%
# company_industry = pd.read_sql("""select *
# from company_industry where industry_id in (
# select id from vertical where name in ('P&B - DB'
# ,'P&B - DC'
# ,'P&B - EB Support'))""",engine_postgre_review)
# tem = company_industry[['company_id']].drop_duplicates()
# tem['industry_id'] = 29036
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['company_id','industry_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'company_industry', mylog)
#
# contact_industry = pd.read_sql("""select *
# from contact_industry where industry_id in (
# select id from vertical where name in ('P&B - DB'
# ,'P&B - DC'
# ,'P&B - EB Support'))""",engine_postgre_review)
# tem = contact_industry[['contact_id']].drop_duplicates()
# tem['industry_id'] = 29036
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['contact_id','industry_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'contact_industry', mylog)
#
# candidate_industry = pd.read_sql("""select *
# from candidate_industry where vertical_id in (
# select id from vertical where name in ('P&B - DB'
# ,'P&B - DC'
# ,'P&B - EB Support'))""",engine_postgre_review)
# tem = candidate_industry[['candidate_id']].drop_duplicates()
# tem['vertical_id'] = 29036
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['candidate_id','vertical_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'candidate_industry', mylog)
#
#
# job_industry = pd.read_sql("""select id as position_id, vertical_id as industry_id from position_description where vertical_id in (
# select id from vertical where name in ('P&B - DB'
# ,'P&B - DC'
# ,'P&B - EB Support'))""",engine_postgre_review)
# job_industry['parent_id'] = 29036
# job_industry['insert_timestamp'] = datetime.datetime.now()
# cols = ['position_id','industry_id','insert_timestamp','parent_id']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(job_industry, connection, cols, 'position_description_industry', mylog)
#
# tem = job_industry[['position_id']].drop_duplicates()
# tem['industry_id'] = 29036
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['position_id','industry_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'position_description_industry', mylog)
#
#
# tem2 = job_industry[['position_id']].drop_duplicates()
# tem2['id'] = tem2['position_id']
# tem2['vertical_id'] = 29036
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['vertical_id'], ['id'], 'position_description', mylog)
#
# # %%
# company_industry = pd.read_sql("""select *
# from company_industry where industry_id in (
# select id from vertical where name in ('P&B - Inhouse'
# ,'P&B - Third Party'))""",engine_postgre_review)
# tem = company_industry[['company_id']].drop_duplicates()
# tem['industry_id'] = 29040
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['company_id','industry_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'company_industry', mylog)
#
# contact_industry = pd.read_sql("""select *
# from contact_industry where industry_id in (
# select id from vertical where name in ('P&B - Inhouse'
# ,'P&B - Third Party'))""",engine_postgre_review)
# tem = contact_industry[['contact_id']].drop_duplicates()
# tem['industry_id'] = 29040
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['contact_id','industry_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'contact_industry', mylog)
#
# candidate_industry = pd.read_sql("""select *
# from candidate_industry where vertical_id in (
# select id from vertical where name in ('P&B - Inhouse'
# ,'P&B - Third Party'))""",engine_postgre_review)
# tem = candidate_industry[['candidate_id']].drop_duplicates()
# tem['vertical_id'] = 29040
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['candidate_id','vertical_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'candidate_industry', mylog)
#
#
# job_industry = pd.read_sql("""select id as position_id, vertical_id as industry_id from position_description where vertical_id in (
# select id from vertical where name in ('P&B - Inhouse'
# ,'P&B - Third Party'))""",engine_postgre_review)
# job_industry['parent_id'] = 29040
# job_industry['insert_timestamp'] = datetime.datetime.now()
# cols = ['position_id','industry_id','insert_timestamp','parent_id']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(job_industry, connection, cols, 'position_description_industry', mylog)
#
# tem = job_industry[['position_id']].drop_duplicates()
# tem['industry_id'] = 29040
# tem['insert_timestamp'] = datetime.datetime.now()
# cols = ['position_id','industry_id','insert_timestamp']
# vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, connection, cols, 'position_description_industry', mylog)
#
#
# tem2 = job_industry[['position_id']].drop_duplicates()
# tem2['id'] = tem2['position_id']
# tem2['vertical_id'] = 29040
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['vertical_id'], ['id'], 'position_description', mylog)


# %%
# cand = pd.read_csv('Export2.csv')
# cand_id = cand[['No.','First Name','Last name','Email']]
# cand_id['id'] = cand_id['No.']
# cand_id['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(cand_id, connection, ['deleted_timestamp'], ['id'], 'candidate', mylog)


# %%
# def split_data_current_employer(value):
#     arr = []
#     # currentEmployer=''
#     for item in value:
#         if 'currentEmployer' in item:
#             currentEmployer = item['currentEmployer']
#             arr.append(str(currentEmployer))
#         else:
#             pass
#     arr = list(dict.fromkeys(arr))
#     return ','.join(arr)
#
# def split_data_job_title(value):
#     arr = []
#     # jobTitle=''
#     for item in value:
#         if 'jobTitle' in item:
#             jobTitle = item['jobTitle']
#             arr.append(str(jobTitle))
#         else:
#             pass
#     arr = list(dict.fromkeys(arr))
#     return ','.join(arr)

# cand_exp = pd.read_sql("""select id, first_name, last_name,email, experience_details_json, phone as primary_phone, phone2 as mobile_phone, last_activity_date, status
# from solr_candidate_view
# where deleted_timestamp is null
# """,engine_postgre_review)
# bwd_cand = pd.read_csv('BWD - Export and Delete 1 Search - To be deleted.csv')
# tem = cand_exp.loc[cand_exp['id'].isin(bwd_cand['id'])]
# tem['last_activity_date'].unique()
# tem2 = tem[['id','last_activity_date']].dropna()
# cand_exp['experience_details_json'] = cand_exp['experience_details_json'].apply(lambda x: x.replace('null','""') if x else x)
# cand_exp['experience_details_json'] = cand_exp['experience_details_json'].apply(lambda x: x.replace('""""','""') if x else x)
# cand_exp['experience_details_json'] = cand_exp['experience_details_json'].apply(lambda x: eval(x) if x else x)
# cand_exp['current_employer'] = cand_exp['experience_details_json'].apply(lambda x: split_data_current_employer(x) if x else x)
# cand_exp['job_title'] = cand_exp['experience_details_json'].apply(lambda x: split_data_job_title(x) if x else x)
# cand_exp.loc[cand_exp['status']==102.0, 'status'] = 'SHORTLISTED'
# cand_exp.loc[cand_exp['status']==103.0, 'status'] = 'SENT'
# cand_exp.loc[cand_exp['status']==104.0, 'status'] = '1ST INTERVIEW'
# cand_exp.loc[cand_exp['status']==105.0, 'status'] = '2ND INTERVIEW'
# cand_exp.loc[cand_exp['status']==106.0, 'status'] = '3RD INTERVIEW'
# cand_exp.loc[cand_exp['status']==200.0, 'status'] = 'OFFER'
# cand_exp.loc[cand_exp['status']==301.0, 'status'] = 'PLACED'
# cand_exp.loc[cand_exp['status']==302.0, 'status'] = 'PLACED'
# cand_exp.loc[cand_exp['status']==303.0, 'status'] = 'PLACED'
# cand_exp['last_activity_date'] = cand_exp['last_activity_date'].apply(lambda x: str(x) if x else x)
# cand_exp = cand_exp.drop(['experience_details_json'], axis=1)
# cand_exp['last_activity_date'] = cand_exp['last_activity_date'].apply(lambda x: x.replace('NaT','') if x else x)
# cand_exp = cand_exp.to_csv('bwd_candidate.csv',index=False)
# cand_exp['status'].unique()

# %% delete
deleted_cand = pd.read_csv('BWD - Export and Delete 1 Search - To be deleted FINAL.csv')
deleted_cand['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(deleted_cand, connection, ['deleted_timestamp'], ['id'], 'candidate', mylog)