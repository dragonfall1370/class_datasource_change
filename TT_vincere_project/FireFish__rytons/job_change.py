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


job = pd.read_csv(os.path.join(standard_file_upload, '5_job_more.csv'))
job_prod = pd.read_sql("""select pd.id, pd.name, pd.company_id, c2.id as comp_id, pd.contact_id, c.id as cont_id, pd.external_id
,c2.name as comp_name, c.first_name, c.last_name
from position_description pd
left join contact c on pd.contact_id = c.id
left join company c2 on c.company_id = c2.id""", engine_postgre_review)

comp_prod = pd.read_sql("""select id, name, external_id from company""", engine_postgre_review)

cont_prod = pd.read_sql("""select id, company_id, first_name, last_name, external_id from contact""", engine_postgre_review)

job['position-externalId'] = job['position-externalId'].astype(str)
job['position-companyId'] = job['position-companyId'].astype(str)
job_prod_match = job_prod.merge(job[['position-externalId','position-title','position-companyId','position-contactId']], left_on='external_id', right_on='position-externalId')
job_prod_match = job_prod_match.merge(comp_prod, left_on='position-companyId', right_on='external_id')
job_prod_match = job_prod_match.merge(cont_prod, left_on='position-contactId', right_on='external_id')

job_fix = job_prod_match[['id_x','company_id_x','comp_id','contact_id','cont_id', 'id_y', 'id', 'company_id_y']]\
    .rename(columns={'id_x':'id','company_id_x': 'old_company_id', 'contact_id': 'old_contact_id', 'id_y': 'new_comp_id', 'id': 'new_cont_id', 'company_id_y': 'new_comp_cont_id'})
job_fix['old_company_id_diff'] = job_fix['old_company_id'].sub(job_fix['comp_id'], axis=0)
job_fix['old_contact_id_diff'] = job_fix['old_contact_id'].sub(job_fix['cont_id'], axis=0)
job_fix['new_company_id_diff'] = job_fix['new_comp_id'].sub(job_fix['new_comp_cont_id'], axis=0)

job_to_fixed = job_fix[['id','new_cont_id','new_comp_cont_id']].rename(columns={'new_cont_id': 'contact_id', 'new_comp_cont_id': 'company_id'})
job_to_fixed.to_csv('job_to_fixed.csv', index=False)

vincere_custom_migration.psycopg2_bulk_update_tracking(job_to_fixed, ddbconn, ['contact_id', 'company_id'], ['id'], 'position_description', mylog)


pos_cand = pd.read_sql("""select id, position_description_id from position_candidate""", engine_postgre_review)
pos_cand = pos_cand.merge(job_to_fixed, left_on='position_description_id', right_on='id')


offer_info = pd.read_sql("""select o.id as offer_id, o.position_candidate_id
     , opi.client_company_id
     , opi.client_company_name
     , opi.client_contact_id
     , opi.client_contact_name
     , opi.client_contact_email
from offer o
left join offer_personal_info opi on o.id = opi.offer_id""", engine_postgre_review)

offer_info = offer_info.merge(pos_cand, left_on='position_candidate_id', right_on='id_x')
comp_prod = pd.read_sql("""select id, name from company""", engine_postgre_review)
cont_prod = pd.read_sql("""select id, first_name, last_name, email from contact""", engine_postgre_review)


offer_info = offer_info.merge(comp_prod, left_on='company_id', right_on='id')
offer_info = offer_info.merge(cont_prod, left_on='contact_id', right_on='id')
offer_info['client_contact_email'] = offer_info['email']
offer_info['client_contact_id'] = offer_info['contact_id']
offer_info['client_company_id'] = offer_info['company_id']
offer_info['client_company_name'] = offer_info['name']
offer_info_to_load = offer_info[['offer_id','position_candidate_id','client_company_id','client_company_name','client_contact_id','client_contact_name','client_contact_email']]
offer_info_to_load['id'] = offer_info_to_load['offer_id']
offer_info_to_load.to_csv('offer_info_to_load.csv')
vincere_custom_migration.psycopg2_bulk_update_tracking(offer_info_to_load, ddbconn, ['client_company_id','client_company_name','client_contact_id','client_contact_name','client_contact_email'], ['offer_id'], 'offer_personal_info', mylog)