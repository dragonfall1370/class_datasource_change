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
cf.read('ec_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
# data_folder = '/Users/truongtung/Desktop'
sqlite_path = cf['default'].get('sqlite_path')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %%
# sql = """
# select id ,name from company
# """
# company = pd.read_sql(sql, engine_postgre_review)
# company['name'] = company['name'].apply(lambda x: x.split('_')[0])
# vincere_custom_migration.load_data_to_vincere(company, dest_db, 'update', 'company', ['name'], ['id'], mylog)
#
# sql = """
# select id ,name from position_description
# """
# job = pd.read_sql(sql, engine_postgre_review)
# job['name'] = job['name'].apply(lambda x: x.split('_')[0])
# vincere_custom_migration.load_data_to_vincere(job, dest_db, 'update', 'position_description', ['name'], ['id'], mylog)
#%%
# sql = """
# select id, external_id from candidate
# """
# cand = pd.read_sql(sql, engine_postgre_review)
#
# sql = """
# select id as contact_id, external_id from contact
# """
# cont = pd.read_sql(sql, engine_postgre_review)
#
# cand = cand.merge(cont, on='external_id')
# vincere_custom_migration.psycopg2_bulk_update_tracking(cand, connection, ['contact_id', ], ['id', ], 'candidate', mylog)
# %%
# sql = """
# select id, phone, mobile_phone from contact where phone = '' and mobile_phone is not null
# """
# cont = pd.read_sql(sql, engine_postgre_review)
# cont['phone'] = cont['mobile_phone']
# vincere_custom_migration.psycopg2_bulk_update_tracking(cont, connection, ['phone', ], ['id', ], 'contact', mylog)
#%%
# candidate = pd.read_csv(os.path.join(standard_file_upload, '6_candidate.csv'))
#
# sql = """
# select id, external_id from candidate
# """
# cand = pd.read_sql(sql, engine_postgre_review)
# cand = cand.loc[cand['external_id'].isin(candidate['candidate-externalId'])]
# cand['experience_details_json'] = None
# vincere_custom_migration.psycopg2_bulk_update_tracking(cand, connection, ['experience_details_json', ], ['id', ], 'candidate', mylog)
#%%
# sql = """
# select id, email, personal_email from contact where email like '%email.com%' and nullif(personal_email, '') notnull
# """
# cont = pd.read_sql(sql, connection)
# cont['email'] = cont['personal_email']
# vincere_custom_migration.psycopg2_bulk_update_tracking(cont, connection, ['email', ], ['id', ], 'contact', mylog)
#
#
# sql = """
# select id , email, work_email from candidate where email like '%email.com%' and nullif(work_email, '') notnull
# """
# cand = pd.read_sql(sql, connection)
# cand['email'] = cand['work_email']
# vincere_custom_migration.psycopg2_bulk_update_tracking(cand, connection, ['email', ], ['id', ], 'candidate', mylog)
#%%
# cand = pd.read_sql("""
# select idPerson as candidate_externalid, pr.Value
# from PersonX p
# left join PersonRating pr on pr.idPersonRating = p.idPersonRating_String
# where idPersonRating_String is not null
# and Value in ('5 Director','6 President/VP')
# """, engine_sqlite)
#
# vc_cand = pd.read_sql("""
# select id ,first_name, last_name, external_id as candidate_externalid from candidate
# """, engine_postgre_review)
# vc_cand = vc_cand.merge(cand,on='candidate_externalid')
# vc_cand.to_csv('LS_candidate_level.csv',index=False)

#%%
# cand1 = pd.read_sql("""
# select idPerson as candidate_externalid, pr.Value
# from PersonX p
# left join PersonRating pr on pr.idPersonRating = p.idPersonRating_String
# where idPersonRating_String is not null
# and Value in ('5 Director','6 President/VP')
# """, engine_sqlite)
# cand2 = pd.read_sql("""
# select  idPerson as candidate_externalid, Value
# from PersonCode pc
# left join udHots h on pc.CodeId = h.idudHots
# where idtablemd = '853718bc-374f-46c2-8b93-709026fcfa8b'
# and Value in ('Hot Potenital Client ','Dani Potential Client')
# """, engine_sqlite)
#
# cand = cand1.merge(cand2, on='candidate_externalid')
#
# vc_cand = pd.read_sql("""
# select id ,first_name, last_name, external_id as candidate_externalid from candidate
# """, engine_postgre_review)
# vc_cand = vc_cand.merge(cand,on='candidate_externalid')
# vc_cand.to_csv('LS_candidate_level_hots.csv',index=False)

#%%
sql = """
select
c.id
, c.name
, cl.address
from company c
left join company_location cl on c.id = cl.company_id
where c.external_id is not null
order by c.id;
"""
company = pd.read_sql(sql, engine_postgre_review)
company['name'] = company[['name', 'address']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
vincere_custom_migration.load_data_to_vincere(company, dest_db, 'update', 'company', ['name'], ['id'], mylog)

#%%
sql = """
select id, first_name, last_name, external_id as candidate_externalid from candidate
"""
cand_vc = pd.read_sql(sql, engine_postgre_review)
sql = """
select idassignment as job_externalid
     , idperson as candidate_externalid
     , createdon
     , cp.value as application_stage
from assignmentcandidate ac
left join candidateprogress cp on ac.idcandidateprogress = cp.idcandidateprogress
where cp.value is not null
and idAssignment = '10f42859-7b81-4d0e-a7fb-fe0111815a4b'
"""
cand = pd.read_sql(sql, engine_sqlite)

cand_vc = cand_vc.merge(cand, on='candidate_externalid')
cand_vc.to_csv('LS_candidate_job.csv',index=False)