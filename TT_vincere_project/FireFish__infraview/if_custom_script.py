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
cf.read('if_config.ini')
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

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()
assert False
contact_imp = pd.read_csv(r'D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\skip_file (4).csv')
# job_imp = pd.read_csv('D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\Infraview- Job - Job Import Template.csv')
# jobapp_imp = pd.read_csv('D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\Infraview - Job Application - Job Application Data Import Template.csv')

cont_prod = pd.read_sql("""
select c.external_id as contact_externalid, c2.external_id as company_externalid, email from contact c
join company c2 on c.company_id = c2.id
where c.external_id is not null
and c. deleted_timestamp is null
""", engine_postgre)

tem = contact_imp.merge(cont_prod, left_on=['contact-externalId','contact-companyId'], right_on=['contact_externalid','company_externalid'])
contact_imp = contact_imp.loc[~contact_imp['contact-externalId'].isin(tem['contact-externalId'])]
contact_imp = contact_imp.loc[contact_imp['contact-companyId'].notnull()]
contact_imp = contact_imp.drop(['Errors'], axis=1)
contact_imp['contact-email'] = '_duplicate_'+contact_imp['contact-email']
contact_imp.to_csv('contact_import_2.csv')

# %%
job_imp = pd.read_csv(r'D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\skip_file (8).csv')
# job_imp = pd.read_csv('D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\Infraview- Job - Job Import Template.csv')
# jobapp_imp = pd.read_csv('D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\Infraview - Job Application - Job Application Data Import Template.csv')

job_prod = pd.read_sql("""
select pd.external_id as job_externalid, c.external_id as contact_externalid, pd.name from position_description pd
join contact c on pd.contact_id = c.id
where pd.external_id is not null
and pd.deleted_timestamp is null
""", engine_postgre)

tem = job_imp[['position-contactId','position-title','position-externalId']].merge(job_prod, left_on=['position-contactId','position-externalId'], right_on=['contact_externalid','job_externalid'])
job_imp = job_imp.loc[~job_imp['position-externalId'].isin(tem['position-externalId'])]
job_imp = job_imp.loc[job_imp['position-contactId'].notnull()]
job_imp['position-startDate'] = pd.to_datetime(job_imp['position-startDate'], format='%d/%m/%Y')
job_imp = job_imp.drop(['Errors'], axis=1)
job_imp['position-title'] = job_imp['position-title']+'__2'
job_imp.to_csv('job_import_2.csv',index=False)

# %%
jobapp_imp = pd.read_csv(r'D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\skip_file (10).csv')
# job_imp = pd.read_csv('D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\Infraview- Job - Job Import Template.csv')
# jobapp_imp = pd.read_csv('D:\Tony\project\infraview\Upsell Infraview\Upsell Infraview\Infraview - Job Application - Job Application Data Import Template.csv')

jobapp_prod = pd.read_sql("""
select c.external_id as candidate_externalid, pd.external_id as job_externalid, pc.status from position_candidate pc
left join candidate c on pc.candidate_id = c.id
left join position_description pd on pc.position_description_id = pd.id
where c.external_id is not null and pd.external_id is not null
""", engine_postgre)

tem = jobapp_imp.merge(jobapp_prod, left_on=['application-positionExternalId','application-candidateExternalId'], right_on=['job_externalid','candidate_externalid'])
job_imp = job_imp.loc[~job_imp['position-externalId'].isin(tem['position-externalId'])]
job_imp = job_imp.loc[job_imp['position-contactId'].notnull()]
job_imp['position-startDate'] = pd.to_datetime(job_imp['position-startDate'], format='%d/%m/%Y')
job_imp = job_imp.drop(['Errors'], axis=1)
job_imp['position-title'] = job_imp['position-title']+'__2'
job_imp.to_csv('job_import_2.csv',index=False)