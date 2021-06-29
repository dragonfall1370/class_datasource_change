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
cf.read('at_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %% candidate
company = pd.read_sql("""
select concat('AT',company_id) as company_externalid,  nullif(trim(client_industry),'') as client_industry
from client_contact
where nullif(trim(client_industry),'') is not null
""", engine_mssql)

company_ind = company['client_industry'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(company['company_externalid'], left_index=True, right_index=True) \
    .melt(id_vars=['company_externalid'], value_name='name') \
    .drop('variable', axis='columns') \
    .dropna()
company_ind = company_ind.loc[company_ind['name']!='']
company_ind = company_ind.drop_duplicates()
# assert False
from common import vincere_company
vcom = vincere_company.Company(connection)
cp1 = vcom.insert_company_industry(company_ind, mylog)

# %% contact
contact = pd.read_sql("""
select concat('AT',contact_id) as contact_externalid, nullif(trim(client_industry),'') as client_industry
from client_contact
where nullif(trim(client_industry),'') is not null
""", engine_mssql)

contact_ind = contact['client_industry'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact['contact_externalid'], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='name') \
    .drop('variable', axis='columns') \
    .dropna()
contact_ind = contact_ind.loc[contact_ind['name']!='']
contact_ind = contact_ind.drop_duplicates()
# assert False
from common import vincere_contact
vcon = vincere_contact.Contact(connection)
cp2 = vcon.insert_contact_industry_subindustry(contact_ind, mylog)

# %% job
job = pd.read_sql("""
select concat('AT',JobID) as job_externalid,  nullif(trim(Industry),'') as Industry
from jobs
where nullif(trim(Industry),'') is not null
""", engine_mssql)

job_ind = job['Industry'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(job['job_externalid'], left_index=True, right_index=True) \
    .melt(id_vars=['job_externalid'], value_name='name') \
    .drop('variable', axis='columns') \
    .dropna()
job_ind = job_ind.loc[job_ind['name']!='']
job_ind = job_ind.drop_duplicates()
from common import vincere_job
vjob = vincere_job.Job(connection)
cp3 = vjob.insert_job_industry_subindustry(job_ind, mylog, True)

# %% candidate
candidate = pd.read_sql("""
select concat('AT',[CD Number]) as candidate_externalid,  nullif(trim(Industry),'') as Industry
from ateca_cand
where nullif(trim(Industry),'') is not null
""", engine_mssql)

candidate_ind = candidate['Industry'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate['candidate_externalid'], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='name') \
    .drop('variable', axis='columns') \
    .dropna()
candidate_ind = candidate_ind.loc[candidate_ind['name']!='']
candidate_ind = candidate_ind.drop_duplicates()
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
cp4 = vcand.insert_candidate_industry_subindustry(candidate_ind, mylog)
