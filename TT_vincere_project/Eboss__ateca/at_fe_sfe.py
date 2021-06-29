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
cand = pd.read_sql("""
select concat('AT',[CD Number]) as candidate_externalid, nullif(trim([Skills and Technologies]),'') as skill
from ateca_cand
where nullif(trim([Skills and Technologies]),'') is not null
""", engine_mssql)

cand_fe = cand['skill'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand['candidate_externalid'], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='fe') \
    .drop('variable', axis='columns') \
    .dropna()
cand_fe = cand_fe.loc[cand_fe['fe']!='']
cand_fe = cand_fe.drop_duplicates()
cand_fe['sfe']=''
cand_fe.loc[cand_fe['candidate_externalid']=='AT729']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
cp1 = vcand.insert_fe_sfe2(cand_fe, mylog)

# %% contact
cont_skill1 = pd.read_sql("""
select concat('AT',contact_id) as contact_externalid, nullif(trim(contact_skill1),'') as contact_skill
from client_contact
where nullif(trim(contact_skill1),'') is not null
""", engine_mssql)

cont_skill2 = pd.read_sql("""
select concat('AT',contact_id) as contact_externalid, nullif(trim(contact_skill2),'') as contact_skill
from client_contact
where nullif(trim(contact_skill2),'') is not null
""", engine_mssql)
cont_skill = pd.concat([cont_skill1,cont_skill2])

cont_fe = cont_skill['contact_skill'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cont_skill['contact_externalid'], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='fe') \
    .drop('variable', axis='columns') \
    .dropna()
cont_fe = cont_fe.loc[cont_fe['fe']!='']
cont_fe = cont_fe.drop_duplicates()
cont_fe['sfe']=''
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
cp2 = vcont.insert_fe_sfe2(cont_fe, mylog)

# %% job
job_skill1 = pd.read_sql("""
select concat('AT',JobID) as job_externalid, nullif(trim([Key Skills]),'') as skill
from jobs
where nullif(trim([Key Skills]),'') is not null
""", engine_mssql)

job_skill2 = pd.read_sql("""
select concat('AT',JobID) as job_externalid, nullif(trim(Language),'') as skill
from jobs
where nullif(trim(Language),'') is not null
""", engine_mssql)
job_skill = pd.concat([job_skill1,job_skill2])

job_fe = job_skill['skill'].map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(job_skill['job_externalid'], left_index=True, right_index=True) \
    .melt(id_vars=['job_externalid'], value_name='fe') \
    .drop('variable', axis='columns') \
    .dropna()
job_fe = job_fe.loc[job_fe['fe']!='']
job_fe = job_fe.drop_duplicates()
job_fe['sfe']=''
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
cp3 = vjob.insert_fe_sfe2(job_fe, mylog)