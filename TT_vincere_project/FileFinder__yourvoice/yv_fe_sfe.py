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
cf.read('yv_config.ini')
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
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %% candidate
cand = pd.read_sql("""
select px.*
from candidate c
join (select P.idperson as candidate_externalid, p.idJobFunction_String_List
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
where idJobFunction_String_List is not null
""", engine_sqlite)
cand = cand.dropna()
func = cand.idJobFunction_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='func') \
    .drop('variable', axis='columns') \
    .dropna()

job_func = pd.read_sql("""
select j1.idJobFunction, j2.Value as fe, j1.Value as sfe
from JobFunction j1
left join JobFunction j2 on j1.ParentId = j2.idJobFunction
""", engine_sqlite)
func = func.merge(job_func, left_on='func', right_on='idJobFunction')

tem1 = func.loc[func['fe'].notnull()]
tem2 = func.loc[func['fe'].isnull()]
tem2['fe']=tem2['sfe']
tem2['sfe']=''
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(tem1, mylog)
vcand.insert_fe_sfe2(tem2, mylog)

# %% contact
cont = pd.read_sql("""
select P.idperson as contact_externalid, p.idJobFunction_String_List
from personx P
where isdeleted = '0' and idJobFunction_String_List is not null
""", engine_sqlite)

cont = cont.dropna()
func = cont.idJobFunction_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cont[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='func') \
    .drop('variable', axis='columns') \
    .dropna()

job_func = pd.read_sql("""
select j1.idJobFunction, j2.Value as fe, j1.Value as sfe
from JobFunction j1
left join JobFunction j2 on j1.ParentId = j2.idJobFunction
""", engine_sqlite)
func = func.merge(job_func, left_on='func', right_on='idJobFunction')
tem1 = func.loc[func['fe'].notnull()]
tem2 = func.loc[func['fe'].isnull()]
tem2['fe']=tem2['sfe']
tem2['sfe']=''
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(tem1, mylog)
vcont.insert_fe_sfe2(tem2, mylog)