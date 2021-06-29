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
cf.read('ls_config.ini')
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
""", engine_sqlite)
cand = cand.dropna()
func = cand.idJobFunction_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='func') \
    .drop('variable', axis='columns') \
    .dropna()
func['func'] = func['func'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

job_func = pd.read_sql("""
select j1.idJobFunction, j2.Value as fe, j1.Value as sfe
from JobFunction j1
left join JobFunction j2 on j1.ParentId = j2.idJobFunction
""", engine_sqlite)
job_func['value'] = job_func[['fe', 'sfe']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
job_func['idJobFunction'] = job_func['idJobFunction'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func = func.merge(job_func, left_on='func', right_on='idJobFunction', how='left')
cand_func = func[['candidate_externalid', 'value']]
cand_func['matcher'] = cand_func['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

function = pd.read_csv('fe-sfe.csv')
function['matcher'] = function['JobFunction'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

cand_func = cand_func.merge(function[['matcher','Functional Expertise','Sub functional Expertise']], on='matcher')
cand_fe = cand_func[['candidate_externalid','Functional Expertise','Sub functional Expertise']]
cand_fe = cand_fe.where(cand_fe.notnull(), None)
cand_fe['fe'] = cand_fe['Functional Expertise']
cand_fe['sfe'] = cand_fe['Sub functional Expertise']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand_fe, mylog)

# %% contact
cont = pd.read_sql("""
select P.idperson as contact_externalid, p.idJobFunction_String_List
from personx P
where isdeleted = '0'
""", engine_sqlite)

cont = cont.dropna()
func = cont.idJobFunction_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cont[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='func') \
    .drop('variable', axis='columns') \
    .dropna()
func['func'] = func['func'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

job_func = pd.read_sql("""
select j1.idJobFunction, j2.Value as fe, j1.Value as sfe
from JobFunction j1
left join JobFunction j2 on j1.ParentId = j2.idJobFunction
""", engine_sqlite)
job_func['value'] = job_func[['fe', 'sfe']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
job_func['idJobFunction'] = job_func['idJobFunction'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
func = func.merge(job_func, left_on='func', right_on='idJobFunction', how='left')
cont_func = func[['contact_externalid', 'value']]
cont_func['matcher'] = cont_func['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

function = pd.read_csv('fe-sfe.csv')
function['matcher'] = function['JobFunction'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

cont_func = cont_func.merge(function[['matcher','Functional Expertise','Sub functional Expertise']], on='matcher')
cont_fe = cont_func[['contact_externalid','Functional Expertise','Sub functional Expertise']]
cont_fe = cont_fe.where(cont_fe.notnull(), None)
cont_fe['fe'] = cont_fe['Functional Expertise']
cont_fe['sfe'] = cont_fe['Sub functional Expertise']
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont_fe, mylog)

# %% job
job = pd.read_sql("""
select ac.idassignment as job_externalid, ac.codeid
from assignmentcode ac
where idtablemd = '6051cf96-6d44-4aeb-925e-175726d0f97b'
""", engine_sqlite)
job = job.dropna()
job['CodeId'] = job['CodeId'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

job_func = pd.read_sql("""
select j1.idJobFunction, j2.Value as fe, j1.Value as sfe
from JobFunction j1
left join JobFunction j2 on j1.ParentId = j2.idJobFunction
""", engine_sqlite)
job_func['value'] = job_func[['fe', 'sfe']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
job_func['idJobFunction'] = job_func['idJobFunction'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
job = job.merge(job_func, left_on='CodeId', right_on='idJobFunction', how='left')

job_func_value = job[['job_externalid', 'value']]
job_func_value['matcher'] = job_func_value['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

function = pd.read_csv('fe-sfe.csv')
function['matcher'] = function['JobFunction'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

job_func_value = job_func_value.merge(function[['matcher','Functional Expertise','Sub functional Expertise']], on='matcher')

job_fe = job_func_value[['job_externalid','Functional Expertise','Sub functional Expertise']]

job_fe = job_fe.where(job_fe.notnull(), None)
job_fe['fe'] = job_fe['Functional Expertise']
job_fe['sfe'] = job_fe['Sub functional Expertise']
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_fe_sfe2(job_fe, mylog)