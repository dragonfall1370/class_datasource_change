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
cf.read('pa_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False

# %% contact
cont = pd.read_sql("""
select cont_info.* from
(select p.idperson as contact_externalid
      , p.jobfunctionvalue_string
from personx p
join
(select cp.idperson, cp.idcompany,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company
where companyid in ('5714'
,'5745'
,'5841'
,'4138'
,'4990'
,'1611'
,'236'
,'1494'
,'379'
,'5038'
,'650'
,'3038'
,'1046'
,'4020'
,'4063'
,'107'
,'1030'
,'2159'
,'3982'
,'2179'
,'5508'
,'757'
,'1792'
,'4634'
,'281'
,'3535'
,'4348'
,'5846'
,'5855'
,'5869'
,'5940'
,'5941'
,'5982'
,'5991'
,'5996'
,'5999'
,'6002'
,'6004'
,'6005'
,'6006'
,'6007'
,'6008'
,'6009'
,'6010'
,'6011'
,'6012'
,'6013'
,'6014'
,'6015'
,'6016'
,'6017'
,'6018'
,'6019'
,'6021'
,'6025'
,'6026'
,'6027'
,'6028'
,'6029'
,'6031'
,'6034'
,'6035'
,'6037'
,'6038'
,'6039'
,'6040'
,'6042'
,'6043'
,'6044'
,'6045'
,'6046'
,'6047'
,'6048'
,'6049'
,'6050'
,'6051'
,'6052'
,'6053'
,'6054'
,'6055'
,'6056'
,'6057'
,'6058'
,'6060'
,'6078'
,'6079'
,'6080'
,'6081'
,'6082'
,'6083'
,'6096'
,'6097'
,'6098'
,'6099'
,'6100'
,'6176'
,'6237'
,'6241')) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
where cont.rn = 1) cont_info
""", engine_postgre_src)
cont = cont.dropna()
func = cont.jobfunctionvalue_string.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cont[['contact_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['contact_externalid'], value_name='func') \
    .drop('variable', axis='columns') \
    .dropna()
func['func'] = func['func'].str.strip()
function = pd.read_csv('fe-sfe.csv')
func = func.merge(function, left_on='func', right_on='Level 2')
cont_func = func[['contact_externalid','Functional Expertise','Sub-Functional Expertise']]
cont_func['fe'] = cont_func['Functional Expertise']
cont_func['sfe'] = cont_func['Sub-Functional Expertise']
# assert False
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
vcont.insert_fe_sfe2(cont_func, mylog)

# %% candidate
cand = pd.read_sql("""
select px.*
from candidate c
join (select P.idperson as candidate_externalid, p.jobfunctionvalue_string
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
""", engine_postgre_src)
cand = cand.dropna()
func = cand.jobfunctionvalue_string.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='func') \
    .drop('variable', axis='columns') \
    .dropna()
func['func'] = func['func'].str.strip()
function = pd.read_csv('fe-sfe.csv')
func = func.merge(function, left_on='func', right_on='Level 2')
cand_func = func[['candidate_externalid','Functional Expertise','Sub-Functional Expertise']]
cand_func['fe'] = cand_func['Functional Expertise']
cand_func['sfe'] = cand_func['Sub-Functional Expertise']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_fe_sfe2(cand_func, mylog)