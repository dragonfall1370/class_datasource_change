# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
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
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
review_db = cf[cf['default'].get('review_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)


connection_str1 = "mysql+pymysql://root:123qwe@dmpfra.vinceredev.com/komplettes"
engine1 = sqlalchemy.create_engine(connection_str1)

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()


from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
assert False
# %%
sql = """
select cand.*
from (select concat('SE',u.user_id) as user_id
            ,u.vorname
           , u.nachname
     ,l.reg_mail
      ,p.gewuenschte_jobbeschreibung
, p.aktuelle_taetigkeit
, p.bewerbungsprozess
, p.ausschlusskriterien
, p.wechselmotivation
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
"""
candidate1 = pd.read_sql(sql, engine)
candidate1 = candidate1.loc[candidate1['wechselmotivation'] !='']

sql2 = """
select cand.*
from (select concat('KL',u.user_id) as user_id
            ,u.vorname
           , u.nachname
     ,l.reg_mail
      ,p.gewuenschte_jobbeschreibung
, p.aktuelle_taetigkeit
, p.bewerbungsprozess
, p.ausschlusskriterien
, p.wechselmotivation
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
"""
candidate2 = pd.read_sql(sql2, engine1)
candidate2 = candidate2.loc[candidate2['wechselmotivation'] !='']
candidate = pd.concat([candidate1,candidate2])
assert False
# %% Wechselmotivation
api = '43be9c274474e3813c3c7d5135d5509f'
cand = candidate[['user_id', 'wechselmotivation']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.wechselmotivation != '']

vincere_custom_migration.insert_candidate_text_field_values2(cand, review_db, mylog, 'user_id', 'wechselmotivation', api, connection)
