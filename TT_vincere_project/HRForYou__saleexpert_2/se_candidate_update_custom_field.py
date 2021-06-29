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
# assert False
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
candidate = pd.read_sql(sql, engine)
# assert False
# %% Gewünschte Jobbeschreibung
api = 'feecaea5b7564fc02b40767302f39a9b'
cand = candidate[['user_id', 'gewuenschte_jobbeschreibung']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.gewuenschte_jobbeschreibung != '']

vincere_custom_migration.insert_candidate_text_field_values2(cand, review_db, mylog, 'user_id', 'gewuenschte_jobbeschreibung', api, connection)

# %% Bewerbungsprozess
api = '9f27257ae9f84bfd4274bebc81e264f1'
cand = candidate[['user_id', 'bewerbungsprozess']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.bewerbungsprozess != '']

vincere_custom_migration.insert_candidate_text_field_values2(cand, review_db, mylog, 'user_id', 'bewerbungsprozess', api, connection)

# %% Ausschlusskriterien
api = '4cfda2b8a380e4a6ce4a865c41bb0fe6'
cand = candidate[['user_id', 'ausschlusskriterien']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.ausschlusskriterien != '']

vincere_custom_migration.insert_candidate_text_field_values2(cand, review_db, mylog, 'user_id', 'ausschlusskriterien', api, connection)

# %% Wechselmotivation
api = 'e7c67bd50ff1ed07146e89219e80f60e'
cand = candidate[['user_id', 'wechselmotivation']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.wechselmotivation != '']

vincere_custom_migration.insert_candidate_text_field_values2(cand, review_db, mylog, 'user_id', 'wechselmotivation', api, connection)

# %% Aktuelle Tätigkeit
api = 'fbf1453ecad8e95b0b554fb24652db4d'
cand = candidate[['user_id', 'aktuelle_taetigkeit']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.aktuelle_taetigkeit != '']

vincere_custom_migration.insert_candidate_text_field_values2(cand, review_db, mylog, 'user_id', 'aktuelle_taetigkeit', api, connection)


# %% Verfügbar ab
sql = """
select cand.*
from (select concat('KL',u.user_id) as user_id
, p.eintritt_text
,p.profil_titel
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
where cand.eintritt_text in
      ('2 Monate zum Monatsende'
      ,'3 Monate zum Monatsende'
      ,'3 Monate zum Quartal'
      ,'4 Wochen zum Monatsende'
      ,'6 Monate zum Monatsende'
      ,'6 Monate zum Quartalsende'
      ,'6 Wochen zum Monatsende'
      ,'6 Wochen zum Quartalsende'
      ,'ab sofort'
      ,'gesetzliche Kündigungsfrist'
      ,'kurzfristig'
      ,'nach Vereinbarung')
and cand.profil_titel = 'Standardprofil'
"""
tem1 = pd.read_sql(sql, engine1)

sql1 = """
select cand.*
from (select concat('SE',u.user_id) as user_id
, p.eintritt_text
,p.profil_titel
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
where cand.eintritt_text in
      ('2 Monate zum Monatsende'
      ,'3 Monate zum Monatsende'
      ,'3 Monate zum Quartal'
      ,'4 Wochen zum Monatsende'
      ,'6 Monate zum Monatsende'
      ,'6 Monate zum Quartalsende'
      ,'6 Wochen zum Monatsende'
      ,'6 Wochen zum Quartalsende'
      ,'ab sofort'
      ,'gesetzliche Kündigungsfrist'
      ,'kurzfristig'
      ,'nach Vereinbarung')
and cand.profil_titel = 'Standard Profil'
"""
tem2 = pd.read_sql(sql1, engine)
candidate_dropdown = pd.concat([tem1,tem2])

api = 'ce4eb1a35ddf60899f72f5584f5deaa2'
cand = candidate_dropdown[['user_id', 'eintritt_text']]
cand['eintritt_text'] = cand['eintritt_text'].str.lower()
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand.eintritt_text != '']

vincere_custom_migration.insert_candidate_drop_down_list_values_2(cand, review_db, mylog,'user_id', 'eintritt_text', api, connection)

# %% Verfügbar ab text
sql = """
select cand.*
from (select concat('SE',u.user_id) as user_id
, p.eintritt_text
,p.profil_titel
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
where cand.eintritt_text not in
      ('2 Monate zum Monatsende'
      ,'3 Monate zum Monatsende'
      ,'3 Monate zum Quartal'
      ,'4 Wochen zum Monatsende'
      ,'6 Monate zum Monatsende'
      ,'6 Monate zum Quartalsende'
      ,'6 Wochen zum Monatsende'
      ,'6 Wochen zum Quartalsende'
      ,'ab sofort'
      ,'gesetzliche Kündigungsfrist'
      ,'kurzfristig'
      ,'nach Vereinbarung')
and cand.profil_titel = 'Standard Profil'
"""

candidate_dropdown_text = pd.read_sql(sql, engine)

api = '338b6754557b9251418ba61f9a2a300f'
cand = candidate_dropdown_text[['user_id', 'eintritt_text']]
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand.eintritt_text != '']
vincere_custom_migration.append_text_field_values_2(cand, review_db, 'user_id', 'eintritt_text', api, connection,'candidate',  mylog)

# %% Arbeitsorte
sql = """
select cand.*
from (select concat('SE',u.user_id) as user_id
             ,u.vorname
           , u.nachname
     ,l.reg_mail
      ,p.arbeitsorte
, p.eintritt_text
, p.gesuchte_region
,p.profil_titel
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
where cand.profil_titel = 'Standard Profil'
"""

candidate = pd.read_sql(sql, engine)

api = '0e4dcb9e7baa6c43a951e625f695cfaf'
cand = candidate[['user_id', 'arbeitsorte']]
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand.arbeitsorte != '']
vincere_custom_migration.append_text_field_values_2(cand,review_db, 'user_id', 'arbeitsorte', api, connection,'candidate',  mylog)

# %% Region
region_mapping = pd.read_sql("select id, region_de from cat_region;", engine)

sql = """
select cand.*
from (select concat('SE',u.user_id) as user_id
      ,nullif(p.gesuchte_region,'') as gesuchte_region
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
where gesuchte_region is not null
"""
region = pd.read_sql(sql, engine)
# assert False
cand = vincere_common.splitDataFrameList(region, 'gesuchte_region', '|')
api = '6badab49d06732c7875c3ae0ec3869cd'
cand['user_id'] = cand['user_id'].astype(str)
cand['gesuchte_region'] = cand['gesuchte_region'].astype(str)
region_mapping['id'] = region_mapping['id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand.gesuchte_region != '']

cand = cand.merge(region_mapping, left_on='gesuchte_region', right_on='id')

vincere_custom_migration.append_muti_selection_checkbox_2(cand, review_db, 'user_id', 'region_de', api, connection, 'candidate', mylog)


# %% Mobilität ab
sql = """
select concat('SE',u.user_id) as user_id,
       u.nachname,
       u.vorname,
       p.mobilitaet
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
where p.profil_id is not null
and p.mobilitaet in ('Umzug','Wochenendpendler','Tagespendler')
"""

candidate_dropdown = pd.read_sql(sql, engine)

api = '9d22f06fdca92cd0c6ce575996a0fe7f'
cand = candidate_dropdown[['user_id', 'mobilitaet']]
cand['mobilitaet'] = cand['mobilitaet'].str.lower()
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand.mobilitaet != '']
vincere_custom_migration.append_drop_down_list_2(cand, review_db, 'user_id', 'mobilitaet', api, connection, 'candidate', mylog)
# assert False

# %% Mobilität text
sql = """
select concat('SE',u.user_id) as user_id,
       u.nachname,
       u.vorname,
       p.mobilitaet
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
where p.profil_id is not null
and p.mobilitaet not in ('Umzug','Wochenendpendler','Tagespendler')
"""

candidate_dropdown_text = pd.read_sql(sql, engine)

api = '07b11e7d4dcde7b0b9b4da88a0ad5269'
cand = candidate_dropdown_text[['user_id', 'mobilitaet']]
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand.mobilitaet != '']
vincere_custom_migration.append_text_field_values_2(cand,review_db, 'user_id', 'mobilitaet', api, connection,'candidate',  mylog)

# %% Status
sql = """
select concat('KL',u.user_id) as user_id,
       kuerzel_de as status
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join cat_bewerber_status cbs on cbs.id = p.status1
where p.profil_id is not null
"""
tem1 = pd.read_sql(sql, engine1)

sql1 = """
select concat('SE',u.user_id) as user_id,
       kuerzel_de as status
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join cat_bewerber_status cbs on cbs.id = p.status1
where p.profil_id is not null
"""
tem2 = pd.read_sql(sql1, engine)
candidate_dropdown = pd.concat([tem1,tem2])
candidate_dropdown = candidate_dropdown.dropna()
api = 'efa6dd4f44c0bd04b1bac7492e81eaf3'
cand = candidate_dropdown[['user_id', 'status']]
cand['user_id'] = cand['user_id'].astype(str)
cand = cand.drop_duplicates()
cand = cand.loc[cand.status != '']
cand['rn'] = cand.groupby('user_id').cumcount()
tem1 = cand.loc[cand['rn']>0]
tem2 = cand.loc[~cand['user_id'].isin(tem1['user_id'])]
cand = pd.concat([tem1,tem2])
cand.loc[cand['user_id']=='KL82']
vincere_custom_migration.insert_candidate_text_field_values2(cand, review_db,mylog, 'user_id', 'status', api, connection)