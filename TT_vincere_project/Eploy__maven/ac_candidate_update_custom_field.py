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
cf.read('maven_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# sql = """
# select cand.*
# from (select u.user_id
#             ,u.vorname
#            , u.nachname
#      ,l.reg_mail
#       ,p.gewuenschte_jobbeschreibung
# , p.aktuelle_taetigkeit
# , p.bewerbungsprozess
# , p.ausschlusskriterien
# , p.wechselmotivation
# from user_data u
# left join user_profil_stellensuchender p on u.user_id = p.user_id
# left join user_login l on u.user_id = l.user_id
# where p.profil_id is not null) cand
# """
# candidate = pd.read_sql(sql, engine)
#
# # %% Gewünschte Jobbeschreibung
# api = 'e47cd0d51b6acb26c65c0c6bf9eecd93'
# cand = candidate[['user_id', 'gewuenschte_jobbeschreibung']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.gewuenschte_jobbeschreibung != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'gewuenschte_jobbeschreibung', api, connection)
#
# # %% Bewerbungsprozess
# api = '63739451f4ca754a001f18d6b2f54a9f'
# cand = candidate[['user_id', 'bewerbungsprozess']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.bewerbungsprozess != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'bewerbungsprozess', api, connection)
#
# # %% Ausschlusskriterien
# api = 'a2f764848645c2c7cc291cedefe35dc0'
# cand = candidate[['user_id', 'ausschlusskriterien']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.ausschlusskriterien != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'ausschlusskriterien', api, connection)
#
# # %% Wechselmotivation
# api = 'e5f66aee72608b7ee8b2cdc48f6ff5c0'
# cand = candidate[['user_id', 'wechselmotivation']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.wechselmotivation != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'wechselmotivation', api, connection)
#
# # %% Aktuelle Tätigkeit
# api = '68821616d186cd12b30514382e64096b'
# cand = candidate[['user_id', 'aktuelle_taetigkeit']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.aktuelle_taetigkeit != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'aktuelle_taetigkeit', api, connection)


#%% Verfügbar ab
# sql = """
# select cand.*
# from (select u.user_id
# , p.eintritt_text
# ,p.profil_titel
# from user_data u
# left join user_profil_stellensuchender p on u.user_id = p.user_id
# left join user_login l on u.user_id = l.user_id
# where p.profil_id is not null) cand
# where cand.eintritt_text in
#       ('2 Monate zum Monatsende'
#       ,'3 Monate zum Monatsende'
#       ,'3 Monate zum Quartal'
#       ,'4 Wochen zum Monatsende'
#       ,'6 Monate zum Monatsende'
#       ,'6 Monate zum Quartalsende'
#       ,'6 Wochen zum Monatsende'
#       ,'6 Wochen zum Quartalsende'
#       ,'ab sofort'
#       ,'gesetzliche Kündigungsfrist'
#       ,'kurzfristig'
#       ,'nach Vereinbarung')
# and cand.profil_titel = 'Standardprofil'
# """
#
# candidate_dropdown = pd.read_sql(sql, engine)
#
# api = 'b5952e647ae76d03999d9418a09bbb8d'
# cand = candidate_dropdown[['user_id', 'eintritt_text']]
# cand['eintritt_text'] = cand['eintritt_text'].str.lower()
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.eintritt_text != '']
# vincere_custom_migration.insert_candidate_drop_down_list_values(cand, 'user_id', 'eintritt_text', api, connection)

# # %% Verfügbar ab text
# sql = """
# select cand.*
# from (select u.user_id
# , p.eintritt_text
# ,p.profil_titel
# from user_data u
# left join user_profil_stellensuchender p on u.user_id = p.user_id
# left join user_login l on u.user_id = l.user_id
# where p.profil_id is not null) cand
# where cand.eintritt_text not in
#       ('2 Monate zum Monatsende'
#       ,'3 Monate zum Monatsende'
#       ,'3 Monate zum Quartal'
#       ,'4 Wochen zum Monatsende'
#       ,'6 Monate zum Monatsende'
#       ,'6 Monate zum Quartalsende'
#       ,'6 Wochen zum Monatsende'
#       ,'6 Wochen zum Quartalsende'
#       ,'ab sofort'
#       ,'gesetzliche Kündigungsfrist'
#       ,'kurzfristig'
#       ,'nach Vereinbarung')
# and cand.profil_titel = 'Standardprofil'
# """
#
# candidate_dropdown_text = pd.read_sql(sql, engine)
#
# api = 'd72d11ac53b6156097ed2bc4b5022324'
# cand = candidate_dropdown_text[['user_id', 'eintritt_text']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.eintritt_text != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'eintritt_text', api, connection)

# %% Arbeitsorte
# sql = """
# select cand.*
# from (select u.user_id
#              ,u.vorname
#            , u.nachname
#      ,l.reg_mail
#       ,p.arbeitsorte
# , p.eintritt_text
# , p.gesuchte_region
# ,p.profil_titel
# from user_data u
# left join user_profil_stellensuchender p on u.user_id = p.user_id
# left join user_login l on u.user_id = l.user_id
# where p.profil_id is not null) cand
# where cand.profil_titel = 'Standardprofil'
# """
#
# candidate = pd.read_sql(sql, engine)
#
# api = 'e8088bb4816646541c755069ac1b9a19'
# cand = candidate[['user_id', 'arbeitsorte']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.arbeitsorte != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'arbeitsorte', api, connection)


# %% Region
# region_mapping = pd.read_sql("select id, region_de from cat_region;", engine)
#
# sql = """
# select cand.*
# from (select u.user_id
#       ,p.gesuchte_region
# from user_data u
# left join user_profil_stellensuchender p on u.user_id = p.user_id
# left join user_login l on u.user_id = l.user_id
# where p.profil_id is not null) cand
# """
#
# region = pd.read_sql(sql, engine)
# # assert False
# cand = vincere_common.splitDataFrameList(region, 'gesuchte_region', '|')
# api = '96f5561394d8d0fe5be82c6907b775cf'
# cand['user_id'] = cand['user_id'].astype(str)
# cand['gesuchte_region'] = cand['gesuchte_region'].astype(str)
# region_mapping['id'] = region_mapping['id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.gesuchte_region != '']
#
# cand = cand.merge(region_mapping, left_on='gesuchte_region', right_on='id')
#
# vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand, 'user_id', 'region_de', api, connection)


# %% BEE status
sql = """
select link.candidate_externalid , cat.KEYWORD from
(select RecordId as candidate_externalid, k.KeywordId
from KeywordRecordLink k
join Applicants c on k.RecordId = c.APP_ID) link
left join  (select kw.DICT_ID, kw.KEYWORD, kw.DEFINITION, kc.TYPE_NAME , kc.CAT_ID from Keywords kw
left join KeywordCategories kc on kc.CAT_ID = kw.TYPE) cat on link.KeywordId = cat.DICT_ID
where
 cat.CAT_ID = 10
"""
candidate_dropdown = pd.read_sql(sql, engine_mssql)
assert False
api = 'c2bec2bd3a55c7c23e2a5bcb018b3941'
cand = candidate_dropdown[['candidate_externalid', 'KEYWORD']]
cand = cand.drop_duplicates()
cand = cand.loc[cand.KEYWORD != '']

vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand, 'candidate_externalid', 'KEYWORD', api, connection)


# # %% Mobilität text
# sql = """
# select u.user_id,
#        u.nachname,
#        u.vorname,
#        p.mobilitaet
# from user_data u
# left join user_profil_stellensuchender p on u.user_id = p.user_id
# where p.profil_id is not null
# and p.mobilitaet not in ('Umzug','Wochenendpendler','Tagespendler')
# """
#
# candidate_dropdown_text = pd.read_sql(sql, engine)
#
# api = 'ae19663877e00fa0a4ebe89beadce8b6'
# cand = candidate_dropdown_text[['user_id', 'mobilitaet']]
# cand['user_id'] = cand['user_id'].astype(str)
# cand = cand.drop_duplicates()
# cand = cand.loc[cand.mobilitaet != '']
# vincere_custom_migration.insert_candidate_text_field_values(cand, 'user_id', 'mobilitaet', api, connection)

