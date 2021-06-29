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
cf.read('ec_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# assert False
# %% hots
sql = """
select idPerson as candidate_externalid, h.Value
from PersonCode pc
left join udHots h on pc.CodeId = h.idudHots
where idtablemd = '853718bc-374f-46c2-8b93-709026fcfa8b'
"""
candidate = pd.read_sql(sql, engine_sqlite)
candidate = candidate.dropna()
candidate['matcher'] = candidate['Value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem2 = pd.read_csv('hots.csv')
tem2['matcher'] = tem2['Document Category field in File Finder'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = candidate.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
tem3['_merge'].unique()
a = tem3.loc[tem3['_merge']=='right_only']

api = 'd5b6dff5678f5c48b0559fbcd4571760'
cand = tem3[['candidate_externalid', 'Vincere Value']]
cand = cand.drop_duplicates()
vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand, 'candidate_externalid', 'Vincere Value', api, connection)

# %% rating
sql = """
select idPerson as candidate_externalid, pr.Value
from PersonX p
left join PersonRating pr on pr.idPersonRating = p.idPersonRating_String
where idPersonRating_String is not null
"""
cand = pd.read_sql(sql, engine_sqlite)
api = '5338ff25fb6ed45bd423c49f9f42f6fe'
cand = cand.drop_duplicates()
cand['Value'] = cand['Value'].str.strip()
vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand, 'candidate_externalid', 'Value', api, connection)

# %% international
sql = """
select P.idperson as candidate_externalid, p.idInternational_String_List
               from personx P
where isdeleted = '0'
and idInternational_String_List is not null
"""
candidate = pd.read_sql(sql, engine_sqlite)
candidate = candidate.dropna()
countries = candidate.idInternational_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idInternational') \
    .drop('variable', axis='columns') \
    .dropna()
countries['idInternational'] = countries['idInternational'].str.lower()
intn = pd.read_sql("""select idInternational , Value from International""", engine_sqlite)
countries = countries.merge(intn, on='idInternational', how='left')
countries['matcher'] = countries['Value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem2 = pd.read_csv('international.csv')
tem2['matcher'] = tem2['Document Category field in File Finder'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

tem3 = countries.merge(tem2, on='matcher', suffixes=['', '_y'], how='outer', indicator=True)
# Guatemala Central America
tem3 = tem3.loc[tem3['Vincere Value'].notnull()]
tem3 = tem3.drop_duplicates()
cand_international_api = 'ab361582310a5db668e64344d04444ed'
cand = tem3[['candidate_externalid', 'Vincere Value']]
cand = cand.drop_duplicates()
cand = cand.where(cand.notnull(), None)
vincere_custom_migration.insert_candidate_muti_selection_checkbox(cand, 'candidate_externalid', 'Vincere Value', cand_international_api, connection)


