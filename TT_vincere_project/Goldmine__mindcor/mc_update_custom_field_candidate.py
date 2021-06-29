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
cf.read('mc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_mssql = sqlalchemy.create_engine(
    'mssql+pymssql://' + src_db.get('user') + ':' + src_db.get('password') + '@' + src_db.get(
        'server') + ':1433' + '/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'),
                                                                         dest_db.get('server'), dest_db.get('port'),
                                                                         dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200,
                                                 client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
candidate_info = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     , c1.CONTACT
     , LASTNAME
, nullif(UFEECAT,'') as currentCTC, nullif(UFEERATE,'') as Shares, nullif(UID,'') as UID
, nullif(UFEEGUARAN,'') as bonus, nullif(UFEEPAYMEN,'') as other
, nullif(USACITZEN,'') as USACITZEN, nullif(URACE,'') as URACE, nullif(USECBDAY,'') as placed_day
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')""", engine_mssql)
assert False

# %% Placed Date
tem = candidate_info[['candidate_externalid','placed_day']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = '7446688dd4fab9da16c87253891985b4'
tem['placed_day'] = pd.to_datetime(tem['placed_day'])
vincere_custom_migration.insert_candidate_date_field_values(tem, 'candidate_externalid', 'placed_day', api, connection)

# %% SA Citizenship
tem = candidate_info[['candidate_externalid','USACITZEN']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = 'aec76f15e7f89b743a7eb9f4bc278dc7'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'USACITZEN', api, connection)

# %% Race
tem = candidate_info[['candidate_externalid','URACE']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = 'f683cc60e02852c49e54a2bed1aac8ff'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'URACE', api, connection)

# %% Current CTC
tem = candidate_info[['candidate_externalid','currentCTC']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = 'f0643f2353f714cd96724a96dc4a94ef'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'currentCTC', api, connection)

# %% Shares
tem = candidate_info[['candidate_externalid','Shares']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = 'af36ecb2dcf307c83a3c74d12a273e7c'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'Shares', api, connection)

# %% Bonus
tem = candidate_info[['candidate_externalid','bonus']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = '39259514d411956bb091586ed01fd8e6'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'bonus', api, connection)

# %% Other
tem = candidate_info[['candidate_externalid','other']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = 'ca395923721b8b28f6f8a69ab291f511'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'other', api, connection)

# %% National ID
tem = candidate_info[['candidate_externalid','UID']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api = '7aee9171f7f9e345b0dea1cac4802d29'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'UID', api, connection)

# %% Contact National ID
tem = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid
     , nullif(UID,'') as UID
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')""", engine_mssql)
tem = tem.drop_duplicates()
api = 'a0379bb7323007e224f510455e46ad1c'
tem = tem.dropna()
vincere_custom_migration.insert_contact_text_field_values(tem, 'contact_externalid', 'UID', api, connection)
