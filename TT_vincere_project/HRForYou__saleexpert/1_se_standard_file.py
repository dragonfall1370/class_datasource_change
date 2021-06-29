# -*- coding: UTF-8 -*-
# import sys
# sys.path.append('D:\Tony\Working\DMvincere')
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
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
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('review_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %% candidate
sql = """
select cand.*
      , concat('KL',cand.user_id) as candidate_externalid
     , u_mail.reg_mail as owner
from (select u.user_id
     ,u.vorname
     ,u.nachname
     ,l.reg_mail
     ,p.zuordnung_intern
from user_data u
left join user_profil_stellensuchender p on u.user_id = p.user_id
left join user_login l on u.user_id = l.user_id
where p.profil_id is not null) cand
left join user_login u_mail on cand.zuordnung_intern = u_mail.user_id
where cand.user_id in (select user_profil_stellensuchender.user_id from user_profil_stellensuchender)
"""
candidate = pd.read_sql(sql, engine)
candidate = candidate.drop_duplicates()
candidate.owner.fillna('', inplace=True)
candidate = candidate.groupby(['candidate_externalid'])['owner'].apply(lambda x: ','.join(x)).reset_index()
#
candidate.rename(columns={
    'candidate_externalid': 'candidate-externalId',
    'vorname': 'candidate-firstName',
    'nachname': 'candidate-lastName',
    'reg_mail': 'candidate-email',
    'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)
assert False
# %% csv
candidate.to_csv(os.path.join(standard_file_upload, '4_candidate.csv'), index=False)
