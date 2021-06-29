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
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)


# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %% candidate
# job_del = pd.read_csv('C:\\Users\\tony\\Desktop\\New folder\\job_to_rmv.csv')
matches = []
not_matches = []
vin_activities = pd.read_sql("""
select insert_timestamp, content from vincere_activity
""", engine_sqlite)
# assert False
limit = 10000
# index = 0
vin_activities['matcher_content'] = vin_activities['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
vin_activities['matcher_time'] = vin_activities['insert_timestamp'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

# activities = pd.read_sql("""
# select id, insert_timestamp, content from activity
# offset %s limit %s
# """ % (str(index*limit), str(limit)), engine_postgre)
# activities['matcher_content'] = activities['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

# activities['matcher_time'] = activities['insert_timestamp'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', str(x))).lower())
# activities['matcher_time'] = activities['matcher_time'] + '000000'
# match = vin_activities.merge(activities, on=['matcher_content','matcher_time'])
# match = match.drop_duplicates()
# not_match = activities.loc[~activities['id'].isin(match['id'])]
# not_match = not_match.drop_duplicates()
# matches.append(match)
# not_matches.append(not_match)
# print("done index: " + str(index))

def get_match(index):
    activities = pd.read_sql("""
         select id, insert_timestamp, content from activity
         offset %s limit %s
         """ % (str(index*limit), str(limit)), engine_postgre)
    activities['matcher_content'] = activities['content'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
    activities['matcher_time'] = activities['insert_timestamp'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', str(x))).lower())
    activities['matcher_time'] = activities['matcher_time'] + '000000'
    match = vin_activities.merge(activities, on=['matcher_content','matcher_time'])
    match = match.drop_duplicates()
    not_match = activities.loc[~activities['id'].isin(match['id'])]
    not_match = not_match.drop_duplicates()
    matches.append(match)
    not_matches.append(not_match)
    print("done index: " + str(index))

for i in range(974,1199):
    print("getting index: "+str(i))
    get_match(i)

# get_match(0)

match_df = pd.concat(matches)
tem1 = pd.DataFrame(match_df['id'].value_counts().keys(), columns=['id'])
not_match_df = pd.concat(not_matches)
tem2 = pd.DataFrame(not_match_df['id'].value_counts().keys(), columns=['id'])

#
# task = pd.read_sql("""
#     select id, insert_timestamp, content from activity
#     offset %s limit %s
#     """ % (str(index*limit), str(limit)), engine_postgre)
#
# cand_src['hidden_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.load_data_to_vincere(cand_src, dest_db, 'update', 'candidate_source', ['hidden_timestamp'], ['id'], mylog)