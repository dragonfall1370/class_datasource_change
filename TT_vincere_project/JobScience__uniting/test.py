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
import json

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

# def split_dataframe_to_chunks(df, n):
#    df_len = len(df)
#    count = 0
#    dfs = []
#    while True:
#       if count > df_len - 1:
#          break
#
#       start = count
#       count += n
#       # print("%s : %s" % (start, count))
#       dfs.append(df.iloc[start: count])
#    return dfs
#
# csv_path = r'C:\Users\tony\Desktop\skip_file_application'
# temp_msg_metadata = vincere_common.get_folder_structure(csv_path)
# application_other = []
# for index, row in temp_msg_metadata.iterrows():
#    print(row['file_fullpath'])
#    tem = pd.read_csv(row['file_fullpath'])
#    application_other.append(tem)
# df = pd.concat(application_other)

# split_df_to_chunks1 = split_dataframe_to_chunks(df, 50000)
# for idx, val in enumerate(split_df_to_chunks1):
#    print(idx)
#    val.to_csv(os.path.join(standard_file_upload, '7_jobapplication_placement' + '_' + str(idx) + '.csv'), index=False)

# # cand_skip = pd.read_csv(os.path.join(standard_file_upload, 'skip_cand_23.csv'))
# cand_skip = pd.concat(skip_file)
# exist_cand = pd.read_sql("""
# select id, external_id, email, first_name, last_name from candidate where deleted_timestamp is null
# """, engine_postgre)
# # assert False
# exist_cand['matcher'] = exist_cand[['external_id', 'email']].apply(lambda x: '_'.join([e for e in x if e]), axis=1)
# cand_skip['matcher'] = cand_skip[['candidate-externalId', 'candidate-email']].apply(lambda x: '_'.join([e for e in x if e]), axis=1)
#
# cand1 = exist_cand.merge(cand_skip, on='matcher')
# exist_cand = exist_cand.loc[~exist_cand['id'].isin(cand1['id'])]
# cand = exist_cand.loc[exist_cand['email'].str.lower().isin(cand_skip['candidate-email'])]
#
#
# cand = cand.loc[~cand['external_id'].str.lower().isin(cand_skip['candidate-externalId'].str.lower())]
# # cand_skip.loc[cand_skip['candidate-email']=='dave.brown@drjtechnology.co.uk']
# # ca = cand.loc[cand['external_id'].isnull()]
# cand_to_add = cand_skip.loc[cand_skip['candidate-email'].str.lower().isin(cand['email'].str.lower())]
# cand_to_add = cand_to_add.drop('Errors', axis=1)
# cand_to_add['candidate-email'] = cand_to_add['candidate-externalId']+'_'+cand_to_add['candidate-email']
# cand_to_add.to_csv(os.path.join(standard_file_upload, 'cand_to_add_final.csv'))





# job_del = pd.read_csv('C:\\Users\\tony\\Desktop\\New folder\\job_to_rmv.csv')
# job = pd.read_sql("""
# select id, name, external_id, insert_timestamp, deleted_timestamp from position_description where external_id is not null
# """, engine_postgre)
#
# assert False
# job['matcher'] = job['external_id'].apply(lambda x: x[:-3])
# job = job.loc[job['matcher'].isin(job_del['Vancacy Id'])]
# job['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.load_data_to_vincere(job, dest_db, 'update', 'position_description', ['deleted_timestamp'], ['id'], mylog)



# candidate = pd.read_csv('Placement_new.csv')
#
# cand = pd.read_sql("""
# select external_id as candidate_externalid from candidate where external_id is not null and deleted_timestamp is null
# """, engine_postgre)
# assert False
# ca = candidate.merge(cand, left_on='ts2__Employee__c', right_on='candidate_externalid')
#
# cand_add = candidate.loc[~candidate['ts2__Employee__c'].isin(ca['ts2__Employee__c'])]
# cand_add.to_csv('cand_add.csv')


job = pd.read_csv('Placement_new.csv')

job_db = pd.read_sql("""
select external_id from position_description
""", engine_postgre)
job = job[['ts2__Job__c']]
job = job.drop_duplicates()
assert False
j = job.merge(job_db, left_on='ts2__Job__c', right_on='external_id')

j_add = job.loc[~job['ts2__Job__c'].isin(j['ts2__Job__c'])]
j_add.info()
j_add.to_csv('j_add.csv')


# contacts = pd.read_csv('Job_new.csv')
#
# cont = pd.read_sql("""
# select external_id from contact
# """, engine_postgre)
# assert False
# c = contacts.merge(cont, left_on='ts2__Contact__c', right_on='external_id')
#
# c_add = contacts.loc[~contacts['ts2__Contact__c'].isin(c['ts2__Contact__c'])]
# c_add.to_csv('contact_add.csv')
#
# c_add['ts2__Contact__c']
#
# company = c_add
#
# comp = pd.read_sql("""
# select external_id from company
# """, engine_postgre)
# com = company.merge(comp, left_on='ts2__Account__c', right_on='external_id')
# company.loc[~company['ts2__Account__c'].isin(com['ts2__Account__c'])]

