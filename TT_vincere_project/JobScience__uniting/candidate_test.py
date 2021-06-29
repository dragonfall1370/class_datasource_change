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

# %%
# from common import vincere_candidate
# vcand = vincere_candidate.Candidate(connection)

# # %% candidate
# candidate = pd.read_sql("""
# select
#        c.Id as candidate_externalid, c.FirstName, c.LastName, c.Current_JobTitle__c
# from Contacts c
# join RecordType r on (c.RecordTypeId || 'IAQ') = r.Id
# left join Account com on (c.AccountId = com.Id and com.IsDeleted=0)
# left join User u on c.OwnerId = u.Id
# where c.IsDeleted = 0
# and r.Name = 'Candidate'
# --and c.CreatedDate >= '2018-01-01'
# """, engine_sqlite)
#
# assert False
# japp = pd.read_sql("""
# select
# c.ts2__Job__c as job_externalid
# , j.Name as job_tile
# , c.ts2__Candidate_Contact__c as candidate_externalid
# from ts2__Application__c c
# left join ts2__Job__c j on c.ts2__Job__c = j.Id
# where c.IsDeleted=0 and c.LastModifiedDate >= '2017-11-01' and c.LastModifiedDate <= '2017-11-30'
# and c.ts2__Stage__c in ('Submittal', 'Offer', 'Placement',
#                         'Second Client Interview',
#                         'Final Client Interview', 'Client Interview')
# """, engine_sqlite)
#
# japp = pd.read_sql("""
# select
# c.ts2__Job__c as job_externalid
# , j.Name as job_tile
# , c.ts2__Candidate_Contact__c as candidate_externalid
# from ts2__Application__c c
# left join ts2__Job__c j on c.ts2__Job__c = j.Id
# where c.IsDeleted=0 and c.LastModifiedDate >= '2019-10-01' and c.LastModifiedDate <= '2019-10-31'
# and c.ts2__Stage__c in ('Submittal', 'Offer', 'Placement',
#                         'Second Client Interview',
#                         'Final Client Interview', 'Client Interview')
# """, engine_sqlite)
#
# match = candidate.merge(japp, on='candidate_externalid')
# match['Candidate_job_tile'] = match['Current_JobTitle__c']
# tem = match[['candidate_externalid', 'FirstName', 'LastName','Candidate_job_tile', 'job_tile']]
# tem1 = tem[0:50]
# tem1.to_csv('candiadte_application_from_Oct_2019.csv')
# a = match[['candidate_externalid']].drop_duplicates()


# %% candidate
# candidate = pd.read_sql("""
# select
#        c.Id as candidate_externalid
# from Contacts c
# join RecordType r on (c.RecordTypeId || 'IAQ') = r.Id
# left join Account com on (c.AccountId = com.Id and com.IsDeleted=0)
# left join User u on c.OwnerId = u.Id
# where c.IsDeleted = 0
# and r.Name = 'Candidate'
# --and c.CreatedDate >= '2018-01-01'
# """, engine_sqlite)
#
# japp = pd.read_sql("""
# select c.Id
# --select distinct c.ts2__Candidate_Contact__c as candidate_externalid
# from ts2__Application__c c where c.IsDeleted=0 and c.LastModifiedDate >= '2017-10-31'
# and c.ts2__Stage__c in ('Submittal', 'Offer', 'Placement', 'Application',
#                          'Second Client Interview',
#                         'Final Client Interview', 'Client Interview')
# """, engine_sqlite)
#
# match = candidate.merge(japp, on='candidate_externalid')

# csv_path = r'C:\Users\tony\Desktop\skip_file'
# temp_msg_metadata = vincere_common.get_folder_structure(csv_path)
# skip_file = []
# for index, row in temp_msg_metadata.iterrows():
#    print(row['file_fullpath'])
#    tem = pd.read_csv(row['file_fullpath'])
#    skip_file.append(tem)
#
#
# cand_skip = pd.read_csv(os.path.join(standard_file_upload, 'skip_cand_23.csv'))
# cand_skip = pd.read_csv('C:\\Users\\tony\\Desktop\\skip_file_cand.csv')
# # cand_skip = pd.concat(skip_file)
# exist_cand = pd.read_sql("""
# select id, external_id, email, first_name, last_name from candidate where deleted_timestamp is null
# """, engine_postgre)
# assert False
#
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
#
#
#
# cand_add = cand_skip.loc[~cand_skip['candidate-externalId'].isin(exist_cand['external_id'])]
# cand_add['candidate-email'] = cand_add['candidate-externalId']+'_'+cand_add['candidate-email']
# cand_add.to_csv(os.path.join(standard_file_upload, 'cand_to_add_final_2.csv'))
