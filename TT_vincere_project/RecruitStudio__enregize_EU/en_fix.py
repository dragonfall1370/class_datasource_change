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
cf.read('en_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
# data_folder = '/Users/truongtung/Desktop'
sqlite_path = cf['default'].get('sqlite_path')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

#%%
# job_cont = pd.read_sql("""
# with cont_tmp as (
# select ContactId
#      , coalesce(com.CompanyId, com2.CompanyId) as companyId
#     , ROW_NUMBER() OVER(PARTITION BY c.ContactId ORDER BY ContactId DESC) rn
# from Contacts c
# left join Companies com on com.CompanyId = c.CompanyId
# left join Companies com2 on com2.CompanyName = c.Company
# where  Descriptor = 1)
# select
# --        cont.ContactId as current, v.ContactId as old, DisplayName
#       nullif(v.JobNumber,'') as external_id
#      , nullif(v.JobTitle,'') as JobTitle
#      , nullif(v.CompanyId,'') as CompanyId
#      , nullif(u.Email,'') as owner
# from Vacancies v
# left join (select * from cont_tmp where rn = 1) cont on cont.ContactId = v.ContactId and cont.companyId = v.CompanyId
# left join Users u on v.UserName = u.UserName
# where nullif(cont.ContactId,'') is null and nullif(v.ContactId,'') is not null and nullif(v.CompanyId,'') is not null
# """, engine_mssql)
# job_cont['external_id'] = 'EUK'+job_cont['external_id']
# # job_cont['CompanyId'] = job_cont['CompanyId'].apply(lambda x: 'EUK'+x if x else x)
# # job_cont['ContactId'] = job_cont['ContactId'].apply(lambda x: 'EUK'+x if x else x)
# assert False
# job_cont_prod = pd.read_sql("""
# select pd.contact_id, pd.external_id, first_name, last_name, c2.name from position_description pd
# join contact c on pd.contact_id = c.id
# join company c2 on c.company_id = c2.id
# """, engine_postgre_review)
#
# job_cont_prod = job_cont_prod.loc[job_cont_prod['external_id'].isin(job_cont['external_id'])]
# job_cont_prod['first_name'] = 'CONTACT'
# job_cont_prod['last_name'] = 'LEFT_Energize-UK'
# job_cont_prod['id'] = job_cont_prod['contact_id']
# vincere_custom_migration.psycopg2_bulk_update_tracking(job_cont_prod, connection, ['first_name','last_name' ], ['id', ], 'contact', mylog)

# %%
# a=[]
# import os,shutil
# for root, dirs, files in os.walk("D:\Tony\Energize_file\EN_PROD\Files_PROD"):
#     for file in files:
#         if file.endswith(".txt"):
#              b = os.path.join(root, file)
#              print(b)
#              shutil.copy2(b, "D:\Tony\Energize_file\EN_PROD\TXT")
#
#
# import glob, os
#
# for filename in glob.iglob(os.path.join("D:\Tony\Energize_file\EN_PROD\TXT", '*.txt')):
#     os.rename(filename, filename[:-4] + '.rtf')

# %% fix task
task = pd.read_sql("""select id, content from activity where content like '%Subject: Call Back on%'""",connection) #284978
task['subject'] = task['content'].apply(lambda x: x.split('\n')[1])
tem1= task.loc[task['subject']!='']
tem2=task.loc[task['subject']=='']
tem2['subject'] = tem2['content'].apply(lambda x: x.split('\n\n')[1])
task1 = pd.concat([tem1, tem2])
task1['subject'] = task1['subject'].apply(lambda x: x.replace('\n',''))
task1['subject'] = task1['subject'].apply(lambda x: x.replace('Subject: ',''))
task1.to_csv('task1.csv')
tem = pd.read_csv('task1.csv')
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, connection, ['subject' ], ['id', ], 'activity', mylog)