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
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

def split_dataframe_to_chunks(df, n):
   df_len = len(df)
   count = 0
   dfs = []
   while True:
      if count > df_len - 1:
         break

      start = count
      count += n
      # print("%s : %s" % (start, count))
      dfs.append(df.iloc[start: count])
   return dfs

# %% contact
sql1 = """
SELECT
j.Id,
j.Name,
u.Email AS owner,
j.CreatedDate,
j.ts2__Account__c AS companyId,
cont.contactid AS contactId
FROM ts2__Job__c j
LEFT JOIN (
    SELECT
    c.Id as contactid,
    c.AccountId as companyid
    FROM Contacts c
    JOIN RecordType r ON (c.RecordTypeId || 'IAQ') = r.Id
    WHERE c.IsDeleted = 0 AND r.Name = 'Contact') cont on (j.ts2__Account__c = cont.companyid and j.ts2__Contact__c = cont.contactid)
LEFT JOIN USER u ON j.OwnerId = u.Id
WHERE j.IsDeleted = 0
order by j.LastModifiedDate desc;
"""
job1 = pd.read_sql(sql1, engine_sqlite)

# %% job
sql2 = """
SELECT
j.Id,
j.Name,
u.Email AS owner,
j.CreatedDate,
cont.companyid AS companyId,
cont.contactid AS contactId
FROM ts2__Job__c j
LEFT JOIN (
    SELECT
    c.Id as contactid,
    c.AccountId as companyid
    FROM Contacts c
    JOIN RecordType r ON (c.RecordTypeId || 'IAQ') = r.Id
    WHERE c.IsDeleted = 0 AND r.Name = 'Contact') cont on (j.ts2__Account__c = cont.companyid and j.ts2__Contact__c = cont.contactid)
LEFT JOIN USER u ON j.OwnerId = u.Id
WHERE j.IsDeleted = 0
order by j.LastModifiedDate desc;
"""
job2 = pd.read_sql(sql2, engine_sqlite)

job_diff = pd.concat([job1, job2]).drop_duplicates(keep=False)
job_diff = job_diff.loc[job_diff['companyId'].notnull()]

job_diff.rename(columns={
    'Id': 'position-externalId',
    'Name': 'position-title',
    'owner': 'position-owners',
    'contactId': 'position-contactId',
    'companyId': 'position-companyId',
}, inplace=True)

job, default_contacts = vincere_standard_migration.process_vincere_job_2(job_diff, mylog)
job.to_csv(os.path.join(standard_file_upload, '5_job_real.csv'), index=False)
default_contacts['contact-email'] = 'new_' + default_contacts['contact-email']
if len(default_contacts):
    default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts_more.csv'), index=False)