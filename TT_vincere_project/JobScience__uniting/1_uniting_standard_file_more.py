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

# # %% company
# sql = """
# SELECT
# 	  a.Id,
#       a.Name,
#       u.Email as owner
# FROM Account a
# LEFT JOIN User u ON a.OwnerId = u.Id
# WHERE a.IsDeleted = 0;
# """
# company = pd.read_sql(sql, engine_sqlite)
# company.rename(columns={
#     'Id': 'company-externalId',
#     'Name': 'company-name',
#     'owner': 'company-owners',
# }, inplace=True)
# company['company-name'] = company['company-name'].apply(lambda x: x if x else 'DEFAULT NAME')
# company = vincere_standard_migration.process_vincere_comp(company, mylog)
# company.loc[company['company-externalId']=='001D0000020bh5nIAA']
#
# # %% contact
# sql = """
# select
#        c.Id,
#        c.FirstName,
#        c.LastName,
#        c.Email,
#        com.Id as companyid,
#        u.Email as owner
#        --c.RecordTypeId
# from Contacts c
# join RecordType r on (c.RecordTypeId || 'IAQ') = r.Id
# left join Account com on (c.AccountId = com.Id and com.IsDeleted=0)
# left join User u on c.OwnerId = u.Id
# where c.IsDeleted = 0
# and r.Name = 'Contact'
# order by c.LastModifiedDate desc
# """
# contact = pd.read_sql(sql, engine_sqlite)
# contact.rename(columns={
#     'Id': 'contact-externalId',
#     'FirstName': 'contact-firstName',
#     'LastName': 'contact-lastName',
#     'Email': 'contact-email',
#     'companyid': 'contact-companyId',
#     'owner': 'contact-owners',
# }, inplace=True)
# contact, default_company = vincere_standard_migration.process_vincere_contact_2(contact)
#
# %% job
# close date ts_date_filled
# sql = """
# SELECT
# j.Id,
# j.Name,
# u.Email AS owner,
# j.CreatedDate,
# cont.companyid AS companyId,
# cont.contactid AS contactId
# FROM ts2__Job__c j
# LEFT JOIN (
#     SELECT
#     c.Id as contactid,
#     c.AccountId as companyid
#     FROM Contacts c
#     JOIN RecordType r ON (c.RecordTypeId || 'IAQ') = r.Id
#     WHERE c.IsDeleted = 0 AND r.Name = 'Contact') cont on (j.ts2__Account__c = cont.companyid and j.ts2__Contact__c = cont.contactid)
# LEFT JOIN USER u ON j.OwnerId = u.Id
# WHERE j.IsDeleted = 0
# order by j.LastModifiedDate desc;
# """
# job = pd.read_sql(sql, engine_sqlite)
# job.rename(columns={
#     'Id': 'position-externalId',
#     'Name': 'position-title',
#     'owner': 'position-owners',
#     'contactId': 'position-contactId',
#     'companyId': 'position-companyId',
# }, inplace=True)
# job, default_contacts = vincere_standard_migration.process_vincere_job_2(job, mylog)

# %% candidate
sql = """
select 
       c.Id,
       c.FirstName,
       c.LastName,
       c.Email,
       com.Id as companyid,
       u.Email as owner
       --c.RecordTypeId
from Contacts c
left join Account com on (c.AccountId = com.Id and com.IsDeleted=0)
left join User u on c.OwnerId = u.Id
where c.IsDeleted = 0
and c.Id like '%003D000002Ej9JPIAZ%'
"""
candidate = pd.read_sql(sql, engine_sqlite)
candidate.rename(columns={
    'Id': 'candidate-externalId',
    'FirstName': 'candidate-firstName',
    'LastName': 'candidate-lastName',
    'Email': 'candidate-email',
    'owner': 'candidate-owners',
}, inplace=True)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% csv
# default_company['company-name'] = 'DEFAULT COMPANY'
# company = pd.concat([default_company, company], sort=True)
# company.to_csv(os.path.join(standard_file_upload, '1_company.csv'), index=False)
#
# default_contacts['contact-firstName'] = 'DEFAULT'
# default_contacts['contact-lastName'] = 'CONTACT'
# # default_contacts['rn'] = default_contacts.groupby('contact-externalId').cumcount()
# default_contacts['rn'] = default_contacts.reset_index().index
# default_contacts['rn'] = default_contacts['rn'].astype(str)
# default_contacts['contact-email'] = 'default_email_' + default_contacts['rn'] + '@vincere.io'
# default_contacts.drop('rn', axis=1, inplace=True)
# contact = pd.concat([default_contacts, contact], sort=True)
# contact.to_csv(os.path.join(standard_file_upload, '2_contact.csv'), index=False)
#
# job.to_csv(os.path.join(standard_file_upload, '3_job.csv'), index=False)
#
# candidate.to_csv(os.path.join(standard_file_upload, '4_candidate.csv'), index=False)


candidate.to_csv(os.path.join(standard_file_upload, 'new_candidate_more.csv'), index=False)
# split_df_to_chunks = split_dataframe_to_chunks(candidate, 50000)
# for idx, val in enumerate(split_df_to_chunks):
#     val.to_csv(os.path.join(standard_file_upload, '6_cand'+'_'+str(idx)+'.csv'), index=False)

# job.to_csv(os.path.join(standard_file_upload, '5_job.csv'), index=False)
# contact.to_csv(os.path.join(standard_file_upload, '4_contact.csv'), index=False)
# company.to_csv(os.path.join(standard_file_upload, '2_company.csv'), index=False)
# if len(default_contacts):
#     default_contacts.to_csv(os.path.join(standard_file_upload, '3_default_contacts.csv'), index=False)
#
# tem = default_contacts.loc[default_contacts['contact-companyId'] == 'DEFAULT_COMPANY']
# if len(default_company):
#     default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
# elif len(tem):
#     default_company = pd.DataFrame({'company-externalId': ['DEFAULT_COMPANY'], 'company-name': ['DEFAULT COMPANY']})
#     default_company.to_csv(os.path.join(standard_file_upload, '1_default_company.csv'), index=False)
