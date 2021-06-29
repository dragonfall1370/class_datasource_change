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
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %% candidate
candidate = pd.read_sql("""
select 
       c.Id as candidate_externalid,
       c.ts2__EmployerOrgName_1__c,
       c.ts2__EmployerOrgName_2__c,
       c.ts2__EmployerOrgName_3__c,
       c.Current_JobTitle__c,
       c.MailingStreet,
       c.MailingState,
       c.MailingCity,
       c.MailingPostalCode,
       c.MailingCountry, c.ts2__Current_Salary__c, c.CreatedDate
from Contacts c
""", engine_sqlite)

cand = pd.read_sql("""
select external_id as candidate_externalid from candidate where external_id is not null and deleted_timestamp is null
""", engine_postgre)

candidate = candidate.merge(cand, on='candidate_externalid')
# assert False

# %% current jobtitle
tem = candidate[['candidate_externalid', 'ts2__EmployerOrgName_1__c', 'Current_JobTitle__c']].rename(columns={'Current_JobTitle__c': 'current_job_title', 'ts2__EmployerOrgName_1__c': 'current_employer'})
tem['current_job_title'] = tem['current_job_title'].apply(lambda x: x.replace('\\','/') if x else x)
tem['current_employer'] = tem['current_employer'].apply(lambda x: x.replace('\\','/') if x else x)
# tem = tem.fillna('')
# tem = tem.where(tem.notnull(), None)
# tem.loc[tem.current_employer.str.contains(r'\u200B',na=False)]
# tem.loc[tem.current_job_title.str.contains(r'\u200B',na=False)]


# tem = tem.drop(164499)
tem['current_employer'] = tem['current_employer'].apply(lambda x: x.replace('\u200B','') if x else x)
tem['current_job_title'] = tem['current_job_title'].apply(lambda x: x.replace('\u200B','') if x else x)
tem['current_job_title'] = tem['current_job_title'].apply(lambda x: x.replace('\u00AD','') if x else x)
tem['current_employer'] = tem['current_employer'].apply(lambda x: x.replace('\u00AD','') if x else x)
tem['current_job_title'] = tem['current_job_title'].apply(lambda x: x.replace('\uF042','') if x else x)
tem['current_employer'] = tem['current_employer'].apply(lambda x: x.replace('\uF042','') if x else x)

vcand.update_candidate_current_employer_title_v2(tem, dest_db, mylog)

# %% 2nd employer
cur_emp_2 = candidate[['candidate_externalid', 'ts2__EmployerOrgName_2__c']].dropna().rename(columns={'ts2__EmployerOrgName_2__c': 'current_employer'})
cur_emp_2['current_employer'] = cur_emp_2['current_employer'].apply(lambda x: x.replace('\u200B','') if x else x)
cur_emp_2['current_employer'] = cur_emp_2['current_employer'].apply(lambda x: x.replace('\u00AD','') if x else x)
cur_emp_2['current_employer'] = cur_emp_2['current_employer'].apply(lambda x: x.replace('\uF042','') if x else x)
cur_emp_2['current_employer'] = cur_emp_2['current_employer'].apply(lambda x: x.replace('\\','/') if x else x)
a = cur_emp_2[100000:]
# a = tem[200000:]
# b = a[4700:4701]
# b.to_csv('error.csv')
vcand.update_candidate_current_employer2(a, dest_db, mylog)
# %% 3nd employer
cur_emp_3 = candidate[['candidate_externalid', 'ts2__EmployerOrgName_3__c']].dropna().rename(columns={'ts2__EmployerOrgName_3__c': 'current_employer'})
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\u200B','') if x else x)
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\u00AD','') if x else x)
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\uF042','') if x else x)
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\\','/') if x else x)
vcand.update_candidate_current_employer2(cur_emp_3, dest_db, mylog)

# %% location name/address
# candidate['location_name'] = candidate[['MailingStreet', 'MailingState', 'MailingCity', 'MailingPostalCode', 'MailingCountry']] \
#    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# candidate['address'] = candidate.location_name
# cp2 = vcand.insert_common_location_v2(candidate, dest_db, mylog)
#
# # update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
comaddr = candidate[['candidate_externalid', 'MailingStreet', 'MailingState', 'MailingCity', 'MailingPostalCode', 'MailingCountry']].drop_duplicates()\
    .rename(columns={'MailingCity': 'city', 'MailingState': 'state', 'MailingPostalCode': 'post_code'})
#
# cp3 = vcand.update_location_city2(comaddr, dest_db, mylog)
# %% update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state_2(tem, dest_db, mylog)
# update postcode
cp5 = vcand.update_location_post_code(comaddr, mylog)
#  update country
tem = comaddr[['candidate_externalid', 'MailingCountry']].dropna()
tem['country_code'] = tem.MailingCountry.map(vcand.get_country_code)
cp6 = vcand.update_location_country_code(tem, mylog)
vcand.set_all_current_candidate_address_as_mailling_address()

# %% current annual salary
tem = candidate[['candidate_externalid', 'ts2__Current_Salary__c']].dropna().rename(columns={'ts2__Current_Salary__c': 'current_salary'})
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% current annual salary
tem = candidate[['candidate_externalid', 'CreatedDate']].dropna().rename(columns={'CreatedDate': 'reg_date'})
tem['reg_date'] = pd.to_datetime(tem['reg_date'])
vcand.update_reg_date(tem, mylog)

# tbls = []
# for i in range(1, 786):
#     tbls.append('ztung_tem_candidate_'+str(i))
#
# logger = mylog
# for t in tbls:
#     for attemp in range(0, 10):
#         try:
#             logger.info("Dropping table %s" % t)
#             sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300,
#                                                       client_encoding='utf8', use_batch_mode=True)
#             connection = sdbconn_engine.raw_connection()
#             vincere_custom_migration.execute_sql_update('drop table {}'.format(t), connection)
#         except Exception as ex:
#             logger.warn(ex)
#             logger.warn("Will retry after 30 seconds")
#             time.sleep(30)
#             pass
#         else:
#             break
#     else:
#         logger.warn("================= Cannot remove temporary table: %s =================" % t)