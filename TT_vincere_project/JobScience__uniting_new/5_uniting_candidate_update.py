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
cand_new = pd.read_csv(os.path.join(standard_file_upload, 'new_candidate.csv'))
cand_new_skip = pd.read_csv(os.path.join(standard_file_upload, 'skip_file_cand.csv'))
cand_new_add = pd.read_csv(os.path.join(standard_file_upload, 'cand_to_add_new.csv'))

candidate = pd.read_sql("""
select
       c.Id as candidate_externalid,
       c.Phone,
       c.MobilePhone,
       c.ts2__Source__c,
       c.Grade__c,
       c.Skills__c,
       c.Candidate_Type__c,
       c.ts2__EmployerOrgName_1__c,
       c.ts2__Current_Salary__c,
       c.ts2__Desired_Salary__c,
       c.Main_Skills__c,
       c.Technology__c,
       c.ts2__EmployerOrgName_2__c,
       c.ts2__EmployerOrgName_3__c,
       c.Current_JobTitle__c,
       c.ts2__Notice__c,
       c.List_of_current_benefits__c,
       c.Current_Benefits__c,
       c.Driving_License__c,
       c.Desired_Commutable_Distance__c,
       c.Can_Drive__c,
       c.UK_Resident__c,
       c.Date_available__c,
       c.ts2__Resume_Last_Updated__c,
       c.MailingStreet,
       c.MailingState,
       c.MailingCity,
       c.MailingPostalCode,
       c.MailingCountry
from Cand_new c
left join User_new u on c.OwnerId = u.Id
""", engine_sqlite)
cand_new = cand_new.loc[~cand_new['candidate-externalId'].isin(cand_new_skip['candidate-externalId'])]
cand = pd.concat([cand_new, cand_new_add])

candidate = candidate.loc[candidate['candidate_externalid'].isin(cand['candidate-externalId'])]
candidate = candidate.drop_duplicates()
assert False

# %% primary phone
tem = candidate[['candidate_externalid', 'Phone', 'MobilePhone']]
tem['primary_phone'] = tem[['Phone', 'MobilePhone']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
vcand.update_primary_phone(tem, mylog)

# %% mobile phone
tem = candidate[['candidate_externalid', 'MobilePhone']].rename(columns={'MobilePhone': 'mobile_phone'}).dropna()
vcand.update_mobile_phone(tem, mylog)

# %% source
tem = candidate[['candidate_externalid', 'source']].dropna()
vcand.insert_source(tem)

# %% education summary
edu = candidate[[
    'candidate_externalid',
    'Grade__c']]


edu = edu.loc[edu['Grade__c'].notnull()]
edu['education_summary'] = 'Grade: ' + edu['Grade__c']
vcand.update_education_summary(edu, mylog)

# %% skills
tem = candidate[['candidate_externalid', 'Skills__c', 'Main_Skills__c', 'Technology__c']]
tem['skills'] = tem[['Skills__c', 'Main_Skills__c', 'Technology__c']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['skills'] != '']
vcand.update_skills2(tem, dest_db, mylog)

# %% jobtype
tem = candidate[['candidate_externalid', 'Candidate_Type__c']].dropna()
tem.Candidate_Type__c.unique()
tem.loc[tem['Candidate_Type__c']=='Permanent', 'desired_job_type'] = 'permanent'
tem.loc[tem['Candidate_Type__c']=='Temp', 'desired_job_type'] = 'contract'
tem.loc[tem['Candidate_Type__c']=='Contract/Interim', 'desired_job_type'] = 'contract'
tem.loc[tem['Candidate_Type__c']=='Contract', 'desired_job_type'] = 'permanent'
tem.loc[tem['Candidate_Type__c']=='Both', 'desired_job_type'] = 'permanent'
tem.loc[tem['Candidate_Type__c']=='Any', 'desired_job_type'] = 'permanent'
tem = tem[['candidate_externalid', 'desired_job_type']].dropna()
vcand.update_desired_job_type(tem, mylog)

# %% current jobtitle
tem = candidate[['candidate_externalid', 'ts2__EmployerOrgName_1__c', 'Current_JobTitle__c']].rename(columns={'Current_JobTitle__c': 'current_job_title', 'ts2__EmployerOrgName_1__c': 'current_employer'})
tem['current_job_title'] = tem['current_job_title'].apply(lambda x: x.replace('\\','/') if x else x)
tem['current_employer'] = tem['current_employer'].apply(lambda x: x.replace('\\','/') if x else x)
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
vcand.update_candidate_current_employer2(cur_emp_2, dest_db, mylog)

# %% 3nd employer
cur_emp_3 = candidate[['candidate_externalid', 'ts2__EmployerOrgName_3__c']].dropna().rename(columns={'ts2__EmployerOrgName_3__c': 'current_employer'})
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\u200B','') if x else x)
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\u00AD','') if x else x)
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\uF042','') if x else x)
cur_emp_3['current_employer'] = cur_emp_3['current_employer'].apply(lambda x: x.replace('\\','/') if x else x)
vcand.update_candidate_current_employer2(cur_emp_3, dest_db, mylog)

# %% desired annual salary
tem = candidate[['candidate_externalid', 'ts2__Desired_Salary__c']].dropna().rename(columns={'ts2__Desired_Salary__c': 'desire_salary'})
tem['desire_salary'] = tem['desire_salary'].astype(float)
vcand.update_desire_salary(tem, mylog)

# %% current annual salary
tem = candidate[['candidate_externalid', 'ts2__Current_Salary__c']].dropna().rename(columns={'ts2__Current_Salary__c': 'current_salary'})
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% notice
tem = candidate[['candidate_externalid', 'ts2__Notice__c']].dropna().rename(columns={'ts2__Notice__c': 'notice_period'})
tem = tem.loc[tem['notice_period'] != 'None']
tem['notice_period'] = tem['notice_period'].apply(lambda x: x.split(' ')[0])
tem = tem.loc[tem['notice_period'] != 'Other']
tem['notice_period'] = tem['notice_period'].astype(int)
tem['notice_period'].unique()
vcand.update_notice_period(tem, mylog)

# %% list benefits
tem = candidate[['candidate_externalid', 'List_of_current_benefits__c', 'Current_Benefits__c']]
tem['other_benefits'] = tem[['List_of_current_benefits__c', 'Current_Benefits__c']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['other_benefits'] != '']
vcand.update_other_benefits(tem, mylog)

# %% note
note = candidate[[
    'candidate_externalid'
    , 'Driving_License__c'
    , 'Desired_Commutable_Distance__c'
    , 'Can_Drive__c'
    , 'UK_Resident__c'
    , 'Date_available__c'
    , 'ts2__Resume_Last_Updated__c'
    ]]

prefixs = [
'UA ID'
, 'Driving License'
, 'Desired Commutable Distance'
, 'Can Drive'
, 'UK Resident'
, 'Date available'
, 'Resume Last Updated'
]

note['note'] = note.apply(lambda x: '\n'.join([': '.join(str(e1) for e1 in e) for e in zip(prefixs, x) if e[1]]), axis='columns')
cp11 = vcand.update_note(note, mylog)

# %% location name/address
candidate['location_name'] = candidate[['MailingStreet', 'MailingState', 'MailingCity', 'MailingPostalCode', 'MailingCountry']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate['address'] = candidate.location_name
cp2 = vcand.insert_common_location_v2(candidate, dest_db, mylog)

# update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
comaddr = candidate[['candidate_externalid', 'MailingStreet', 'MailingState', 'MailingCity', 'MailingPostalCode', 'MailingCountry', 'address']].drop_duplicates()\
    .rename(columns={'MailingCity': 'city', 'MailingState': 'state', 'MailingPostalCode': 'post_code'})

cp3 = vcand.update_location_city2(comaddr, dest_db, mylog)
# %% update state
cp4 = vcand.update_location_state(comaddr, mylog)
# update postcode
cp5 = vcand.update_location_post_code(comaddr, mylog)
#  update country
comaddr['country_code'] = comaddr.MailingCountry.map(vcand.get_country_code)
cp6 = vcand.update_location_country_code(comaddr, mylog)
vcand.set_all_current_candidate_address_as_mailling_address()
