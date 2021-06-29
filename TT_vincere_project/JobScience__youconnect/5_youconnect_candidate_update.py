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
cf.read('yc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')

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
candidate = pd.read_sql("""
select
       c.Id as candidate_externalid,
       c.Phone,
       c.MobilePhone,
       c.Salutation,
       c.ts2__Date_Available__c,
       c.Birthdate,
       c.Industry_Experience__c,
       c.ts2__EmployerOrgName_1__c,
       c.YC_Comments__c,
       c.YC_Executive_Summary__c,
       c.ts2__EmployerOrgName_2__c,
       c.ts2__EmployerOrgName_3__c,
       c.MailingStreet,
       c.MailingState,
       c.MailingCity,
       c.MailingPostalCode,
       c.MailingCountry,
       c.Email,
       c.E_mail_2__c,
       c.Native_Language__c,
       c.Working_Proficiency_Languages__c,
       c.ts2__Job_Type__c, c.CreatedDate
from Contact c
""", engine_sqlite)
assert False
# %% job type
jobtp = candidate[['candidate_externalid', 'ts2__Job_Type__c']].dropna()
jobtp = jobtp.ts2__Job_Type__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(jobtp[['candidate_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['candidate_externalid'], value_name='job_type') \
   .drop('variable', axis='columns') \
   .dropna()

jobtp.loc[jobtp['job_type']=='Perm', 'desired_job_type'] = 'permanent'
jobtp.loc[jobtp['job_type']=='Temp', 'desired_job_type'] = 'contract'
jobtype = jobtp[['candidate_externalid', 'desired_job_type']].dropna()
jobtype['desired_job_type'].unique()
cp = vcand.update_desired_job_type_2(jobtype, mylog)

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
# update state
cp4 = vcand.update_location_state2(comaddr, dest_db, mylog)
# update postcode
cp5 = vcand.update_location_post_code2(comaddr, dest_db, mylog)
#  update country
comaddr['country_code'] = comaddr.MailingCountry.map(vcand.get_country_code)
cp6 = vcand.update_location_country_code2(comaddr, dest_db, mylog)
vcand.set_all_current_candidate_address_as_mailling_address()

# %% phones
indt = candidate[['candidate_externalid', 'Phone']].dropna()
indt['home_phone'] = indt['Phone']
cp = vcand.update_home_phone2(indt, dest_db, mylog)
indt = candidate[['candidate_externalid', 'MobilePhone']].dropna()
indt['primary_phone'] = indt['MobilePhone']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)

# %% emails
indt = candidate[['candidate_externalid', 'E_mail_2__c']].dropna()
indt['work_email'] = indt['E_mail_2__c']
cp = vcand.update_work_email(indt, mylog)
# indt['primary_email'] = indt[['Email', 'E_mail_2__c']].apply(lambda x: set(e for e in x if e), axis=1)
# indt['primary_email'] = indt['primary_email'].apply(lambda x: ', '.join(x))
# indt = indt.loc[indt['primary_email']!='']
# indt.loc[indt['E_mail_2__c'].notnull()]
# cp = vcand.update_primary_email(indt, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'ts2__EmployerOrgName_1__c']].dropna()
cur_emp['current_employer'] = cur_emp['ts2__EmployerOrgName_1__c']
vcand.update_candidate_current_employer_v2(cur_emp, dest_db, mylog)

# %% 2nd employer
cur_emp = candidate[['candidate_externalid', 'ts2__EmployerOrgName_2__c']].dropna()
cur_emp['current_employer'] = cur_emp['ts2__EmployerOrgName_2__c']
vcand.update_candidate_current_employer2(cur_emp, dest_db, mylog)

# %% 3rd employer
cur_emp = candidate[['candidate_externalid', 'ts2__EmployerOrgName_3__c']].dropna()
cur_emp['current_employer'] = cur_emp['ts2__EmployerOrgName_3__c']
vcand.update_candidate_current_employer2(cur_emp, dest_db, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'Salutation']].dropna().drop_duplicates()
tem['gender_title'] = tem['Salutation']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% note
note = candidate[[
    'candidate_externalid'
    , 'YC_Comments__c'
    , 'YC_Executive_Summary__c'
    , 'ts2__Date_Available__c'
    ]]

note['ts2__Date_Available__c'] = pd.to_datetime(note['ts2__Date_Available__c'], errors='coerce')
note['ts2__Date_Available__c'] = note['ts2__Date_Available__c'].apply(lambda x: datetime.datetime.strftime(x, '%d/%m/%Y') if str(x) != 'NaT' else None)

prefixs = [
'YC ID'
, 'YC Comments'
, 'YC Executive Summary'
, 'Date Available'
]

note['note'] = note.apply(lambda x: '\n'.join([': '.join(str(e1) for e1 in e) for e in zip(prefixs, x) if e[1]]), axis='columns')
cp11 = vcand.update_note2(note, dest_db, mylog)


# %% dob
dob = candidate[['candidate_externalid', 'Birthdate']].dropna()
dob['date_of_birth'] = dob['Birthdate']
dob['date_of_birth'] = pd.to_datetime(dob['date_of_birth'], errors='coerce')
# dob['date_of_birth'] = dob['date_of_birth'].apply(lambda x: datetime.datetime.strftime(x, '%d/%m/%Y') if str(x) != 'NaT' else None)
vcand.update_date_of_birth(dob, mylog)

# %% availability_start
availability_date = candidate[['candidate_externalid', 'ts2__Date_Available__c']].dropna()
availability_date['availability_start'] = availability_date['ts2__Date_Available__c']
availability_date['availability_start'] = pd.to_datetime(availability_date['availability_start'], errors='coerce')
vcand.update_availability_start(availability_date, mylog)

# %% industry
industries = pd.read_csv('industries.csv')

industry = candidate[['candidate_externalid', 'Industry_Experience__c']].dropna()
industry = industry.Industry_Experience__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(industry[['candidate_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['candidate_externalid'], value_name='name') \
   .drop('variable', axis='columns') \
   .dropna()

industry['name'] = industry['name'].apply(lambda x: 'Utility Services' if x == 'Utility_Services' else x)
industry['matcher'] = industry['name']
industry = industry.merge(industries, left_on='matcher', right_on='Industry Experience Candidates Jobscience')
tem = industry[['candidate_externalid', 'Industries Vincere']].dropna()
tem['name'] = tem['Industries Vincere']
tem = tem.drop_duplicates()
cp8 = vcand.insert_candidate_industry(tem, mylog)

# %% language
first_language = candidate[['candidate_externalid', 'Native_Language__c']].dropna()
first_language = first_language.Native_Language__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(first_language[['candidate_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['candidate_externalid'], value_name='language') \
   .drop('variable', axis='columns') \
   .dropna()
first_language['level'] = 'native'
first_language['matcher'] = first_language['candidate_externalid']+first_language['language']

other_language = candidate[['candidate_externalid', 'Working_Proficiency_Languages__c']].dropna()
other_language = other_language.Working_Proficiency_Languages__c.map(lambda x: x.split(';')) \
   .apply(pd.Series) \
   .merge(other_language[['candidate_externalid']], left_index=True, right_index=True) \
   .melt(id_vars=['candidate_externalid'], value_name='language') \
   .drop('variable', axis='columns') \
   .dropna()
other_language['matcher'] = other_language['candidate_externalid']+other_language['language']
other_language = other_language.loc[~other_language['matcher'].isin(first_language['matcher'])]
other_language['level'] = 'fluent'


language = pd.concat([first_language, other_language])
# df = language.loc[language['candidate_externalid'] == '0030N000023iEtyQAE']

df = language
tem2 = df[['candidate_externalid', 'language', 'level']]

try:
    tem2.loc[tem2.level.str.lower().isin(['native']), 'level'] = 5  # native
except:
    pass

try:
    tem2.loc[tem2.level.str.lower().isin(['excellent', 'fluent']), 'level'] = 4  # fluent
except:
    pass

try:
    tem2.loc[tem2.level.str.lower().isin(['advanced', ]), 'level'] = 3  # advanced
except:
    pass

try:
    tem2.loc[tem2.level.str.lower().isin(['intermediate', ]), 'level'] = 2  # intermediate
except:
    pass

try:
    tem2.loc[tem2.level.str.lower().isin(['beginner', 'good', 'basic']), 'level'] = 1  # intermediate
except:
    pass
tem2.level.unique()
from common import vincere_company
vcom = vincere_company.Company(connection)
tem2['languageCode'] = tem2['language'].map(lambda x: vcom.get_country_code(x.lower()))
tem2 = tem2.fillna('')
tem2.languageCode = tem2.languageCode.map(lambda x: '"languageCode":"%s"' % x.lower())
tem2.level = tem2.level.map(lambda x: '"level":"%s"' % x)
tem2['skill_details_json'] = tem2[['languageCode', 'level']].apply(lambda x: '{%s}' % (','.join(x)), axis=1)
tem2 = tem2.groupby('candidate_externalid')['skill_details_json'].apply(','.join).reset_index()
tem2.skill_details_json = tem2.skill_details_json.map(lambda x: '[%s]' % x)
assert False
# [{"languageCode":"km","level":""},{"languageCode":"my","level":""}]
tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid from candidate", vcand.ddbconn), on=['candidate_externalid'])
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcand.ddbconn, ['skill_details_json', ], ['id', ], 'candidate', mylog)

# vcand.update_skill_languages(df, mylog)

# -----------------------------------

# %% dob
tem = candidate[['candidate_externalid', 'YC_Executive_Summary__c']].dropna()
tem2 = tem.merge(vcand.candidate, on=['candidate_externalid'])

# %% reg date
tem = candidate[['candidate_externalid', 'CreatedDate']].dropna()
tem['reg_date'] = pd.to_datetime(tem['CreatedDate'])
vcand.update_reg_date(tem, mylog)
