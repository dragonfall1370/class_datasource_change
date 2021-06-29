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
cf.read('at_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
candidate = pd.read_sql("""
select concat('AT',[CD Number]) as candidate_externalid
     , nullif(trim(Phone),'') as Phone
     , nullif(trim([Mobile Phone]),'') as mobile_phone
     , nullif(trim([Work Phone]),'') as work_phone
     , nullif(trim(Email),'') as work_email
     , nullif(trim([Birth Day]),'') as dob
     , nullif(trim([Region of work]),'') as region_of_work
     , nullif(trim([Residential Location]),'') as residential_location
     , nullif(trim([Temp or Perm]),'') as job_type
     , nullif(trim([Job Title]),'') as job_title
     , nullif(trim(Status),'') as status
     , nullif(trim([Current Employer]),'') as current_employer
     , nullif(trim(Gender),'') as gender
     , nullif(trim([Date Registered]),'') as reg_date
     , nullif(trim([Candidate Profile]),'') as candidate_profile
     , nullif(trim(Source),'') as source
     , nullif(trim([Languages Known               ]),'') as language
     , nullif(trim([linkedin URL]),'') as linkedin
     , nullif(trim(Salary),'') as salary
     , nullif(trim([Notice Period]),'') as notice_period
     , nullif(trim([Address One]),'') as address_line1
     , nullif(trim([Address Two]),'') as address_line2
     , nullif(trim(Region),'') as state
     , nullif(trim(City),'') as city
     , nullif(trim(Postcode),'') as post_code
     , nullif(trim(Country),'') as country
from ateca_cand
""", engine_mssql)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','address_line1', 'address_line2','city','state','post_code','country']]
c_location['address'] = c_location[['address_line1', 'address_line2','city','state','post_code','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['address']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid','address_line1', 'address_line2','city','state','post_code','country']].drop_duplicates()

tem = comaddr[['candidate_externalid', 'address_line1']].dropna()
tem['count'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['count']<100]
cp3 = vcand.update_address_line1(tem, mylog)

tem = comaddr[['candidate_externalid', 'address_line2']].dropna()
tem['count'] = tem['address_line2'].apply(lambda x: len(x))
tem = tem.loc[tem['count']<100]
cp3 = vcand.update_address_line2(tem, mylog)

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
from common import vincere_company
vcom = vincere_company.Company(connection)
tem = comaddr[['candidate_externalid','country']].dropna()
tem['country_code'] = tem.country.map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)

# %% home phones
home_phone = candidate[['candidate_externalid', 'Phone']].dropna()
home_phone['home_phone'] = home_phone['Phone']
home_phone = home_phone.loc[home_phone['home_phone']!='+']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'work_phone']].dropna()
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile/primary phones
mphone = candidate[['candidate_externalid', 'mobile_phone']].dropna()
mphone = mphone.loc[mphone['mobile_phone']!='+']
mphone['primary_phone'] = mphone['mobile_phone']
cp = vcand.update_mobile_phone(mphone, mylog)
cp = vcand.update_primary_phone(mphone, mylog)

# %% work email
wphone = candidate[['candidate_externalid', 'work_email']].dropna()
wphone = wphone.loc[wphone['work_email'].str.contains('@')]
cp = vcand.update_work_email(wphone, mylog)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = candidate[['candidate_externalid', 'TITLE']].dropna().drop_duplicates()
# tem['gender_title'] = tem['TITLE']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# tem2 = tem[['candidate_externalid','gender_title']].dropna().drop_duplicates()
# cp = vcand.update_gender_title(tem2, mylog)

# %% job type
tem = candidate[['candidate_externalid', 'job_type']].dropna().drop_duplicates()
tem.loc[(tem['job_type'] == 'Permanent'), 'desired_job_type'] = 'permanent'
tem.loc[(tem['job_type'] == 'Temp'), 'desired_job_type'] = 'temporary'
tem.loc[(tem['job_type'] == 'Contract'), 'desired_job_type'] = 'contract'
tem1 = tem[['candidate_externalid', 'desired_job_type']].dropna().drop_duplicates()
vcand.update_desired_job_type(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'dob']].dropna().drop_duplicates()
tem = tem.loc[tem['dob']!='0000-00-00']
tem['date_of_birth'] = pd.to_datetime(tem['dob'])
vcand.update_dob(tem, mylog)

# %% source
tem = candidate[['candidate_externalid', 'source']].dropna().drop_duplicates()
cp = vcand.insert_source(tem)

# %% reg date
tem = candidate[['candidate_externalid', 'reg_date']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['reg_date'])
vcand.update_reg_date(tem, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'current_employer', 'job_title']]
cur_emp['current_job_title'] = cur_emp['job_title'].str.strip()
vcand.update_candidate_current_employer_v3(cur_emp, dest_db, mylog)

# %% linkedin
tem = candidate[['candidate_externalid', 'linkedin']].dropna().drop_duplicates()
# tem['linkedin'] = tem['LINKEDIN URL']
vcand.update_linkedin(tem, mylog)

# %% salary
# tem = candidate[['candidate_externalid', 'notice_period']].dropna().drop_duplicates()
# tem['current_salary'] = tem['salary']
# vcand.update_current_salary(tem, mylog)

# %% gnder
tem = candidate[['candidate_externalid', 'gender']].dropna().drop_duplicates()
tem['gender'].unique()
tem.loc[(tem['gender'] == 'Female'), 'male'] = 0
tem.loc[(tem['gender'] == 'Male'), 'male'] = 1
vcand.update_gender(tem, mylog)

# %% note
note = candidate[['candidate_externalid', 'region_of_work', 'residential_location','candidate_profile','salary','notice_period']]
note['note'] = note[['region_of_work', 'residential_location','candidate_profile','salary','notice_period']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['region of work', 'residential location','candidate profile','salary','notice_period'], x) if e[1]]), axis=1)
cp7 = vcand.update_note2(note, dest_db, mylog)

# %% status
tem = candidate[['candidate_externalid', 'status']].dropna().drop_duplicates()
tem['name'] = tem['status']
tem1 = tem[['name']].drop_duplicates()
tem1['owner'] = ''
vcand.create_status_list(tem1, mylog)
vcand.add_candidate_status(tem, mylog)

# %% languages
cand_languages = candidate[['candidate_externalid', 'language']].dropna().drop_duplicates()
languages = cand_languages.language.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand_languages[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='language_v') \
    .drop('variable', axis='columns') \
    .dropna()
# languages['language_v'] = languages['language_v'].str.lower()
languages['language'] = languages['language_v']
# idlanguage = pd.read_sql("""
# select idlanguage, value from language
# """, engine_postgre_review)
#
# language = languages.merge(idlanguage, left_on='idlanguage_string', right_on='idlanguage', how='left')
# language['language'] = language['value'].apply(lambda x: x.split(' ')[0])
# language['language'].unique()
languages['level'] = ''

df = languages
logger = mylog

tem2 = df[['candidate_externalid', 'language', 'level']]
tem2.loc[tem2['language']=='Africaans', 'language'] = 'Afrikaans'
tem2.loc[tem2['language']=='Chinese', 'language'] = 'Chinese (Mandarin/Putonghua)'
tem2.loc[tem2['language']=='Malaysian', 'language'] = 'Malay'

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
tem2 = tem2.merge(pd.read_sql("select code, system_name as language from language", vcand.ddbconn), on='language', how='left') \
    .rename(columns={'code': 'languageCode'})
# tem2.loc[tem2['languageCode'].isnull()]
tem2 = tem2.fillna('')
tem2.languageCode = tem2.languageCode.map(lambda x: '"languageCode":"%s"' % x)
tem2.level = tem2.level.map(lambda x: '"level":"%s"' % x)
tem2['skill_details_json'] = tem2[['languageCode', 'level']].apply(lambda x: '{%s}' % (','.join(x)), axis=1)
tem2 = tem2.groupby('candidate_externalid')['skill_details_json'].apply(','.join).reset_index()
tem2.skill_details_json = tem2.skill_details_json.map(lambda x: '[%s]' % x)
# [{"languageCode":"km","level":""},{"languageCode":"my","level":""}]
tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid from candidate", vcand.ddbconn), on=['candidate_externalid'])
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcand.ddbconn, ['skill_details_json', ], ['id', ], 'candidate', logger)