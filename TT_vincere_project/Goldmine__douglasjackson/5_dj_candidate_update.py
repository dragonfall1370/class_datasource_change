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
cf.read('dj_config.ini')
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
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
candidate = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     , c1.CONTACT, c1.COMPANY
     , trim(LASTNAME) as LASTNAME
     , nullif(ADDRESS1,'') as ADDRESS1
     , nullif(ADDRESS2,'') as ADDRESS2
     , nullif(ADDRESS3,'') as ADDRESS3
     , nullif(CITY,'') as CITY
     , nullif(STATE,'') as STATE
     , nullif(ZIP,'') as ZIP
     , nullif(COUNTRY,'') as COUNTRY
     , nullif(SOURCE,'') as SOURCE
     , nullif(TITLE,'') as TITLE
     , nullif(DEAR,'') as DEAR
     , nullif(PHONE1,'') as PHONE1
     , nullif(PHONE2,'') as PHONE2
     , nullif(PHONE3,'') as PHONE3
     , nullif(UINTERIM,'') as UINTERIM
     , nullif(ULINKEDIN,'') as ULINKEDIN
     , nullif(UTWITTER,'') as UTWITTER
     , nullif(KEY1,'') as KEY1
     , nullif(KEY5,'') as relocate
     , CREATEON
     , nullif(URANGE,'') as salryrange
     , nullif(UDAYRATE,'') as dayrate
     , nullif(USERDEF02,'') as FTE
     , nullif(UHOME,'') as homeworking
     , nullif(UEMPLOYER1,'') as UEMPLOYER1
     , nullif(UEMPLOYER2,'') as UEMPLOYER2
     , nullif(UEMPLOYER3,'') as UEMPLOYER3
     , nullif(UCODE,'') as UCODE
     , nullif(ULOCATIONS,'') as langauages
     , nullif(USERDEF01,'') as division
from CONTACT1 c1
left join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
""", engine_mssql)
# candidate.loc[candidate['homeworking'].notnull()]
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'STATE', 'ZIP', 'COUNTRY']]
c_location['location_name'] = c_location[['CITY', 'STATE', 'ZIP', 'COUNTRY']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['address'] = c_location[['ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'STATE', 'ZIP', 'COUNTRY']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid', 'CITY', 'STATE', 'ZIP', 'COUNTRY']].drop_duplicates()\
    .rename(columns={'CITY': 'city', 'STATE': 'state', 'ZIP': 'post_code'})
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
tem = comaddr[['candidate_externalid','COUNTRY']].dropna()
tem['country_code'] = tem.COUNTRY.map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)

# %%
email = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid, CONTSUPREF from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where c2.CONTACT = 'E-mail Address'
and KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
""", engine_mssql)
email['rn'] = email.groupby('candidate_externalid').cumcount()
email = email.loc[email['rn']==0]
email['primary_email'] = email[['CONTSUPREF']]
tem = email[['candidate_externalid','primary_email']].dropna().drop_duplicates()
vcand.update_primary_email(tem, mylog)

# %%
web = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid, CONTSUPREF from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where c2.CONTACT = 'Web Site'
and KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
""", engine_mssql)
web['rn'] = web.groupby('candidate_externalid').cumcount()
web = web.loc[web['rn']==0]
web['website'] = web[['CONTSUPREF']]
tem = web[['candidate_externalid','website']].dropna().drop_duplicates()
vcand.update_website(tem, mylog)

# %% home phones
home_phone = candidate[['candidate_externalid', 'PHONE2', 'PHONE3']].drop_duplicates()
home_phone['home_phone'] = home_phone[['PHONE2','PHONE3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
home_phone = home_phone.loc[home_phone['home_phone']!='']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% mobile
# tem = candidate[['candidate_externalid', 'PHONE1']].dropna().drop_duplicates()
# tem['def'] = tem['PHONE1'].apply(lambda x: x[0:2])
# tem['def2'] = tem['PHONE1'].apply(lambda x: x[0:5])
# tem['def3'] = tem['PHONE1'].apply(lambda x: x[0:6])
# tem1 = tem.loc[tem['def']=='07']
# tem2 = tem.loc[tem['def2']=='+4407']
# tem3 = tem.loc[tem['def3']=='+44 07']
# tem1['mobile_phone'] = tem1['PHONE1']


tem_p = candidate[['candidate_externalid', 'PHONE1']].dropna().drop_duplicates()
tem_p['mobile_phone'] = tem_p['PHONE1']
tem_p['def'] = tem_p['mobile_phone'].apply(lambda x: x[0:2])
tem_p['def2'] = tem_p['mobile_phone'].apply(lambda x: x[0:5])
tem_p['def3'] = tem_p['mobile_phone'].apply(lambda x: x[0:6])
tem_p1 = tem_p.loc[tem_p['def']=='07']
tem_p2 = tem_p.loc[tem_p['def2']=='+4407']
tem_p3 = tem_p.loc[tem_p['def3']=='+44 07']
tem1 = tem_p1[['candidate_externalid','mobile_phone']]

tem_ph2 = candidate[['candidate_externalid', 'PHONE2']].dropna().drop_duplicates()
tem_ph2['mobile_phone'] = tem_ph2['PHONE2']
tem_ph2['def'] = tem_ph2['mobile_phone'].apply(lambda x: x[0:2])
tem_ph2['def2'] = tem_ph2['mobile_phone'].apply(lambda x: x[0:5])
tem_ph2['def3'] = tem_ph2['mobile_phone'].apply(lambda x: x[0:6])
tem_ph21 = tem_ph2.loc[tem_ph2['def']=='07']
tem_ph22 = tem_ph2.loc[tem_ph2['def2']=='+4407']
tem_ph23 = tem_ph2.loc[tem_ph2['def3']=='+44 07']
tem2 = tem_ph21[['candidate_externalid','mobile_phone']]
tem3 = tem_ph23[['candidate_externalid','mobile_phone']]

tem_ph3 = candidate[['candidate_externalid', 'PHONE3']].dropna().drop_duplicates()
tem_ph3['mobile_phone'] = tem_ph3['PHONE3']
tem_ph3['def'] = tem_ph3['mobile_phone'].apply(lambda x: x[0:2])
tem_ph3['def2'] = tem_ph3['mobile_phone'].apply(lambda x: x[0:5])
tem_ph3['def3'] = tem_ph3['mobile_phone'].apply(lambda x: x[0:6])
tem_ph31 = tem_ph3.loc[tem_ph3['def']=='07']
tem_ph32 = tem_ph3.loc[tem_ph3['def2']=='+4407']
tem_ph33 = tem_ph3.loc[tem_ph3['def3']=='+44 07']
tem4 = tem_ph31[['candidate_externalid','mobile_phone']]
tem = pd.concat([tem1, tem2, tem3, tem4])
tem = tem.drop_duplicates()
tem = tem.groupby('candidate_externalid')['mobile_phone'].apply(','.join).reset_index()
cp = vcand.update_mobile_phone(tem, mylog)

# %% primary phones
indt = candidate[['candidate_externalid', 'PHONE1']].dropna().drop_duplicates()
indt['primary_phone'] = indt['PHONE1']
cp = vcand.update_primary_phone(indt, mylog)

# %% job type
tem1 = candidate[['candidate_externalid', 'UINTERIM']].dropna().drop_duplicates()
tem1['UINTERIM'].unique()
tem1.loc[tem1['UINTERIM']=='Perm', 'desired_job_type'] = 'permanent'
tem1.loc[tem1['UINTERIM']=='perm', 'desired_job_type'] = 'permanent'
tem1.loc[tem1['UINTERIM']=='Interim', 'desired_job_type'] = 'contract'
tem1.loc[tem1['UINTERIM']=='interim', 'desired_job_type'] = 'contract'
tem  = tem1[['candidate_externalid', 'desired_job_type']].dropna().drop_duplicates()
cp = vcand.update_desired_job_type(tem, mylog)

# %% gender
# tem = pd.read_sql("""
# select concat('', Reference) as candidate_externalid
#      , Gender
# from Candidate_Report_1
# """, engine_mssql)
# tem = tem.drop_duplicates()
# tem['Gender'].unique()
# tem.loc[tem['Gender']=='Male', 'male'] = 1
# tem.loc[tem['Gender']=='Female', 'male'] = 0
# tem2 = tem[['candidate_externalid','male']].dropna()
# tem2['male'] = tem2['male'].astype(int)
# cp = vcand.update_gender(tem2, mylog)

# %% source
tem = candidate[['candidate_externalid', 'SOURCE']].dropna().drop_duplicates()
src = pd.read_csv('source.csv')
tem = tem.merge(src, left_on='SOURCE', right_on='GM value')
tem['source'] = tem['Vincere value']
cp = vcand.insert_source(tem)

# %% linkedin
tem = candidate[['candidate_externalid', 'ULINKEDIN']].dropna().drop_duplicates()
tem['linkedin'] = tem['ULINKEDIN']

tem = tem.loc[tem['linkedin']!='YES']
tem = tem.loc[tem['linkedin']!='Yes']
tem = tem.loc[tem['linkedin']!='yes']
tem = tem.loc[tem['ULINKEDIN']!='Vince']
tem = tem.loc[tem['ULINKEDIN']!='Nathan-Brewer']
# tem = tem.loc[tem['linkedin']!='Lexine Sentance']
# tem = tem.loc[tem['linkedin']!='Nicolas Zibell\tChief Commercia']
tem['linkedin'].unique()

tem1 = tem[~tem['linkedin'].str.contains('https://')]
tem2 = tem[tem['linkedin'].str.contains('https://')]
tem3 = tem1[~tem1['linkedin'].str.contains('http://')]
tem4 = tem1[tem1['linkedin'].str.contains('http://')]

tem3['linkedin'] = tem3['linkedin'].apply(lambda x: x.replace('ttps://',''))
tem3['linkedin'] = 'https://' + tem3['linkedin']
tem = pd.concat([tem2,tem3,tem4])

cp = vcand.update_linkedin(tem, mylog)

# %% twitter
tem = candidate[['candidate_externalid', 'UTWITTER']].dropna().drop_duplicates()
tem['twitter'] = tem['UTWITTER']
tem['twitter'] = tem['twitter'].apply(lambda x: x.split('/')[-1])
tem['twitter'] = tem['twitter'].apply(lambda x: x.replace('@',''))
tem['twitter'] = 'https://twitter.com/' + tem['twitter']
cp = vcand.update_twitter(tem, mylog)

# %% contract rate
tem = candidate[['candidate_externalid', 'dayrate']].dropna().drop_duplicates()
tem['contract_rate'] = tem['dayrate']
cp = vcand.update_contract_rate(tem, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'UEMPLOYER1','TITLE']].drop_duplicates()
cur_emp['current_employer'] = cur_emp['UEMPLOYER1'].str.strip()
cur_emp['current_job_title'] = cur_emp['TITLE'].str.strip()
cur_emp = cur_emp.fillna('')
cur_emp['current_employer'] = cur_emp['current_employer'].apply(lambda x: x.replace('\\','|'))
cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\\','|'))
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

# %% current employer 2
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
cur_emp = candidate[['candidate_externalid', 'UEMPLOYER2']].dropna().drop_duplicates()
cur_emp['current_employer'] = cur_emp['UEMPLOYER2'].str.strip()
cur_emp['current_job_title'] = ''
cur_emp['current_employer'] = cur_emp['current_employer'].apply(lambda x: x.replace('\\','|'))
vcand.update_candidate_current_employer2(cur_emp, dest_db, mylog)

# %% current employer 3
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
cur_emp = candidate[['candidate_externalid', 'UEMPLOYER3']].dropna().drop_duplicates()
cur_emp['current_employer'] = cur_emp['UEMPLOYER3'].str.strip()
cur_emp['current_job_title'] = ''
cur_emp['current_employer'] = cur_emp['current_employer'].apply(lambda x: x.replace('\\','|'))
vcand.update_candidate_current_employer2(cur_emp, dest_db, mylog)

# %% owner
candidate = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     ,  c1.KEY4 as NAME
from CONTACT1 c1
where KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
""", engine_mssql)
candidate = candidate.drop_duplicates().dropna()
candidate = candidate.loc[candidate['NAME']!='']
user = pd.read_csv('user.csv')
candidate = candidate.merge(user, on='NAME', how='left')
candidate['owner'] = candidate['email']

tem = candidate[['candidate_externalid', 'owner']].dropna()
owner = candidate[['candidate_externalid','owner']].dropna().drop_duplicates()
owner = owner.drop_duplicates()
owner['email'] = owner['owner']
owner = owner.loc[owner['email']!='']
tem2 = owner[['candidate_externalid', 'email']]
tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid, candidate_owner_json from candidate", vcand.ddbconn), on=['candidate_externalid'])
tem2.candidate_owner_json.fillna('', inplace=True)
tem2 = tem2.merge(pd.read_sql("select id as user_account_id, email from user_account", vcand.ddbconn), on='email')
tem2['candidate_owner_json'] = tem2.user_account_id.map(lambda x: '{"ownerId":"%s"}' % x)

tem2 = tem2.groupby('id').apply(lambda subdf: list(set(subdf.candidate_owner_json))).reset_index().rename(columns={0: 'candidate_owner_json'})
tem2.candidate_owner_json = tem2.candidate_owner_json.map(lambda x: '[%s]' % ', '.join(x))
tem2.candidate_owner_json = tem2.candidate_owner_json.astype(str).map(lambda x: x.replace("'", ''))
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['candidate_owner_json', ], ['id', ], 'candidate', mylog)

# %% note
note = candidate[['candidate_externalid','division','KEY1','relocate','salryrange','FTE','homeworking','UCODE','langauages']]
note = note.where(note.notnull(),None)
note['UCODE'] = note['UCODE'].apply(lambda x: str(x) if x else x)
note.info()
note['note'] = note[['division','KEY1','relocate','salryrange','FTE','homeworking','UCODE','langauages']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Division','Contact Type','Relocate','Slry Rnge','FTE','HOMEWORKING','Code','Langauages'], x) if e[1]]), axis=1)
cp7 = vcand.update_note2(note, dest_db, mylog)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = candidate[['candidate_externalid', 'title']].dropna().drop_duplicates()
# tem['gender_title'] = tem['title']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# vcand.update_gender_title(tem, mylog)

# %% reg date
tem = candidate[['candidate_externalid', 'CREATEON']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['CREATEON'])
vcand.update_reg_date(tem, mylog)

# %% annual salary
tem = candidate[['candidate_externalid','salryrange']].dropna().drop_duplicates()
tem['salryrange'] = tem['salryrange'].apply(lambda x: x.replace(',','') if x else x)
tem['numeric'] = tem['salryrange'].apply(lambda x: x.isnumeric() if x else x)
tem = tem.loc[tem['numeric']==True]
tem['current_salary'] = tem['salryrange']
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% salary type
tem = candidate[['candidate_externalid']]
tem['SalaryType'] = 'peryear'
vcand.update_salary_type(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# %% last activity date
tem = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid,
max(CombinedDate) as last_date
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
CROSS APPLY ( VALUES ( LASTCONTON ), ( LASTDATE ),(LASTATMPON)) AS x ( CombinedDate )
where KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
group by c1.ACCOUNTNO
""", engine_mssql)
tem = tem.drop_duplicates().dropna()
tem['last_activity_date'] = pd.to_datetime(tem['last_date'])
vcand.update_last_activity_date(tem, mylog)

# %% industry
industries = pd.read_csv('industry.csv')
ind = pd.read_sql("""select c1.ACCOUNTNO as candidate_externalid, nullif(U_KEY2,'') as industry
from CONTACT1 c1
where KEY1 in (
'Placed'
,'Do not Use'
,'Candidate')
""", engine_mssql)
ind = ind.dropna().drop_duplicates()
ind = ind.merge(industries, left_on='industry', right_on='GM Value')
ind['name'] = ind['Vincere Industry']
cp10 = vcand.insert_candidate_industry_subindustry(ind, mylog)