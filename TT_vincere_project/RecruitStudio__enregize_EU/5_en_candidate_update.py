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
select c.ContactId as candidate_externalid
     , nullif(c.FirstName,'') as FirstName
     , nullif(c.LastName,'') as LastName
     , nullif(c.AlsoKnownAs,'') as AlsoKnownAs
      , nullif(Address1,'') as Address1
     , nullif(Address2,'') as Address2
     , nullif(Address3,'') as Address3
     , nullif(City,'') as City
     , nullif(c.Postcode,'') as Add_Postcode
     , nullif(County,'') as County
     , nullif(c.Country,'') as Country
     , nullif(c.EMail2,'') as wEmail
     , nullif(c.DirectTel,'') as DirectTel
     , nullif(c.WorkTel,'') as WorkTel
     , nullif(c.MobileTel,'') as MobileTel
     , nullif(c.HomeTel,'') as HomeTel
     , nullif(c.Department,'') as Department
     , nullif(c.Location,'') as Location
     , nullif(c.SubLocation,'') as SubLocation
     , nullif(u.Email,'') as owner
     , nullif(c.RegDate,'') as RegDate
     , nullif(c.Title,'') as Title
     , nullif(c.Latitude,'') as latitude
     , nullif(c.Longitude,'') as longitude
     , nullif(c.IsMaleGender,'') as IsMaleGender
     , nullif(c.Email3,'') as pEmail
     , nullif(cand.DoB,'') as DoB
     , nullif(cand.Nationality,'') as Nationality
     , nullif(cand.CurrentSalary,'') as CurrentSalary
     , nullif(cand.Currency1,'') as Currency1
     , nullif(cand.NoticePeriod,'') as NoticePeriod
     , nullif(cand.Benefits,'') as Benefits
     , nullif(c.Type,'') as Type
     , nullif(ContactSource,'') as Source
     , nullif(c.JobTitle,'') as JobTitle 
     , nullif(c.Company,'') as Company 
from Contacts c
left join Users u on u.UserId = c.LastUser
left join Candidates cand on cand.ContactId = c.ContactId
where  c.Descriptor = 2 or c.Descriptor is null
""", engine_mssql)
candidate['candidate_externalid'] = 'EUK'+candidate['candidate_externalid']
assert False
# %% location name/address
candidate['address'] = candidate[['Address1', 'Address2', 'Address3', 'City', 'Add_Postcode', 'County', 'Country']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate['location_name'] = candidate[['Add_Postcode', 'SubLocation', 'Location']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = candidate[['candidate_externalid', 'address', 'location_name']].drop_duplicates()
cp2 = vcand.insert_common_location_v2(tem, dest_db, mylog)

# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = candidate[['candidate_externalid', 'City', 'Add_Postcode', 'County', 'Country']].drop_duplicates()\
    .rename(columns={'City': 'city', 'County': 'district', 'Add_Postcode': 'post_code'})
comaddr.loc[comaddr['city'].notnull()]
tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'district']].dropna()
cp4 = vcand.update_location_district2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
tem = comaddr[['candidate_externalid', 'Country']].dropna()
tem['country_code'] = tem.Country.map(vcand.get_country_code)
cp6 = vcand.update_location_country_code2(tem, dest_db, mylog)
# %% latitude longitude
tem = candidate[['candidate_externalid', 'latitude', 'longitude']]
tem = tem.loc[tem['latitude'].notnull()]
tem = tem.loc[tem['longitude'].notnull()]
tem['latitude'] = tem['latitude'].astype(float)
tem['longitude'] = tem['longitude'].astype(float)
tem['longitude'].unique()
cp6 = vcand.update_location_latlong(tem, mylog)

# %% home phones
home_phone = candidate[['candidate_externalid', 'HomeTel']].drop_duplicates().dropna()
home_phone['home_phone'] = home_phone['HomeTel']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'WorkTel']].drop_duplicates().dropna()
wphone['work_phone'] = wphone['WorkTel']
cp = vcand.update_work_phone(wphone, mylog)

# %% source
tem = candidate[['candidate_externalid', 'Source']].dropna().drop_duplicates()
tem['Source'].unique()
tem.loc[tem['Source']=='Xing']
tem.loc[tem['Source']=='Connect (Odro)', 'source'] = 'Connect (Odro)'
tem.loc[tem['Source']=='Energize Mailshot', 'source'] = 'Energize Mailshot'
tem.loc[tem['Source']=='Energize Website', 'source'] = 'Energize Website'
tem.loc[tem['Source']=='Headhunt', 'source'] = 'Headhunt'
tem.loc[tem['Source']=='Indeed', 'source'] = 'Indeed'
tem.loc[tem['Source']=='JobServe - Ad Response', 'source'] = 'JobServe - Ad Response'
tem.loc[tem['Source']=='LinkedIn', 'source'] = 'LinkedIn'
tem.loc[tem['Source']=='LinkedIn - Ad Response', 'source'] = 'LinkedIn - Ad Response'
tem.loc[tem['Source']=='LinkedIn Recruiter', 'source'] = 'LinkedIn Recruiter'
tem.loc[tem['Source']=='Recruit Studio', 'source'] = 'Recruit Studio'
tem.loc[tem['Source']=='Reed - Ad Response', 'source'] = 'Reed - Ad Response'
tem.loc[tem['Source']=='Reed - Search', 'source'] = 'Reed - Search'
tem.loc[tem['Source']=='Referral', 'source'] = 'Referral'
tem.loc[tem['Source']=='Totaljobs - Ad Response', 'source'] = 'Totaljobs - Ad Response'
tem.loc[tem['Source']=='Totaljobs - Search', 'source'] = 'Totaljobs - Search'
tem.loc[tem['Source']=='Xing', 'source'] = 'Xing'
tem2 = tem[['candidate_externalid', 'source']].dropna().drop_duplicates()
cp = vcand.insert_source(tem2)

# %% met notmet
# cand = pd.read_sql("""
# select distinct cand.idperson as candidate_externalid
# from activitylogentity ale
# join (select px.idperson
#      , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
# from candidate c
# join (select personx.idperson, personx.createdon from personx where isdeleted = '0') px on c.idperson = px.idperson
# ) cand on cand.idperson = ale.idPerson
# join activitylog al on al.idactivitylog = ale.idactivitylog
# LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
# LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
# LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
# LEFT JOIN task t ON tl.idtask = t.idtask
# LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
# LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
# left join "user" u on u.fullname = al.createdby
# where al.subject like '%F2F Meeting%'
# """, engine_sqlite)
# cand['status'] = 1
# cp = vcand.update_met_notmet(cand, mylog)

# %% mobile
indt = candidate[['candidate_externalid', 'MobileTel']].dropna().drop_duplicates()
indt['mobile_phone'] = indt['MobileTel']
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)

# %% gender
tem = candidate[['candidate_externalid', 'IsMaleGender']]
tem['IsMaleGender'].unique()
tem.loc[tem['IsMaleGender']==True, 'male'] = 1
tem.loc[tem['IsMaleGender'].isnull(), 'male'] = 0
tem['male'] = tem['male'].astype(int)
cp = vcand.update_gender(tem, mylog)

# %% primary phones
indt = candidate[['candidate_externalid', 'MobileTel']].dropna().drop_duplicates()
indt['primary_phone'] = indt['MobileTel']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)

# %% work emails
mail = candidate[['candidate_externalid', 'wEmail']].drop_duplicates().dropna()
mail['work_email'] = mail['wEmail']
cp = vcand.update_work_email(mail, mylog)

# %% personal emails
mail = candidate[['candidate_externalid', 'pEmail']].drop_duplicates().dropna()
mail['personal_email'] = mail['pEmail']
cp = vcand.update_personal_email(mail, mylog)

# %% skills
sql ="""select ski.* from
(select si.ObjectId, s.Skill from SkillInstances si
left join Skills s on s.SkillId = si.SkillId) ski
join (select ContactId from Contacts where Descriptor = 2 or Descriptor is not null) c on c.ContactId = ski.ObjectId"""
sk = pd.read_sql(sql, engine_mssql)
sk['candidate_externalid'] = 'EUK'+sk['ObjectId']
sk['skills'] = sk['Skill']
sk = sk.dropna()
cp = vcand.update_skills(sk, mylog)
# %% current employer
cur_emp = candidate[['candidate_externalid', 'Company', 'JobTitle']]
cur_emp['current_employer'] = cur_emp['Company']
cur_emp['current_job_title'] = cur_emp['JobTitle']
cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\\',' ') if x else x)
cur_emp['current_employer'] = cur_emp['current_employer'].apply(lambda x: x.replace('\\',' ') if x else x)
a = cur_emp[6952:6953]
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

# %% current employer 2
# cur_emp = candidate[['candidate_externalid', 'PreviousCompany', 'PreviousJobTitle']]
# cur_emp['current_employer'] = cur_emp['PreviousCompany']
# cur_emp['current_job_title'] = cur_emp['PreviousJobTitle']
# cur_emp = cur_emp.loc[cur_emp['current_employer'].notnull()]
# cur_emp.loc[cur_emp['candidate_externalid'] == 'ff771e56-7283-4519-91c1-dd7b8a3e5492']
# vcand.update_candidate_current_employer2(cur_emp, dest_db, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'Title']].dropna().drop_duplicates()
tem['gender_title'] = tem['Title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% note
note = pd.read_sql("""
select c.ContactId as candidate_externalid
     , nullif(c.Type,'') as Type
     , nullif(CanRelocate,'') as CanRelocate
     , nullif(c.DateCVAdded,'') as DateCVAdded
     , nullif(ContactStatus,'') as ContactStatus
     , nullif(Location,'') as Location
     , nullif(c.SubLocation,'') as SubLocation
     , nullif(Source,'') as Source
     , nullif(Comments,'') as Comments
     , nullif(GDPRStatus,'') as GDPRStatus
from Contacts c
left join Candidates cand on c.ContactId = cand.ContactId
where  c.Descriptor = 2 or c.Descriptor is null""", engine_mssql)
note['candidate_externalid'] = 'EUK'+note['candidate_externalid']
note['CanRelocate'] = note['CanRelocate'].apply(lambda x: str(x) if x else x)
# note['DateCVAdded'] = note['DateCVAdded'].apply(lambda x: str(x) if x else x)
# note['DateCVAdded'] = note['DateCVAdded'].apply(lambda x: x.replace('NaT',''))
note['note'] = note[['Type', 'CanRelocate'
    , 'ContactStatus','Comments','GDPRStatus']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Contact Type', 'Relocation'
                                                      , 'Status','Registration Notes','GDPR status'], x) if e[1]]), axis=1)

cp7 = vcand.update_note2(note, dest_db, mylog)

# %% job type
jobtp = candidate[['candidate_externalid', 'Type']].dropna().drop_duplicates()
jobtp['Type'].unique()
jobtp.loc[jobtp['candidate_externalid']=='EUK588647-1286-18348']
jobtp.loc[jobtp['Type']=='Candidate', 'desired_job_type'] = 'permanent'
jobtp.loc[jobtp['Type']=='Contractor', 'desired_job_type'] = 'contract'
tem = jobtp[['candidate_externalid', 'desired_job_type']].dropna().drop_duplicates()
cp = vcand.update_desired_job_type(tem, mylog)

# %% preferred name
tem = candidate[['candidate_externalid', 'AlsoKnownAs']].dropna().drop_duplicates()
tem['preferred_name'] = tem['AlsoKnownAs']
vcand.update_preferred_name(tem, mylog)

# %% middle name
tem = candidate[['candidate_externalid', 'middlename']].dropna().drop_duplicates()
tem['middle_name'] = tem['middlename']
vcand.update_middle_name(tem, mylog)

# %% reg date
reg_date = candidate[['candidate_externalid', 'RegDate']].dropna().drop_duplicates()
reg_date['reg_date'] = pd.to_datetime(reg_date['RegDate'])
vcand.update_reg_date(reg_date, mylog)

# %% citizenship
tem = candidate[['candidate_externalid', 'Nationality']].dropna().drop_duplicates()
tem['nationality'] = tem['Nationality'].map(vcand.get_country_code)
tem.loc[tem['candidate_externalid']=='100558-2618-1415']
vcand.update_nationality(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid','Currency1']].dropna().drop_duplicates()
tem['Currency1'].unique()
tem.loc[tem['Currency1']=='GBP', 'currency_of_salary'] = 'pound'
tem.loc[tem['Currency1']=='USD', 'currency_of_salary'] = 'usd'
tem.loc[tem['Currency1']=='Euro', 'currency_of_salary'] = 'euro'
tem2 = tem[['candidate_externalid', 'currency_of_salary']].dropna().drop_duplicates()
vcand.update_currency_of_salary(tem2, mylog)


# %% dob
tem = candidate[['candidate_externalid', 'DoB']].dropna().drop_duplicates()
tem.to_csv('age.csv')
tem = tem.loc[tem['DoB'] != '9998-12-31 00:00:00']
tem['date_of_birth'] = pd.to_datetime(tem['DoB'])
vcand.update_dob(tem, mylog)

# %% other benefits
tem = candidate[['candidate_externalid', 'Benefits']].dropna().drop_duplicates()
tem['other_benefits'] = tem['Benefits']
vcand.update_other_benefits(tem, mylog)

# %% notice
tem = candidate[['candidate_externalid', 'NoticePeriod']].dropna().drop_duplicates()
tem['NoticePeriod'].unique()
tem.loc[tem['NoticePeriod']=='1 Month', 'notice_period'] = 30
tem.loc[tem['NoticePeriod']=='1 Week', 'notice_period'] = 7
tem.loc[tem['NoticePeriod']=='2 Months', 'notice_period'] = 60
tem.loc[tem['NoticePeriod']=='3 Months', 'notice_period'] = 90
tem.loc[tem['NoticePeriod']=='2 Weeks', 'notice_period'] = 14
tem.loc[tem['NoticePeriod']=='6 Months', 'notice_period'] = 180
tem.loc[tem['NoticePeriod']=='1 month', 'notice_period'] = 30
tem.loc[tem['NoticePeriod']=='3 Weeks', 'notice_period'] = 21
tem.loc[tem['NoticePeriod']=='4 Weeks', 'notice_period'] = 28
tem.loc[tem['NoticePeriod']=='6 Weeks', 'notice_period'] = 42
tem.loc[tem['NoticePeriod']=='6 weeks', 'notice_period'] = 42
tem.loc[tem['NoticePeriod']=='3 months ', 'notice_period'] = 90
tem.loc[tem['NoticePeriod']=='4 weeks', 'notice_period'] = 28
tem.loc[tem['NoticePeriod']==' 8 weeks', 'notice_period'] = 56
tem.loc[tem['NoticePeriod']=='2 WEEKS', 'notice_period'] = 14
tem.loc[tem['NoticePeriod']=='2 weeks', 'notice_period'] = 14
tem.loc[tem['NoticePeriod']=='4 Months', 'notice_period'] = 120
tem.loc[tem['NoticePeriod']=='1month', 'notice_period'] = 30
tem.loc[tem['NoticePeriod']=='6 weeks to quarter end', 'notice_period'] = 180
tem.loc[tem['NoticePeriod']=='7 months', 'notice_period'] = 210
tem.loc[tem['NoticePeriod']=='2 weeks ', 'notice_period'] = 14
tem.loc[tem['NoticePeriod']=='7 Months', 'notice_period'] = 210
tem.loc[tem['NoticePeriod']=='6 weeks ', 'notice_period'] = 42
tem.loc[tem['NoticePeriod']=='3 months', 'notice_period'] = 90
tem.loc[tem['NoticePeriod']=='Under 1 month ', 'notice_period'] = 30
tem.loc[tem['NoticePeriod']=='3 Months/6weeks', 'notice_period'] = 90
tem.loc[tem['NoticePeriod']=='3months flexible', 'notice_period'] = 90
tem.loc[tem['NoticePeriod']=='4 months', 'notice_period'] = 120
tem.loc[tem['NoticePeriod']=='1 week ', 'notice_period'] = 7
tem.loc[tem['NoticePeriod']=='7 Weeks', 'notice_period'] = 49
tem.loc[tem['NoticePeriod']=='13 Weeks', 'notice_period'] = 91
tem.loc[tem['NoticePeriod']=='1week', 'notice_period'] = 7
tem.loc[tem['NoticePeriod']=='6 week', 'notice_period'] = 42
tem.loc[tem['NoticePeriod']=='2 months ', 'notice_period'] = 60
tem2 = tem[['candidate_externalid','notice_period']].dropna()
vcand.update_notice_period(tem2, mylog)

# %% Gross per anum
# tem = cand_salary[['candidate_externalid', 'Package']].dropna().drop_duplicates()
# tem['total_p_a'] = tem['Package']
# tem['total_p_a'] = tem['total_p_a'].astype(float)
# vcand.update_total_gross_per_annum(tem, mylog)

# %% current salary
tem = candidate[['candidate_externalid', 'CurrentSalary']].dropna().drop_duplicates()
tem['current_salary'] = tem['CurrentSalary']
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% marital
tem = candidate[['candidate_externalid', 'marital']].dropna().drop_duplicates()
tem.loc[tem['marital']=='Single', 'maritalstatus'] = 1
tem.loc[tem['marital']=='Married', 'maritalstatus'] = 2
tem.loc[tem['marital']=='Widowed', 'maritalstatus'] = 4
tem2 = tem[['candidate_externalid','maritalstatus']].dropna().drop_duplicates()
tem2['maritalstatus'] = tem2['maritalstatus'].apply(lambda x: int(str(x).split('.')[0]))
vcand.update_marital_status(tem2, mylog)

# %% current bonus
# tem = cand_salary[['candidate_externalid', 'Bonus']].dropna().drop_duplicates()
# tem['current_bonus'] = tem['Bonus']
# tem['current_bonus'] = tem['current_bonus'].astype(float)
# vcand.update_current_bonus(tem, mylog)

# %% linkedin
# tem = cand1.loc[cand1['type'] == 'LinkedIn']
# tem = tem.dropna()
# tem3 = cand1.loc[cand1['type'] == 'URL']
# tem3 = tem3.dropna()
# tem2 = candidate[['candidate_externalid', 'urlprivate']].dropna().drop_duplicates().rename(columns={'urlprivate': 'value'})
# a = tem2.loc[tem2['value'].str.contains('linkedin')]
# b = tem.loc[tem['value'].str.contains('linkedin')]
# c = tem3.loc[tem3['value'].str.contains('linkedin')]
#
# lik = pd.concat([a,b,c])
# lk = lik[['candidate_externalid','value']]
# lk['linkedin'] = lk['value']
# vcand.update_linkedin(lk, mylog)

# %% industry
sql ="""select skc.* from
(select sc.ObjectId, s.Sector from SectorInstances sc
left join Sectors s on s.SectorId = sc.SectorId) skc
join (select ContactId from Contacts where Descriptor = 2 or Descriptor is null) c on c.ContactId = skc.ObjectId
where skc.Sector is not null"""
industry = pd.read_sql(sql, engine_mssql)
industry['candidate_externalid'] = 'EUK'+industry['ObjectId']
industry['name'] = industry['Sector']
cp10 = vcand.insert_candidate_industry_subindustry(industry, mylog)

# %% education
# edu = pd.read_sql("""
# select px.*, edu.*
# from candidate c
# join (select P.idperson as candidate_externalid, p.firstname, p.lastname
#                from personx P
# where isdeleted = '0') px on c.idperson = px.candidate_externalid
# left join (select e.*, q.value from education e left join qualification q on e.idqualification = q.idqualification) edu on edu.idperson = px.candidate_externalid
# """, engine_postgre_src)
# edu['schoolName'] = edu['educationestablishment']
# edu['degreeName'] = edu['educationsubject']
# edu['graduationDate'] = edu['educationto']
# edu['startDate'] = edu['educationfrom']
# edu['qualification'] = edu['value']
# cp9 = vcand.update_education(edu, mylog)

# %% languages
sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid, idlanguage_string_list
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
where idlanguage_string_list is not null
"""
cand_languages = pd.read_sql(sql, engine_sqlite)

languages = cand_languages.idlanguage_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand_languages[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idlanguage_string') \
    .drop('variable', axis='columns') \
    .dropna()
languages['idlanguage_string'] = languages['idlanguage_string'].str.lower()
idlanguage = pd.read_sql("""
select idlanguage, value from language
""", engine_sqlite)

language = languages.merge(idlanguage, left_on='idlanguage_string', right_on='idLanguage', how='left')
language['language'] = language['Value'].apply(lambda x: x.split('-')[0])
language['level'] = language['Value'].apply(lambda x: x.split('-')[1])

language['language'].unique()
language['level'].unique()

df = language
logger = mylog

tem2 = df[['candidate_externalid', 'language', 'level']]
tem2.loc[tem2['language']=='Portugese', 'language'] = 'Portuguese'
tem2.loc[tem2['language']=='Chinese Mandarin', 'language'] = 'Chinese (Mandarin/Putonghua)'
tem2.loc[tem2['language']=='Chinese Wu', 'language'] = 'Chinese (Shanghainese)'
tem2.loc[tem2['language']=='Serbo Croation', 'language'] = 'Serbo-Croatian'
tem2.loc[tem2['language']=='Chinese Cantonese/Yue', 'language'] = 'Chinese (Other)'

tem2.loc[tem2['level']=='Working', 'level'] = 'fluent'
tem2.loc[tem2['level']=='Fluent', 'level'] = 'fluent'
tem2.loc[tem2['level']=='Mother Tongue', 'level'] = 'native'

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
tem2 = tem2.fillna('')

tem2.languageCode = tem2.languageCode.map(lambda x: '"languageCode":"%s"' % x)
tem2.level = tem2.level.map(lambda x: '"level":"%s"' % x)
tem2['skill_details_json'] = tem2[['languageCode', 'level']].apply(lambda x: '{%s}' % (','.join(x)), axis=1)
tem2 = tem2.groupby('candidate_externalid')['skill_details_json'].apply(','.join).reset_index()
tem2.skill_details_json = tem2.skill_details_json.map(lambda x: '[%s]' % x)
# [{"languageCode":"km","level":""},{"languageCode":"my","level":""}]
tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid from candidate", vcand.ddbconn), on=['candidate_externalid'])
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcand.ddbconn, ['skill_details_json', ], ['id', ], 'candidate', logger)

# cp8 = vcand.update_skill_languages()




# %% create talent pool
d_list = pd.read_sql("""
select
u.Email as owner, AccessModifier,ListName
from Collections_3 col
join (select ContactId from Contacts where Descriptor = 2 or Descriptor is null) c on c.ContactId = col.ObjectId
left join Users u on col.Username = u.UserName
where charindex('^',ListName) > 0 or charindex('*',ListName) > 0
""", engine_mssql)
d_list = d_list.drop_duplicates()
d_list['name'] = d_list['ListName']
d_list['share_permission'] = d_list['AccessModifier'].apply(lambda x: x.lower() if x else x)

df = d_list
tem2 = df.drop_duplicates()
if 'insert_timestamp' not in tem2.columns:
    tem2['insert_timestamp'] = datetime.datetime.now()
if 'share_permission' not in tem2.columns:
    tem2['share_permission'] = 1
else:
    tem2.loc[tem2.share_permission == 'public', 'share_permission'] = 1
    tem2.loc[tem2.share_permission == 'private', 'share_permission'] = 2
    tem2.loc[tem2.share_permission == 'team', 'share_permission'] = 3
tem2['owner']=tem2['owner'].fillna('')
df_owner = pd.read_sql("select id as owner_id, email from user_account", vcand.ddbconn)
tem2 = tem2.merge(df_owner, left_on='owner', right_on='email', how='left')
tem2 = tem2.where(tem2.notnull(),None)
tem2['owner_id'] = tem2['owner_id'].map(lambda x: x if x else -10)
vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, vcand.ddbconn, ['owner_id', 'name', 'share_permission', 'insert_timestamp'], 'candidate_group', mylog)



d_list_cont = pd.read_sql("""
select
ObjectId as candidate_externalid
,ListName as name,u.Email as owner
from Collections_3 col
join (select ContactId from Contacts where Descriptor = 2 or Descriptor is null) c on c.ContactId = col.ObjectId
left join Users u on col.Username = u.UserName
""", engine_mssql)
d_list_cont = d_list_cont.drop_duplicates()
d_list_cont['candidate_externalid'] = 'EUK'+d_list_cont['candidate_externalid']


# d_list_cont.loc[d_list_cont['name']=='Clients - Suspects - North West']
d_list_cont['owner'] = d_list_cont['owner'].fillna('')

df = d_list_cont
tem2 = df[['candidate_externalid', 'name','owner']].drop_duplicates()
if 'insert_timestamp' not in tem2.columns:
    tem2['insert_timestamp'] = datetime.datetime.now()
tem2 = tem2.merge(vcand.candidate, on=['candidate_externalid'])

df_owner = pd.read_sql("select id as owner_id, email from user_account", vcand.ddbconn)
tem2 = tem2.merge(df_owner, left_on='owner', right_on='email', how='left')
tem2 = tem2.where(tem2.notnull(), None)
tem2['owner_id'] = tem2['owner_id'].map(lambda x: x if x else -10)
tem2.loc[tem2['owner']=='']
tem2.loc[tem2['owner_id'] == -10]

df_group = pd.read_sql("select id as candidate_group_id,owner_id, name from candidate_group", vcand.ddbconn)
tem2 = tem2.merge(df_group, on=['name','owner_id'])
tem2 = tem2.drop_duplicates()
tem2['candidate_id'] = tem2['id']
tem = tem2[['candidate_group_id', 'candidate_id', 'insert_timestamp']].drop_duplicates()

df_group_cand = pd.read_sql("select id, candidate_group_id,candidate_id from candidate_group_candidate", vcand.ddbconn)
tem = tem.merge(df_group_cand, on=['candidate_group_id', 'candidate_id'], how='left')
tem = tem.query("id.isnull()")

vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, vcand.ddbconn, ['candidate_group_id', 'candidate_id', 'insert_timestamp'], 'candidate_group_candidate', mylog)




d_list_team = pd.read_sql("""
select ListName as name, u.Email as owner
from Collections_3 col
join (select ContactId from Contacts where Descriptor = 2 or Descriptor is null) c on c.ContactId = col.ObjectId
left join Users u on u.TeamId = col.TeamId
where AccessModifier = 'Team'
and (charindex('^',ListName) > 0 or charindex('*',ListName) > 0)
""", engine_mssql)
d_list_team['owner']=d_list_team['owner'].fillna('')
df_owner = pd.read_sql("select id as owner_id, email from user_account", vcand.ddbconn)
tem2 = d_list_team.merge(df_owner, left_on='owner', right_on='email', how='left')

df_group = pd.read_sql("select id as candidate_group_id, name from candidate_group where share_permission = 3", vcand.ddbconn)
tem3 = tem2.merge(df_group, on='name', how='left')
tem3['insert_timestamp'] = datetime.datetime.now()
tem3['user_account_id'] = tem3['owner_id']
tem3 = tem3.loc[tem3['user_account_id'].notnull()]
tem3.loc[tem3['contact_group_id']==22406]
tem3=tem3.drop_duplicates()
vincere_custom_migration.psycopg2_bulk_insert_tracking(tem3, vcand.ddbconn, ['candidate_group_id', 'user_account_id', 'insert_timestamp'], 'candidate_group_user_account', mylog)

# %% GDPR
tem1 = candidate[['candidate_externalid']]

tem1['external_id'] = tem1['candidate_externalid']
tem1['portal_status'] = 5  # 1:Consent given / 2:Pending [Consent to keep] / 3:To be forgotten / 4:Contract / 5:Legitimate interest
tem1['insert_timestamp'] = datetime.datetime.now()
cols = ['candidate_id',
        'portal_status',  # 1:Consent given / 2:Pending [Consent to keep]
        'insert_timestamp']
vincere_custom_migration.insert_candidate_gdpr_compliance(tem1, connection, cols)