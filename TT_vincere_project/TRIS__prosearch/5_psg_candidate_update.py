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
cf.read('psg_config.ini')
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
select c.PersonID as candidate_externalid,
       c.PersonFirstName,
       c.PersonSurname,
       c.PersonHomeEMail,
       tT.TName as title,
       nullif(convert(varchar,c.PersonGender),'') as PersonGender,
       nullif(convert(varchar,c.PersonHomeTelephone),'') as PersonHomeTelephone,
       nullif(convert(varchar,c.PersonWorkTelephone),'') as PersonWorkTelephone,
       nullif(convert(varchar,c.PersonOtherPhone),'') as PersonOtherPhone,
       nullif(convert(varchar,c.PersonMobileTelephone),'') as PersonMobileTelephone,
       nullif(convert(varchar,c.PersonCreateDate),'') as PersonCreateDate,
       nullif(convert(varchar,c.PersonLastUpdate),'') as PersonLastUpdate,
       nullif(convert(varchar,c.PersonWorkEMail),'') as PersonWorkEMail,
       nullif(convert(varchar,c.PersonKnownAs),'') as PersonKnownAs,
       nullif(convert(varchar,DateOfBirth),'') as DateOfBirth,
       nullif(convert(varchar,SkypeAccount),'') as SkypeAccount,
       nullif(convert(varchar,LinkedInUrl),'') as LinkedInUrl,
       nullif(convert(varchar,FacebookAccount),'') as FacebookAccount,
       nullif(convert(varchar,TwitterAccount),'') as TwitterAccount,
       jt.JobTypeName,
       o.OriginName,
       r.ResidenceDesc,
       nullif(convert(varchar,c.CandidateVisaExpireDate),'') as CandidateVisaExpireDate,
       nullif(convert(varchar,c.CandidateCurrentSalary),'') as CandidateCurrentSalary,
       nullif(convert(varchar,c.CandidateWantedSalary),'') as CandidateWantedSalary,
       nullif(convert(varchar,c.CandidateCurrentRate),'') as CandidateCurrentRate,
       nullif(convert(varchar,c.CandidateWantedRate),'') as CandidateWantedRate,
       nullif(convert(varchar,c.CandidateWorkingAtCompany),'') as CandidateWorkingAtCompany,
       nullif(convert(varchar,c.CandidateOnHold),'') as CandidateOnHold,
       nullif(convert(varchar,c.CandidateReasonOnHold),'') as CandidateReasonOnHold,
       nullif(convert(nvarchar(max),c.CandidateCurrentSkills),'') as CandidateCurrentSkills,
       nullif(convert(varchar,c.CandidateClass),'') as CandidateClass,
       nullif(convert(nvarchar(max),c.CandidateInfo),'') as CandidateInfo,
       nullif(convert(varchar,c.CandidateAddress),'') as CandidateAddress,
       nullif(convert(varchar,c.CandidateAddress2),'') as CandidateAddress2,
       nullif(convert(varchar,c.CandidateCity),'') as CandidateCity,
       nullif(convert(varchar,c.CandidateState),'') as CandidateState,
       nullif(convert(varchar,c.CandidatePostcode),'') as CandidatePostcode,
       nullif(convert(varchar,c.CandidateCountry),'') as CandidateCountry,
       nullif(convert(varchar,c.CandidatePOAddress),'') as CandidatePOAddress,
       nullif(convert(varchar,c.CandidatePOAddress2),'') as CandidatePOAddress2,
       nullif(convert(varchar,c.CandidatePOCity),'') as CandidatePOCity,
       nullif(convert(varchar,c.CandidatePOState),'') as CandidatePOState,
       nullif(convert(varchar,c.CandidatePOPostcode),'') as CandidatePOPostcode,
       nullif(convert(varchar,c.CandidatePOCountry),'') as CandidatePOCountry,
       nullif(convert(varchar,CurrentPosition),'') as CurrentPosition, tf.FName as salary_type, c.CandidateAvailability, WantedWorkLocation
from Candidate c
left join tblTitle tT on c.TitleID = tT.TitleID
left join tblPerson p on p.PersonID = c.PersonID
left join Origin o on o.OriginID = c.CandidateOriginID
left join ResidentalStatus r on r.ResidenceID = c.CandidateOriginID
left join tblCandidate tc on c.PersonID = tc.CandidateID
left join JobType jt on c.CandidateWantedJobType = jt.JobTypeID
left join tblFrequency tF on tc.PayFrequencyID = tF.FrequencyID
""", engine_mssql)
candidate['candidate_externalid'] = candidate['candidate_externalid'].astype(str)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','CandidateAddress', 'CandidateAddress2', 'CandidateCity', 'CandidateState', 'CandidatePostcode', 'CandidateCountry']]
c_location['address'] = c_location[['CandidateAddress', 'CandidateAddress2', 'CandidateCity', 'CandidateState', 'CandidatePostcode', 'CandidateCountry']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['address']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid', 'CandidateCity', 'CandidatePostcode', 'CandidateState', 'CandidateCountry']].drop_duplicates()\
    .rename(columns={'CandidateCity': 'city', 'CandidateState': 'state', 'CandidatePostcode': 'post_code'})
tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
tem = comaddr[['candidate_externalid', 'CandidateCountry']].dropna()
tem['country_code'] = tem.CandidateCountry.map(vcand.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code2(tem, dest_db, mylog)

# %% desired
df = candidate[['candidate_externalid','WantedWorkLocation']].dropna()
df = df.loc[df['WantedWorkLocation']!='']
df['location_name'] = df['WantedWorkLocation']
df['address'] = df['location_name']
tem2 = df[['location_name', 'address', 'candidate_externalid', ]]

tem3 = tem2[['address','location_name']].drop_duplicates()


existed_add = pd.read_sql("select address, id as location_id,insert_timestamp from common_location", vcand.ddbconn)
existed_add = existed_add.where(existed_add.notnull(), None)

tem3 = tem3.merge(existed_add, on=['address'], how='left')
tem3 = tem3.query("location_id.isnull()")

if tem3.shape[0]>0:
    tem3['insert_timestamp'] = datetime.datetime.now()
    vincere_custom_migration.psycopg2_bulk_insert_tracking(tem3, vcand.ddbconn, ['location_name', 'address', 'insert_timestamp'], 'common_location', mylog)

tem2 = tem2.merge(vcand.candidate, on=['candidate_externalid'])
tem2['insert_timestamp'] = tem3[['insert_timestamp']].iat[0,0]
# tem2['insert_timestamp'] = tem2['insert_timestamp'].apply(lambda x: str(x) if x else x)

# existed_add['insert_timestamp'] = existed_add['insert_timestamp'].apply(lambda x: str(x) if x else x)
addr = pd.read_sql("select address, id as location_id from common_location where nullif (address,'') is not null", vcand.ddbconn)
addr['rn'] = addr.groupby('address').cumcount()
addr = addr.loc[addr['rn']==0]
pref_location = tem2.merge(addr, on=['address'], how='left')
tem4 = pref_location[['id','location_id']]
tem4['location_id'] = tem4['location_id'].astype(str)
tem4 = tem4.groupby('id')['location_id'].apply(','.join).reset_index()
tem4['desired_work_location_list'] = tem4['location_id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem4, vcand.ddbconn, ['desired_work_location_list', ], ['id', ], 'candidate', mylog)

# %% home phones
home_phone = candidate[['candidate_externalid', 'PersonHomeTelephone']].drop_duplicates().dropna()
home_phone['home_phone'] = home_phone['PersonHomeTelephone']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'PersonWorkTelephone']].drop_duplicates().dropna()
wphone['work_phone'] = candidate['PersonWorkTelephone']
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile
indt = candidate[['candidate_externalid', 'PersonMobileTelephone']].dropna().drop_duplicates()
indt['mobile_phone'] = indt['PersonMobileTelephone']
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)

# %% primary phones
indt = candidate[['candidate_externalid', 'PersonMobileTelephone']].dropna().drop_duplicates()
indt['primary_phone'] = indt['PersonMobileTelephone']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)

# %% keyword
tem = pd.read_sql("""
select CandidateID, skill.*
from CandExperience ce
join (select SkillID, SkillDesc
from Skills s
left join SkillCategory sc on s.CategoryID = sc.CategoryID) skill on skill.SkillID = ce.SkillID
""", engine_mssql)
tem['candidate_externalid'] = tem['CandidateID'].astype(str)
tem['keyword'] = tem['SkillDesc']
vcand.update_keyword(tem, mylog)

# %% met notmet
# cand = candidate[['candidate_externalid']]
# cand['status'] = 1
# cp = vcand.update_met_notmet(cand, mylog)

# %% gender
tem = candidate[['candidate_externalid', 'PersonGender']]
tem['PersonGender'].unique()
tem.loc[tem['PersonGender']=='M', 'male'] = 1
tem.loc[tem['PersonGender']=='F', 'male'] = 0
tem2 = tem[['candidate_externalid','male']].dropna()
tem2['male'] = tem2['male'].astype(int)
cp = vcand.update_gender(tem2, mylog)

# %% work emails
mail = candidate[['candidate_externalid', 'PersonWorkEMail']].drop_duplicates().dropna()
mail['work_email'] = mail['PersonWorkEMail']
cp = vcand.update_work_email(mail, mylog)

# %% personal emails
# mail = candidate[['candidate_externalid', 'pEmail']].drop_duplicates().dropna()
# mail['personal_email'] = mail['pEmail']
# cp = vcand.update_personal_email(mail, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'CandidateWorkingAtCompany', 'CurrentPosition']]
cur_emp['currentEmployer'] = cur_emp['CandidateWorkingAtCompany']
cur_emp['jobTitle'] = cur_emp['CurrentPosition']

cur_emp['current_employer'] = cur_emp['CandidateWorkingAtCompany']
cur_emp['current_job_title'] = cur_emp['CurrentPosition']

cur_emp['jobTitle'] = cur_emp['jobTitle'].apply(lambda x: x.replace('\\','|') if x else x)

vcand.update_candidate_current_employer_v3(cur_emp, dest_db, mylog)

# %% last activity date
tem = candidate[['candidate_externalid', 'PersonLastUpdate']].dropna().drop_duplicates()
tem['last_activity_date'] = pd.to_datetime(tem['PersonLastUpdate'])
vcand.update_last_activity_date(tem, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'title']].dropna().drop_duplicates()
tem['gender_title'] = tem['title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% linkedin
tem = candidate[['candidate_externalid', 'LinkedInUrl']].dropna().drop_duplicates()
tem['linkedin'] = tem['LinkedInUrl']
vcand.update_linkedin(tem, mylog)

# %% skype
tem = candidate[['candidate_externalid', 'SkypeAccount']].dropna().drop_duplicates()
tem['skype'] = tem['SkypeAccount']
vcand.update_skype(tem, mylog)

# %% fb
tem = candidate[['candidate_externalid', 'FacebookAccount']].dropna().drop_duplicates()
tem['facebook'] = tem['FacebookAccount']
vcand.update_facebook(tem, mylog)

# %% twitter
tem = candidate[['candidate_externalid', 'TwitterAccount']].dropna().drop_duplicates()
tem['twitter'] = tem['TwitterAccount']
vcand.update_twitter(tem, mylog)

# %% note
tem = candidate[['candidate_externalid', 'CandidateAvailability','CandidateInfo','PersonOtherPhone']]
tem['CandidateAvailability'] = tem['CandidateAvailability'].astype(str)
tem['CandidateAvailability'] = tem['CandidateAvailability'].apply(lambda x: x.replace('NaT',''))
tem['note'] = tem[['candidate_externalid', 'CandidateAvailability','CandidateInfo','PersonOtherPhone']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID', 'Availability', 'Information','Other Phone'], x) if e[1]]), axis=1)
cp7 = vcand.update_note2(tem, dest_db, mylog)

# %% work history
wh = pd.read_sql("""
select CandidateID as candidate_externalid
     , nullif(convert(varchar,HFrom),'') as HFrom
     , nullif(convert(varchar,HTo),'') as HTo
     , HEmployer
     , HPosition
     , nullif(convert(nvarchar(max),HComments),'') as HComments
     , jt.DisplayName as job_type
from tblHistory h
left join JobType jt on jt.JobTypeID = h.JobTypeID
""", engine_mssql)
wh['candidate_externalid'] = wh['candidate_externalid'].astype(str)
wh['experience'] = wh[['HEmployer', 'HPosition','job_type','HFrom','HTo','HComments']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Employer', 'Position', 'Job Type','From','To','Comments'], x) if e[1]]), axis=1)
cp7 = vcand.update_exprerience_work_history2(wh, dest_db, mylog)

# %% internal note
# tem = pd.read_sql("""
# select CandidateID as candidate_externalid, Answer
# from InternalInterview ii
# left join tblInternalInterviewAnswer tIIA on ii.InternalInterviewID = tIIA.InternalInterviewID
# where nullif(convert(varchar,Answer),'') is not null
# """, engine_mssql)
# tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
# tem['title'] = 'Interview Notes'
# tem['insert_timestamp'] = datetime.datetime.now()
# tem['note'] = tem['Answer']
# tem['rn'] = tem.groupby('candidate_externalid').cumcount()
# cp7 = vcand.insert_internal_note(tem, mylog)

# %% update internal note
tem = pd.read_sql("""
select CandidateID as candidate_externalid, Answer,CreatedDate,UpdatedDate,a.AMName,InternalInterviewDate, tIITIIQ.Name
from InternalInterview ii
left join tblInternalInterviewAnswer tIIA on ii.InternalInterviewID = tIIA.InternalInterviewID
left join AM A on ii.AMID = A.AMID
left join tblInternalInterviewType tIITIIQ on ii.InternalInterviewTypeID = tIITIIQ.ID
where nullif(convert(varchar,Answer),'') is not null
""", engine_mssql)
tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
tem['InternalInterviewDate'] = tem['InternalInterviewDate'].astype(str)
tem['CreatedDate'] = tem['CreatedDate'].astype(str)
tem['UpdatedDate'] = tem['UpdatedDate'].astype(str)
tem['note'] = tem[['InternalInterviewDate', 'Name','Answer','AMName','CreatedDate','UpdatedDate']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Date', 'Type', 'Interview Note','Manager','Created','Updated'], x) if e[1]]), axis=1)
tem = tem.groupby('candidate_externalid')['note'].apply('\n\n'.join).reset_index()
cp7 = vcand.update_internal_note(tem, dest_db, mylog)

# %% job type
jobtp = candidate[['candidate_externalid', 'JobTypeName']].dropna().drop_duplicates()
jobtp['JobTypeName'].unique()
jobtp.loc[jobtp['JobTypeName']=='Permanent', 'desired_job_type'] = 'permanent'
jobtp.loc[jobtp['JobTypeName']=='Pipeline', 'desired_job_type'] = 'permanent'
jobtp.loc[jobtp['JobTypeName']=='Temporary', 'desired_job_type'] = 'contract'
jobtp.loc[jobtp['JobTypeName']=='Contract', 'desired_job_type'] = 'contract'
jobtp.loc[jobtp['JobTypeName']=='Temp & Cont', 'desired_job_type'] = 'contract'
jobtp.loc[jobtp['JobTypeName']=='Retained', 'desired_job_type'] = 'permanent'
tem = jobtp[['candidate_externalid', 'desired_job_type']].dropna().drop_duplicates()
cp = vcand.update_desired_job_type(tem, mylog)

# %% preferred name
tem = candidate[['candidate_externalid', 'PersonKnownAs']].dropna().drop_duplicates()
tem['preferred_name'] = tem['PersonKnownAs']
vcand.update_preferred_name(tem, mylog)

# %% reg date
reg_date = candidate[['candidate_externalid', 'PersonCreateDate']].dropna().drop_duplicates()
reg_date['reg_date'] = pd.to_datetime(reg_date['PersonCreateDate'])
vcand.update_reg_date(reg_date, mylog)

# %% avail
tem = candidate[['candidate_externalid', 'CandidateAvailability']].dropna().drop_duplicates()
tem['availability_start'] = pd.to_datetime(tem['CandidateAvailability'])
vcand.update_availability_start(tem, mylog)

# %% visa status
tem = candidate[['candidate_externalid', 'ResidenceDesc']].dropna().drop_duplicates()
tem['visa_status'] = tem['ResidenceDesc']
vcand.update_visa_status(tem, dest_db, mylog)

# %% visa renewal date
tem = candidate[['candidate_externalid', 'CandidateVisaExpireDate']].dropna().drop_duplicates()
tem['visa_renewal_date'] = pd.to_datetime(tem['CandidateVisaExpireDate'])
vcand.update_visa_renewal_date(tem,dest_db, mylog)

# %% source
tem = candidate[['candidate_externalid', 'OriginName']].dropna().drop_duplicates()
tem['source'] = tem['OriginName']
cp = vcand.insert_source(tem)

# %% citizenship
# tem = candidate[['candidate_externalid', 'Nationality']].dropna().drop_duplicates()
# tem['nationality'] = tem['Nationality'].map(vcand.get_country_code)
# tem.loc[tem['candidate_externalid']=='100558-2618-1415']
# vcand.update_nationality(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'aud'
vcand.update_currency_of_salary(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'DateOfBirth']].dropna().drop_duplicates()
tem['date_of_birth'] = pd.to_datetime(tem['DateOfBirth'])
vcand.update_dob(tem, mylog)

# %% other benefits
# tem = candidate[['candidate_externalid', 'Benefits']].dropna().drop_duplicates()
# tem['other_benefits'] = tem['Benefits']
# vcand.update_other_benefits(tem, mylog)

# %% notice
# tem = candidate[['candidate_externalid', 'NoticePeriod']].dropna().drop_duplicates()
# tem['NoticePeriod'].unique()
# tem.loc[tem['NoticePeriod']=='1 Month', 'notice_period'] = 30
# tem.loc[tem['NoticePeriod']=='1 Week', 'notice_period'] = 7
# tem.loc[tem['NoticePeriod']=='2 Months', 'notice_period'] = 60
# tem.loc[tem['NoticePeriod']=='3 Months', 'notice_period'] = 90
# tem.loc[tem['NoticePeriod']=='2 Weeks', 'notice_period'] = 14
# tem.loc[tem['NoticePeriod']=='6 Months', 'notice_period'] = 180
# tem.loc[tem['NoticePeriod']=='1 month', 'notice_period'] = 30
# tem.loc[tem['NoticePeriod']=='3 Weeks', 'notice_period'] = 21
# tem.loc[tem['NoticePeriod']=='4 Weeks', 'notice_period'] = 28
# tem.loc[tem['NoticePeriod']=='6 Weeks', 'notice_period'] = 42
# tem.loc[tem['NoticePeriod']=='6 weeks', 'notice_period'] = 42
# tem.loc[tem['NoticePeriod']=='3 months ', 'notice_period'] = 90
# tem.loc[tem['NoticePeriod']=='4 weeks', 'notice_period'] = 28
# tem.loc[tem['NoticePeriod']==' 8 weeks', 'notice_period'] = 56
# tem.loc[tem['NoticePeriod']=='2 WEEKS', 'notice_period'] = 14
# tem.loc[tem['NoticePeriod']=='2 weeks', 'notice_period'] = 14
# tem.loc[tem['NoticePeriod']=='4 Months', 'notice_period'] = 120
# tem.loc[tem['NoticePeriod']=='1month', 'notice_period'] = 30
# tem.loc[tem['NoticePeriod']=='6 weeks to quarter end', 'notice_period'] = 180
# tem.loc[tem['NoticePeriod']=='7 months', 'notice_period'] = 210
# tem.loc[tem['NoticePeriod']=='2 weeks ', 'notice_period'] = 14
# tem.loc[tem['NoticePeriod']=='7 Months', 'notice_period'] = 210
# tem.loc[tem['NoticePeriod']=='6 weeks ', 'notice_period'] = 42
# tem.loc[tem['NoticePeriod']=='3 months', 'notice_period'] = 90
# tem.loc[tem['NoticePeriod']=='Under 1 month ', 'notice_period'] = 30
# tem.loc[tem['NoticePeriod']=='3 Months/6weeks', 'notice_period'] = 90
# tem.loc[tem['NoticePeriod']=='3months flexible', 'notice_period'] = 90
# tem.loc[tem['NoticePeriod']=='4 months', 'notice_period'] = 120
# tem.loc[tem['NoticePeriod']=='1 week ', 'notice_period'] = 7
# tem.loc[tem['NoticePeriod']=='7 Weeks', 'notice_period'] = 49
# tem.loc[tem['NoticePeriod']=='13 Weeks', 'notice_period'] = 91
# tem.loc[tem['NoticePeriod']=='1week', 'notice_period'] = 7
# tem.loc[tem['NoticePeriod']=='6 week', 'notice_period'] = 42
# tem.loc[tem['NoticePeriod']=='2 months ', 'notice_period'] = 60
# tem2 = tem[['candidate_externalid','notice_period']].dropna()
# vcand.update_notice_period(tem2, mylog)

# %% Gross per anum
# tem = cand_salary[['candidate_externalid', 'Package']].dropna().drop_duplicates()
# tem['total_p_a'] = tem['Package']
# tem['total_p_a'] = tem['total_p_a'].astype(float)
# vcand.update_total_gross_per_annum(tem, mylog)

# %% current salary
tem = candidate[['candidate_externalid', 'CandidateCurrentSalary']].dropna().drop_duplicates()
tem['current_salary'] = tem['CandidateCurrentSalary']
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% desired salary
tem = candidate[['candidate_externalid', 'CandidateWantedSalary']].dropna().drop_duplicates()
tem['desire_salary'] = tem['CandidateWantedSalary']
tem['desire_salary'] = tem['desire_salary'].astype(float)
vcand.update_desire_salary(tem, mylog)

# %% contract rate
tem = candidate[['candidate_externalid', 'CandidateCurrentRate']].dropna().drop_duplicates()
tem['contract_rate'] = tem['CandidateCurrentRate']
tem['contract_rate'] = tem['contract_rate'].astype(float)
vcand.update_contract_rate(tem, mylog)

# %% desired contract rate
tem = candidate[['candidate_externalid', 'CandidateWantedRate']].dropna().drop_duplicates()
tem['desired_contract_rate'] = tem['CandidateWantedRate']
tem['desired_contract_rate'] = tem['desired_contract_rate'].astype(float)
vcand.update_desired_contract_rate(tem, mylog)

# %% interval
jt = candidate[['candidate_externalid', 'salary_type']].dropna()
jt['salary_type'].unique()
jt.loc[jt['salary_type']=='Monthly', 'contract_interval'] = 'monthly'
jt.loc[jt['salary_type']=='Weekly', 'contract_interval'] = 'weekly'
jt2 = jt[['candidate_externalid', 'contract_interval']].dropna()
cp5 = vcand.update_contract_interval(jt2, mylog)

# %% marital
# tem = candidate[['candidate_externalid', 'marital']].dropna().drop_duplicates()
# tem.loc[tem['marital']=='Single', 'maritalstatus'] = 1
# tem.loc[tem['marital']=='Married', 'maritalstatus'] = 2
# tem.loc[tem['marital']=='Widowed', 'maritalstatus'] = 4
# tem2 = tem[['candidate_externalid','maritalstatus']].dropna().drop_duplicates()
# tem2['maritalstatus'] = tem2['maritalstatus'].apply(lambda x: int(str(x).split('.')[0]))
# vcand.update_marital_status(tem2, mylog)

# %% current bonus
# tem = cand_salary[['candidate_externalid', 'Bonus']].dropna().drop_duplicates()
# tem['current_bonus'] = tem['Bonus']
# tem['current_bonus'] = tem['current_bonus'].astype(float)
# vcand.update_current_bonus(tem, mylog)

# %% make hot
# tem = candidate[['candidate_externalid', 'CandidateClass']].dropna().drop_duplicates()
# tem = tem.loc[tem['CandidateClass']=='1']
# tem['hot_end_date'] = datetime.datetime.now() + datetime.timedelta(days=999)
# # date = date.today() + timedelta(days=10)
# vcand.update_make_hot(tem, mylog)

# %% education
edu = pd.read_sql("""
select CandidateID as candidate_externalid,
       nullif(convert(varchar,e.FromDate),'') as FromDate,
       nullif(convert(varchar,e.ToDate),'') as ToDate,
       nullif(convert(nvarchar(max),e.OrganisationName),'') as OrganisationName,
       nullif(convert(varchar,e.Degree),'') as Degree,
       nullif(convert(varchar,e.Location),'') as Location,
       nullif(convert(nvarchar(max),e.Comments),'') as Comments
from tblEducationHistory e
""", engine_mssql)
edu['candidate_externalid'] = edu['candidate_externalid'].astype(str)
# edu.loc[edu['OrganisationName'].str.contains('"')]
edu['schoolName'] = edu['OrganisationName']
edu['degreeName'] = edu['Degree']
edu['graduationDate'] = edu['ToDate'].apply(lambda x: pd.to_datetime(x) if x else x)
edu['startDate'] = edu['FromDate'].apply(lambda x: pd.to_datetime(x) if x else x)
edu['schoolAddress'] = edu['Location']
# edu['description'] = edu['Comments']
# edu['description'] = edu['description'].apply(lambda x: x.replace('|','') if x else x)
# edu['description'] = edu['description'].apply(lambda x: x.replace('\u2756','') if x else x)
# edu['description'] = edu['description'].apply(lambda x: x.replace('\u25e6','') if x else x)
# edu['description'] = edu['description'].apply(lambda x: x.replace('\n',' ') if x else x)
# edu['description'] = edu['description'].apply(lambda x: x.replace('\t',' ') if x else x)

cp9 = vcand.update_education(edu, dest_db, mylog)
tem = edu[['candidate_externalid','Comments','Location']].drop_duplicates()
tem['education_summary'] = tem[['Location','Comments']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Location','Comments'], x) if e[1]]), axis=1)
vcand.update_education_summary_v2(tem, dest_db, mylog)

# %% delete
# df = candidate[['candidate_externalid', 'CandidateOnHold']].dropna()
# df = df.loc[df['CandidateOnHold']=='1']
# df['deleted_timestamp'] = datetime.datetime.now()
# tem2 = df[['candidate_externalid', 'deleted_timestamp']]
# # transform data
# tem2 = tem2.merge(vcand.candidate, on=['candidate_externalid'])
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcand.ddbconn, ['deleted_timestamp'], ['id'], 'candidate', mylog)

# %% delete
# cand = pd.read_sql("""Select c.PersonID as candidate_externalid,
#        c.PersonFirstName,
#        c.PersonSurname,
#        nullif(convert(varchar,c.CandidateOnHold),'') as CandidateOnHold,
#        nullif(convert(varchar,c.CandidateReasonOnHold),'') as CandidateReasonOnHold
#
# from Candidate c
# left join tblCandidate tc on c.PersonID = tc.CandidateID
# where c.CandidateReasonOnHold NOT in ('Permanent placement by us','Placed by Eclipse')""", engine_mssql.raw_connection())
# cand['candidate_externalid'] = cand['candidate_externalid'].apply(lambda x: str(x) if x else x)
#
# cand_prod = pd.read_sql("""select id, external_id from candidate where deleted_timestamp is null""", engine_postgre_review)
# tem =cand_prod.loc[cand_prod['external_id'].isin(cand['candidate_externalid'])]
# tem['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcand.ddbconn, ['deleted_timestamp'], ['id'], 'candidate', mylog)
#
# # %% delete
# cand = pd.read_sql("""Select c.PersonID as candidate_externalid,
#        c.PersonFirstName,
#        c.PersonSurname,
#        nullif(convert(varchar,c.CandidateOnHold),'') as CandidateOnHold,
#        nullif(convert(varchar,c.CandidateReasonOnHold),'') as CandidateReasonOnHold
#
# from Candidate c
# left join tblCandidate tc on c.PersonID = tc.CandidateID
# where c.CandidateOnHold = 0""", engine_mssql.raw_connection())
# cand['candidate_externalid'] = cand['candidate_externalid'].apply(lambda x: str(x) if x else x)
#
# cand_prod = pd.read_sql("""select id, external_id from candidate where deleted_timestamp is not null""", engine_postgre_review)
# tem =cand_prod.loc[cand_prod['external_id'].isin(cand['candidate_externalid'])]
# tem['deleted_timestamp'] = datetime.datetime(2011, 1, 1)
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcand.ddbconn, ['deleted_timestamp'], ['id'], 'candidate', mylog)

# %% delete
cand = candidate[['candidate_externalid', 'CandidateOnHold','CandidateReasonOnHold']].drop_duplicates()
cand = cand.loc[cand['CandidateOnHold']=='1']
cand = cand.loc[cand['CandidateReasonOnHold'].notnull()]
cand['matcher'] = cand['CandidateReasonOnHold'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

reason = pd.read_csv('CandidateReasonOnHold.csv')
reason['matcher'] = reason['CandidateReasonOnHold'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
tem = cand.merge(reason,on='matcher')

tem['deleted_timestamp'] = datetime.datetime.now()
tem = tem.merge(vcand.candidate, on=['candidate_externalid'])
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcand.ddbconn, ['deleted_timestamp'], ['id'], 'candidate', mylog)

# %% last activity
tem2 = company = pd.read_sql("""
select candidate_id, max(insert_timestamp) as last_activity_date
from activity_candidate
group by candidate_id""", engine_postgre_review)
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcand.ddbconn, ['last_activity_date', ], ['candidate_id', ],'candidate_extension', mylog)