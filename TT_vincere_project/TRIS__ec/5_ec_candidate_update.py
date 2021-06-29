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
cf.read('ec_config.ini')
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
       nullif(convert(varchar,c.CandidateWantedJobType),'') as CandidateWantedJobType,
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
       nullif(convert(varchar,c.CandidateCurrentSkills),'') as CandidateCurrentSkills,
       nullif(convert(varchar,c.CandidateClass),'') as CandidateClass,
       nullif(convert(varchar,c.CandidateInfo),'') as CandidateInfo,
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
       nullif(convert(varchar,CurrentPosition),'') as CurrentPosition
from Candidate c
left join tblTitle tT on c.TitleID = tT.TitleID
left join tblPerson p on p.PersonID = c.PersonID
left join Origin o on o.OriginID = c.CandidateOriginID
left join ResidentalStatus r on r.ResidenceID = c.CandidateOriginID
left join tblCandidate tc on c.PersonID = tc.CandidateID
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

# %% personal location name/address
p_location = candidate[['candidate_externalid','CandidatePOAddress', 'CandidatePOAddress2', 'CandidatePOCity', 'CandidatePOState', 'CandidatePOPostcode', 'CandidatePOCountry']]
p_location['address'] = p_location[['CandidatePOAddress', 'CandidatePOAddress2', 'CandidatePOCity', 'CandidatePOState', 'CandidatePOPostcode', 'CandidatePOCountry']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
p_location['location_name'] = p_location['address']
p_location = p_location.loc[p_location['address']!='']
cp2 = vcand.update_personal_location_address_2(p_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = p_location[['candidate_externalid', 'CandidatePOCity', 'CandidatePOState', 'CandidatePOPostcode', 'CandidatePOCountry']].drop_duplicates()\
    .rename(columns={'CandidatePOCity': 'city', 'CandidatePOState': 'state', 'CandidatePOPostcode': 'post_code'})
tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_personal_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_personal_location_state(tem, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_personal_location_post_code(tem, mylog)
#  update country
tem = comaddr[['candidate_externalid', 'CandidatePOCountry']].dropna()
tem['country_code'] = tem.CandidatePOCountry.map(vcand.get_country_code)
cp6 = vcand.update_personal_location_country_code(tem, mylog)

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
cur_emp['current_employer'] = cur_emp['CandidateWorkingAtCompany']
cur_emp['current_job_title'] = cur_emp['CurrentPosition']
# df = cur_emp
# tem2 = df[['candidate_externalid', 'current_employer', 'current_job_title']].fillna('')
# tem2 = tem2.merge(vcand.candidate, left_on='candidate_externalid', right_on='candidate_externalid')
#
# # replace job title
# tem2.loc[tem2['experience_details_json'].isnull(), 'experience_details_json'] = '[{"company":null,"jobTitle":null,"currentEmployer":null,"yearOfExperience":null,"industry":null,"functionalExpertiseId":null,"subFunctionId":null,"cbEmployer":null,"currentEmployerId":null,"dateRangeFrom":null,"dateRangeTo":null}]'
# tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"jobTitle\":\".*?\",|,\"jobTitle\":null,', (',"jobTitle":"%s",' % ('%s'%x['current_job_title']).strip("'")), x['experience_details_json']), axis=1)
# tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"currentEmployer\":null,|,\"currentEmployer\":\".*?\",', (',"currentEmployer":"%s",' % ('%s'%x['current_employer']).strip("'")), x['experience_details_json']), axis=1)
# tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"cbEmployer\":null,|,\"cbEmployer\":\".*?\",', (',"cbEmployer":"%s",' % '1'), x['experience_details_json']), axis=1)
# tem2.loc[tem2['id'] == 63025]
# tem2['candidate_id'] = tem2['id']
# tem2['job_title'] = tem2['current_job_title']
#
# cur_emp.loc[cur_emp['candidate_externalid'] == 'cca824bc-f3d1-4fe2-8772-6384ef45c9ce']
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

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
tem = candidate[['candidate_externalid', 'CandidateReasonOnHold','CandidateInfo']]
tem['note'] = tem[['candidate_externalid', 'CandidateReasonOnHold','CandidateInfo']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID', 'Reason On Hold', 'Information'], x) if e[1]]), axis=1)
cp7 = vcand.update_note2(tem, dest_db, mylog)

# %% work history
wh = pd.read_sql("""
select CandidateID as candidate_externalid
     , nullif(convert(varchar,HFrom),'') as HFrom
     , nullif(convert(varchar,HTo),'') as HTo
     , HEmployer
     , HPosition
     , HComments
     , jt.DisplayName as job_type
from tblHistory h
left join JobType jt on jt.JobTypeID = h.JobTypeID
""", engine_mssql)
wh['candidate_externalid'] = wh['candidate_externalid'].astype(str)
wh['experience'] = wh[['HEmployer', 'HPosition','job_type','HFrom','HTo','HComments']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Employer', 'Position', 'Job Type','From','To','Comments'], x) if e[1]]), axis=1)
cp7 = vcand.update_exprerience_work_history2(wh, dest_db, mylog)

# %% internal note
tem = candidate[['candidate_externalid', 'CandidateCurrentSkills']].dropna()
tem['title'] = 'Current Skills'
tem['insert_timestamp'] = datetime.datetime.now()
tem['note'] = tem['CandidateCurrentSkills']
cp7 = vcand.insert_internal_note(tem, mylog)

# %% job type
jobtp = candidate[['candidate_externalid', 'CandidateWantedJobType']].dropna().drop_duplicates()
jobtp['CandidateWantedJobType'].unique()
jobtp.loc[jobtp['Type']=='Candidate', 'desired_job_type'] = 'permanent'
jobtp.loc[jobtp['Type']=='Contractor', 'desired_job_type'] = 'contract'
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
# tem = candidate[['candidate_externalid','Currency1']].dropna().drop_duplicates()
# tem['Currency1'].unique()
# tem.loc[tem['Currency1']=='GBP', 'currency_of_salary'] = 'pound'
# tem.loc[tem['Currency1']=='USD', 'currency_of_salary'] = 'usd'
# tem.loc[tem['Currency1']=='Euro', 'currency_of_salary'] = 'euro'
# tem2 = tem[['candidate_externalid', 'currency_of_salary']].dropna().drop_duplicates()
# vcand.update_currency_of_salary(tem2, mylog)

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
tem = candidate[['candidate_externalid', 'CandidateClass']].dropna().drop_duplicates()
tem = tem.loc[tem['CandidateClass']=='1']
tem['hot_end_date'] = datetime.datetime.now() + datetime.timedelta(days=999)
# date = date.today() + timedelta(days=10)
vcand.update_make_hot(tem, mylog)

# %% industry
# sql = """
# select P.idperson as candidate_externalid, idIndustry_String_List
#                from personx P
# where isdeleted = '0'
# """
# contact_industries = pd.read_sql(sql, engine_sqlite)
# contact_industries = contact_industries.dropna()
#
# industry = contact_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
#     .apply(pd.Series) \
#     .merge(contact_industries[['candidate_externalid']], left_index=True, right_index=True) \
#     .melt(id_vars=['candidate_externalid'], value_name='idIndustry') \
#     .drop('variable', axis='columns') \
#     .dropna()
# industry['idIndustry'] = industry['idIndustry'].str.lower()
#
# industries = pd.read_sql("""
# select i1.idIndustry, i2.Value as ind, i1.Value as sind
# from Industry i1
# left join Industry i2 on i1.ParentId = i2.idIndustry
# """, engine_sqlite)
# industries['idIndustry'] = industries['idIndustry'].str.lower()
#
# industry_1 = industry.merge(industries, on='idIndustry')
# industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
# industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
#
# industries_csv = pd.read_csv('industries.csv')
# industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# contact_industries = industry_1.merge(industries_csv, on='matcher')
#
# contact_industries_2 = contact_industries[['candidate_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
# contact_industries_2 = contact_industries_2.where(contact_industries_2.notnull(),None)
#
#
# tem1 = contact_industries_2[['candidate_externalid','Vincere Industry']].drop_duplicates().dropna()
# tem1['name'] = tem1['Vincere Industry']
# cp10 = vcand.insert_candidate_industry_subindustry(tem1, mylog)
#
# tem2 = contact_industries_2[['candidate_externalid','Sub Industry']].drop_duplicates().dropna()
# tem2['name'] = tem2['Sub Industry']
# cp10 = vcand.insert_candidate_industry_subindustry(tem2, mylog)
#
# # %% industry
# sql = """
# select P.idperson as candidate_externalid, idIndustry_String_List
#                from personx P
# where isdeleted = '0'
# """
# contact_industries = pd.read_sql(sql, engine_sqlite)
# contact_industries = contact_industries.dropna()
#
# industry = contact_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
#     .apply(pd.Series) \
#     .merge(contact_industries[['candidate_externalid']], left_index=True, right_index=True) \
#     .melt(id_vars=['candidate_externalid'], value_name='idIndustry') \
#     .drop('variable', axis='columns') \
#     .dropna()
# industry['idIndustry'] = industry['idIndustry'].str.lower()
#
# industries = pd.read_sql("""
# select i1.idIndustry, i2.Value as ind, i1.Value as sind
# from Industry i1
# left join Industry i2 on i1.ParentId = i2.idIndustry
# """, engine_sqlite)
# industries['idIndustry'] = industries['idIndustry'].str.lower()
#
# industry_1 = industry.merge(industries, on='idIndustry')
# industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
# industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
#
# industries_csv = pd.read_csv('industries.csv')
# industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
# contact_industries = industry_1.merge(industries_csv, on='matcher')
#
# contact_industries_2 = contact_industries[['candidate_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
# contact_industries_2 = contact_industries_2.where(contact_industries_2.notnull(),None)
# tem1 = contact_industries_2[['candidate_externalid','Vincere Industry']].drop_duplicates().dropna()
# tem1['name'] = tem1['Vincere Industry']
# sql = """
# select distinct external_id from candidate c
# join (select ci.*
# from candidate_industry ci
# join vertical v on ci.vertical_id = v.id
# where v.parent_id is not null) ci1 on c.id = ci1.candidate_id
# where external_id is not null
#
# """
# cand = pd.read_sql(sql, engine_postgre_review)
# tem1.loc[~tem1['candidate_externalid'].isin(cand['external_id'])]
#
#
# cp10 = vcand.insert_candidate_industry_subindustry(tem1, mylog)
#
#
#
#
# tem2 = contact_industries_2[['candidate_externalid','Sub Industry']].drop_duplicates().dropna()
# tem2['name'] = tem2['Sub Industry']
# tem2.loc[~tem2['candidate_externalid'].isin(cand['external_id'])]
# cp10 = vcand.insert_candidate_industry_subindustry(tem2, mylog)

# %% education
edu = pd.read_sql("""
select CandidateID as candidate_externalid,
       nullif(convert(varchar,e.FromDate),'') as FromDate,
       nullif(convert(varchar,e.ToDate),'') as ToDate,
       nullif(convert(varchar,e.OrganisationName),'') as OrganisationName,
       nullif(convert(varchar,e.Degree),'') as Degree,
       nullif(convert(varchar,e.Location),'') as Location,
       nullif(convert(varchar,e.Comments),'') as Comments
from tblEducationHistory e
""", engine_mssql)
edu['candidate_externalid'] = edu['candidate_externalid'].astype(str)
edu['schoolName'] = edu['OrganisationName']
edu['degreeName'] = edu['Degree']
edu['graduationDate'] = pd.to_datetime(edu['ToDate'])
edu['startDate'] = pd.to_datetime(edu['FromDate'])
edu['schoolAddress'] = edu['Location']
edu['description'] = edu['Comments']
edu['description'] = edu['description'].apply(lambda x: x.replace('|','') if x else x)
edu['description'] = edu['description'].apply(lambda x: x.replace('\n',' ') if x else x)
edu['description'] = edu['description'].apply(lambda x: x.replace('\t',' ') if x else x)
cp9 = vcand.update_education(edu, mylog)
tem = edu[['candidate_externalid','Comments']].dropna()
tem['education_summary'] = tem['Comments']
vcand.update_education_summary_v2(tem, dest_db, mylog)

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