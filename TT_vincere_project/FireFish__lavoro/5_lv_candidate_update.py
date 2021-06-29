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
cf.read('lv_config.ini')
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
select p.ID as candidate_externalid
     , p.FirstName
     , p.MiddleName
     , p.Surname
     , p.KnownAs
     , c.Address1
     , c.Address2
     , c.Address3
     , c.Town
     , c.PostCode
     , c.County
     , c.Country
     , c.WebAddress
     , c.PhoneMobile
     , c.PhoneHome
     , c.DateOfBirth
     , c.DrivingLicenceWSIID
     , p.JobTitle
     , com.company_name
     , com.DateFrom
     , com.JobTitle as current_job_title
     , rn.Value as citizenship
     , crt.Name as rate_type
     , c.Reference1
     , c.Reference2
     , c.LookingForRateMin
     , c.RequiredPackage
     , c.CurrentSalary
     , c.LookingForSalaryMin
     , c.PersonalEMail
     , o.WorkEMail as owner
     , n.Value as notice
     , p.DirectDial
     , t.Value as salutation
     , p.UpdatedDate
     , CurrentRate
     , p.DisplayID
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select ce.*, comp.Name as company_name from CandidateEmployment ce left join Company comp on ce.CompanyID = comp.ID) com on p.ID = com.CandidateID
left join Drop_Down___Nationalities rn on rn.ID = c.NationalityWSIID
left join ContractRateType crt on crt.ID = c.ContractRateTypeID
left join Drop_Down___NoticePeriod n on n.ID = NoticeWSIID
left join Drop_Down___Titles t on p.TitleWSIID = t.ID
where c.IsActivated = 1
and p.IsActivated = 0
and c.IsArchived = 0
UNION
select p.ID as candidate_externalid
     , p.FirstName
     , p.MiddleName
     , p.Surname
     , p.KnownAs
     , c.Address1
     , c.Address2
     , c.Address3
     , c.Town
     , c.PostCode
     , c.County
     , c.Country
     , c.WebAddress
     , c.PhoneMobile
     , c.PhoneHome
     , c.DateOfBirth
     , c.DrivingLicenceWSIID
     , p.JobTitle
     , com.company_name
     , com.DateFrom
     , com.JobTitle as current_job_title
     , c.Reference1
     , c.Reference2
     , c.LookingForRateMin
     , rn.Value as citizenship
     , crt.Name as rate_type
     , c.RequiredPackage
     , c.CurrentSalary
     , c.LookingForSalaryMin
     , c.PersonalEMail
     , o.WorkEMail as owner
     , n.Value as notice
     , p.DirectDial
     , t.Value as salutation
     , p.UpdatedDate
     , CurrentRate
     , p.DisplayID
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select ce.*, comp.Name as company_name from CandidateEmployment ce left join Company comp on ce.CompanyID = comp.ID) com on p.ID = com.CandidateID
left join Drop_Down___Nationalities rn on rn.ID = c.NationalityWSIID
left join ContractRateType crt on crt.ID = c.ContractRateTypeID
left join Drop_Down___NoticePeriod n on n.ID = NoticeWSIID
left join Drop_Down___Titles t on p.TitleWSIID = t.ID
where c.IsActivated = 1
and p.IsActivated = 1
and c.IsArchived = 0
""", engine_sqlite)
candidate.sort_values('DateFrom', inplace=True, ascending=False)
candidate['rn'] = candidate.groupby('candidate_externalid').cumcount()
candidate = candidate.loc[candidate['rn'] == 0]
candidate['candidate_externalid'] = candidate['candidate_externalid'].apply(lambda x: str(x) if x else x)

# cand_del = pd.read_sql("""with cand as (
# select
#      ad.CandidateID as candidate_externalid
#      , max(ad.CreatedDate) as max_date
# from ActionDetail ad
# where CandidateID is not null
# group by CandidateID)
# select * from cand where max_date < '2018-01-01T00:00:00.000'""",engine_sqlite)
# cand_del['candidate_externalid'] = cand_del['candidate_externalid'].astype(str)
#
# tem = cand_del.merge(pd.read_sql("""select id, external_id as candidate_externalid from candidate where external_id is not null""",engine_postgre), on=['candidate_externalid'])
# tem['deleted_timestamp'] = datetime.datetime.now()
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem, connection, ['deleted_timestamp'], ['id'], 'candidate', mylog)
# cand_del.loc[~cand_del['candidate_externalid'].isin(tem['candidate_externalid'])]

assert False
# %% job type
# jobtp = candidate[['candidate_externalid', 'ts2__Job_Type__c']].dropna()
# jobtp = jobtp.ts2__Job_Type__c.map(lambda x: x.split(';')) \
#    .apply(pd.Series) \
#    .merge(jobtp[['candidate_externalid']], left_index=True, right_index=True) \
#    .melt(id_vars=['candidate_externalid'], value_name='job_type') \
#    .drop('variable', axis='columns') \
#    .dropna()
#
# jobtp.loc[jobtp['job_type']=='Perm', 'desired_job_type'] = 'permanent'
# jobtp.loc[jobtp['job_type']=='Temp', 'desired_job_type'] = 'contract'
# jobtype = jobtp[['candidate_externalid', 'desired_job_type']].dropna()
# jobtype['desired_job_type'].unique()
# cp = vcand.update_desired_job_type_2(jobtype, mylog)

# %% location name/address
candidate['location_name'] = candidate[['Address1', 'Address2', 'Address3', 'Town', 'County', 'PostCode', 'Country']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate['address'] = candidate.location_name
tem = candidate[['candidate_externalid', 'address', 'location_name']].drop_duplicates()
tem = tem.loc[tem['address'] != '']
cp2 = vcand.insert_common_location_v2(tem, dest_db, mylog)

# update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
comaddr = candidate[['candidate_externalid', 'Town', 'PostCode', 'County', 'Country']].drop_duplicates()\
    .rename(columns={'Town': 'city', 'County': 'state', 'PostCode': 'post_code'})
tem = comaddr[['candidate_externalid', 'city']].drop_duplicates().dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].drop_duplicates().dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].drop_duplicates().dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
tem = comaddr[['candidate_externalid', 'Country']].dropna().drop_duplicates()
tem['country_code'] = tem.Country.map(vcand.get_country_code)
cp6 = vcand.update_location_country_code2(tem, dest_db, mylog)
# vcand.set_all_current_candidate_address_as_mailling_address()

# %% phones
indt = candidate[['candidate_externalid', 'PhoneHome','DirectDial']]
indt['home_phone'] = indt[['PhoneHome','DirectDial']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
indt = indt.loc[indt['home_phone'] !='']
cp = vcand.update_home_phone2(indt, dest_db, mylog)
indt = candidate[['candidate_externalid', 'PhoneMobile']].dropna()
indt['primary_phone'] = indt['PhoneMobile']
indt['mobile_phone'] = indt['PhoneMobile']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)

# %% owner
# owner = candidate[['candidate_externalid','owner']].dropna().drop_duplicates()
# owner['email'] = owner['owner']
# tem2 = owner[['candidate_externalid', 'email']]
# tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid, candidate_owner_json from candidate", vcand.ddbconn), on=['candidate_externalid'])
# tem2.candidate_owner_json.fillna('', inplace=True)
# tem2 = tem2.merge(pd.read_sql("select id as user_account_id, email from user_account", vcand.ddbconn), on='email')
# tem2['candidate_owner_json'] = tem2.user_account_id.map(lambda x: '{"ownerId":"%s"}' % x)
#
# tem2 = tem2.groupby('id').apply(lambda subdf: list(set(subdf.candidate_owner_json))).reset_index().rename(columns={0: 'candidate_owner_json'})
# tem2.candidate_owner_json = tem2.candidate_owner_json.map(lambda x: '[%s]' % ', '.join(x))
# tem2.candidate_owner_json = tem2.candidate_owner_json.astype(str).map(lambda x: x.replace("'", ''))
# vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['candidate_owner_json', ], ['id', ], 'candidate', mylog)
# cp = vcand.update_owner(tem, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'company_name', 'current_job_title', 'rn']]
cur_emp['current_employer'] = cur_emp['company_name']
cur_emp = cur_emp.loc[cur_emp['rn'] == 0]
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'salutation']].dropna().drop_duplicates()
tem['gender_title'] = tem['salutation']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% note
note = pd.read_sql("""
select p.ID as candidate_externalid
     , RecruiterSummary
     , d.Value as drivinglicence
     , RequiredPackage
     , cws.Description as permstatus
     , cws1.Description as contstatus
     , visa_note
     , legal
     , Consented
     , l2.Value as LocationArea
     , l1.Value as Location
     , j1.Value as Discipline
     , j2.Value as Role
     , DisplayID
from Person p
join Candidate c on p.ID = c.ID
left join Drop_Down___DrivingLicence d on d.ID = DrivingLicenceWSIID
left join WorkStatus cws on cws.ID = p.WorkStatusID
left join WorkStatus cws1 on cws1.ID = p.ContractWorkStatusID
left join(
select CandidateID as candidate_externalid, Value as visa_note from CandidateEligibility ce
left join CandidateEligibilityType cet on ce.EligibilityTypeID = cet.ID
left join Drop_Down___VisaEligibility ve on ce.DropDownValueID = ve.ID
where Note = 'Visa') v on v.candidate_externalid = p.ID
left join(
select CandidateID, FriendlyName||' '||Date as legal, Consented from LegalItemCandidateHistory l left join LegalType li on l.LegalItemID = li.ID) l on l.CandidateID = p. ID
left join Drop_Down___Locations l1 on l1.id = LocationWSIID
left join Drop_Down___Locations l2 on l2.id = LocationAreaWSIID
left join Drop_Down___JobCategories j1 on j1.id = PrimaryJobCategoryWSIID
left join Drop_Down___JobCategories j2 on j2.id = SecondaryJobCategoryWSIID
""", engine_sqlite)
note['candidate_externalid'] = note['candidate_externalid'].apply(lambda x: str(x) if x else x)
note.loc[note['Consented'] == '0', 'Consented'] = 'No'
note.loc[note['Consented'] == '1', 'Consented'] = 'Yes'
note['note'] = note[['DisplayID', 'RecruiterSummary', 'drivinglicence', 'RequiredPackage', 'permstatus','contstatus','visa_note','legal','Consented','LocationArea','Location','Discipline','Role']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['lavoro ID', 'RecruiterSummary', 'Driving Licence', 'Required Package', 'Permanent','Contract','Visa','Legal','Consented','Location Area','Location','Discipline','Role'], x) if e[1]]), axis=1)
cp11 = vcand.update_note2(note, dest_db, mylog)

# %% last activity date
tem = candidate[['candidate_externalid', 'UpdatedDate']].dropna()
tem['last_activity_date'] = pd.to_datetime(tem['UpdatedDate'])
vcand.update_last_activity_date(tem, mylog)

# %% dob
# dob = candidate[['candidate_externalid', 'DateOfBirth']].dropna().drop_duplicates()
# dob['date_of_birth'] = dob['DateOfBirth']
# dob['date_of_birth'] = pd.to_datetime(dob['date_of_birth'])
# vcand.update_date_of_birth(dob, mylog)

# %% preferred name
tem = candidate[['candidate_externalid', 'KnownAs']].dropna().drop_duplicates()
tem['preferred_name'] = tem['KnownAs']
vcand.update_preferred_name(tem, mylog)

# %% citizenship
tem = candidate[['candidate_externalid', 'citizenship']].dropna().drop_duplicates()
tem['nationality'] = tem['citizenship'].map(vcand.get_country_code)
vcand.update_nationality(tem, mylog)

# %% current salary
tem = candidate[['candidate_externalid', 'CurrentSalary']].dropna().drop_duplicates()
tem['current_salary'] = tem['CurrentSalary']
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% desire salary
tem = candidate[['candidate_externalid', 'LookingForSalaryMin']].dropna().drop_duplicates()
tem['desire_salary'] = tem['LookingForSalaryMin']
tem['desire_salary'] = tem['desire_salary'].astype(float)
vcand.update_desire_salary(tem, mylog)

# %% other benefits
# tem = candidate[['candidate_externalid', 'RequiredPackage']].dropna().drop_duplicates()
# tem['other_benefits'] = tem['RequiredPackage']
# vcand.update_other_benefits(tem, mylog)

# %% contract rate
tem = candidate[['candidate_externalid', 'CurrentRate']].dropna().drop_duplicates()
tem['contract_rate'] = tem['CurrentRate']
tem.loc[tem['contract_rate']=='Daily', 'contract_rate'] = '0.00'
tem['contract_rate'] = tem['contract_rate'].astype(float)
vcand.update_contract_rate(tem, mylog)

# %% contract interval
tem = candidate[['candidate_externalid', 'rate_type']].dropna().drop_duplicates()
# tem['contract_interval'] = tem['rate_type']
tem.loc[tem['rate_type']=='Daily', 'contract_interval'] = 'daily'
tem.loc[tem['rate_type']=='Hourly', 'contract_interval'] = 'hourly'
tem2 = tem[['candidate_externalid', 'contract_interval']].dropna()
vcand.update_contract_interval(tem2, mylog)

# %% web
tem = candidate[['candidate_externalid', 'WebAddress']].dropna().drop_duplicates()
tem['website'] = tem['WebAddress']
vcand.update_website(tem, mylog)

# %% social
social = pd.read_sql("""
select p.ID as candidate_externalid, detail.*
from Person p
left join (select cd.Detail, cd.PersonID, cdt.Name as type
from ContactDetail cd
left join ContactDetailType cdt on cdt.ID = TypeID) detail on p.ID = detail.PersonID
""", engine_sqlite)
social['candidate_externalid'] = social['candidate_externalid'].apply(lambda x: str(x) if x else x)
social = social.dropna()
social['Detail'] = social['Detail'].apply(lambda x: x[:100])
social['type'].unique()

lk = social.loc[social['type'] == 'LinkedIn']
lk['linkedin'] = lk['Detail']
xing = social.loc[social['type'] == 'Xing']
xing['xing'] = xing['Detail']
skype = social.loc[social['type'] == 'Skype']
skype['skype'] = skype['Detail']
tw = social.loc[social['type'] == 'Twitter']
tw['twitter'] = tw['Detail']
fb = social.loc[social['type'] == 'Facebook']
fb['facebook'] = fb['Detail']

vcand.update_linkedin(lk, mylog)
vcand.update_skype(skype, mylog)
vcand.update_xing(xing, mylog)
vcand.update_facebook(fb, mylog)
vcand.update_twitter(tw, mylog)

# %% industries
# cand = pd.read_sql("""
# select cp.CandidateID as candidate_externalid
#      , i."Level 1" as speciality
# from CandidatePreference cp
# left join rytons_mapping_industries_speciality i on(lower(cp.PrimaryPreferenceWSIID) = lower(i."Primary ID"))""", engine_sqlite)
# cand = cand.dropna()
# cand['candidate_externalid'] = cand['candidate_externalid'].astype(str)
# cand['speciality'].unique()
# cand.loc[cand['speciality'] == 'Planning Consultant']
# cand.loc[cand['speciality'] == 'Planning Consultants', 'speciality'] = 'Planning Consultant'
# cand['name'] = cand['speciality']
# cp8 = vcand.insert_candidate_industry(cand, mylog)

# %% education
# edu = pd.read_sql("""
# select CandidateID as candidate_externalid
# , Establishment
# , Qualification
# , FieldOfStudy as Course
# , MonthStarted
# , YearStarted
# , MonthEnded
# , YearEnded
# , Notes from CandidateEducation""", engine_sqlite)
# edu['candidate_externalid'] = edu['candidate_externalid'].astype(str)
# edu['started'] = edu[['MonthStarted', 'YearStarted']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
# edu['ended'] = edu[['MonthEnded', 'YearEnded']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
# edu['edu'] = edu[['Establishment','Qualification','Course','started','ended','Notes']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Establishment','Qualification','Course','From','To','Activities, Societies and Additional Information'], x) if e[1]]), axis=1)
# edu_1 = edu[['candidate_externalid','edu']]
# edu_1 = edu_1.groupby('candidate_externalid')['edu'].apply(lambda x: '\n\n'.join(x)).reset_index()
# edu_1['edu'] = '---EDUCATION---\n' + edu_1['edu']
# edu_1 = edu_1.rename(columns={'edu':'experience'})

quali = pd.read_sql("""
select CandidateID as candidate_externalid
, ProfessionalBody
, AwardedDate
, ExpiryDate
, Notes
, Qualification
 from CandidateProfessionalQualification""", engine_sqlite)
quali['candidate_externalid'] = quali['candidate_externalid'].astype(str)
quali['quali'] = quali[['ProfessionalBody','Qualification','AwardedDate','ExpiryDate','Notes']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Professional Body','Qualification','Date Awarded','Expiry Date','Additional Notes'], x) if e[1]]), axis=1)
quali_1 = quali[['candidate_externalid','quali']]
quali_1 = quali_1.groupby('candidate_externalid')['quali'].apply(lambda x: '\n\n'.join(x)).reset_index()
quali_1['quali'] = '---Professional Certifications---\n' + quali_1['quali']
quali_1 = quali_1.rename(columns={'quali':'education_summary'})

# memeber = pd.read_sql("""
# select CandidateID as candidate_externalid
# , ProfessionalBody
# , MembershipNumber
#  from CandidateProfessionalMembership""", engine_sqlite)
# memeber['candidate_externalid'] = memeber['candidate_externalid'].astype(str)
# memeber['memeber'] = memeber[['ProfessionalBody','MembershipNumber']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Professional Body','Membership Number'], x) if e[1]]), axis=1)
# memeber_1 = memeber[['candidate_externalid','memeber']]
# memeber_1 = memeber_1.groupby('candidate_externalid')['memeber'].apply(lambda x: '\n\n'.join(x)).reset_index()
# memeber_1['memeber'] = '---Professional Memberships---\n' + memeber_1['memeber']
# memeber_1 = memeber_1.rename(columns={'memeber':'experience'})
# tem = pd.concat([edu_1,quali_1,memeber_1])
# tem['education_summary'] = tem['experience']
vcand.update_education_summary(quali_1, mylog)

# %% lanaguage
# lan = pd.read_sql("""
# select CandidateID as candidate_externalid
# , Language as language
# , Rating as level
# from CandidateLanguage""", engine_sqlite)
# lan['candidate_externalid'] = lan['candidate_externalid'].astype(str)
# lan['language'].unique()
# lan.loc[lan['language']=='Engllish', 'language'] = 'English'
# vcand.update_skill_languages(tem, mylog)

# %% skill
# sk = pd.read_sql("""
# select CandidateID as candidate_externalid, s.Name as skills from CandidateSkill cs
# left join Skill s on s.ID = cs.SkillID""", engine_sqlite)
# sk['candidate_externalid'] = sk['candidate_externalid'].astype(str)
# vcand.update_skills(sk, mylog)

# %% empoyment
employ_his = pd.read_sql("""
select CandidateID as candidate_externalid
     , a.Name as company_name
     , JobTitle
     , MonthFrom
     , YearFrom
     , MonthTo
     , YearTo
     , Responsibilities
     , Achievements from CandidateEmployment ce
left join Company a on a.ID = ce.CompanyID""", engine_sqlite)
employ_his['candidate_externalid'] = employ_his['candidate_externalid'].astype(str)
employ_his['started'] = employ_his[['MonthFrom', 'YearFrom']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
employ_his['ended'] = employ_his[['MonthTo', 'YearTo']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
employ_his['experience'] = employ_his[['company_name','JobTitle','started','ended','Responsibilities','Achievements']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Company Name','Job Title','From','To','Responsibilities','Achievements'], x) if e[1]]), axis=1)
employ_his_1 = employ_his[['candidate_externalid','experience']]
employ_his_1 = employ_his_1.groupby('candidate_externalid')['experience'].apply(lambda x: '\n\n'.join(x)).reset_index()
employ_his_1['experience'] = '--- Employment---\n' + employ_his_1['experience']

ref = pd.read_sql("""
select ID as candidate_externalid, Reference1, Reference2 from Candidate""", engine_sqlite)
ref['candidate_externalid'] = ref['candidate_externalid'].astype(str)
ref['experience'] = ref[['Reference1','Reference2']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Reference 1','Reference 2'], x) if e[1]]), axis=1)
ref = ref.loc[ref['experience']!='']
ref_1 = ref[['candidate_externalid','experience']]
ref_1 = ref_1.groupby('candidate_externalid')['experience'].apply(lambda x: '\n\n'.join(x)).reset_index()
ref_1['experience'] = '--- Reference---\n' + ref_1['experience']
tem = pd.concat([employ_his_1,ref_1])
vcand.update_exprerience_work_history(tem, mylog)

# %% email sub
tem = candidate[['PersonalEMail']].dropna().drop_duplicates()
tem['subscribed'] = 1
tem['email'] = tem['PersonalEMail']
vcand.email_subscribe(tem,mylog)

# %% visa note
# tem = pd.read_sql("""
# select CandidateID as candidate_externalid, Value as visa_note from CandidateEligibility ce
# left join CandidateEligibilityType cet on ce.EligibilityTypeID = cet.ID
# left join Drop_Down___VisaEligibility ve on ce.DropDownValueID = ve.ID
# where Note = 'Visa'""", engine_sqlite)
# tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
# cp = vcand.update_visa_note(tem, dest_db, mylog)

# %% active
tem = pd.read_sql("""
select p.ID as candidate_externalid
     , cws.Description as permstatus
from Person p
join Candidate c on p.ID = c.ID
left join WorkStatus cws on cws.ID = p.WorkStatusID
""", engine_sqlite)
tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
tem['active'] = 1
# tem.loc[tem['permstatus']=='Immediately Available', 'active'] = 1
# tem.loc[tem['permstatus']=='Actively Looking', 'active'] = 1
# tem.loc[tem['permstatus']=='Passively Looking', 'active'] = 0
# tem.loc[tem['permstatus']=='Happy in Current Position', 'active'] = 0
# tem.loc[tem['permstatus']=='Not Interested', 'active'] = 0
# tem2 = tem[['candidate_externalid','active']].dropna().drop_duplicates()
vcand.update_active(tem,mylog)

# %% source
tem = pd.read_sql("""
select PersonID as candidate_externalid, Value as source from PersonMarketing m
left join Drop_Down___CandidateSource cs on m.CandidateSourceWSIID = cs.ID
where CandidateSourceWSIID is not null
""", engine_sqlite)
tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
cp = vcand.insert_source(tem)

# %% notice
tem = candidate[['candidate_externalid','notice']].dropna().drop_duplicates()
tem['notice'].unique()
tem.loc[tem['notice']=='1 Month', 'notice_period'] = 30
tem.loc[tem['notice']=='2 Weeks', 'notice_period'] = 14
tem.loc[tem['notice']=='1 Week', 'notice_period'] = 7
tem2 = tem[['candidate_externalid','notice_period']].dropna().drop_duplicates()
vcand.update_notice_period(tem2, mylog)