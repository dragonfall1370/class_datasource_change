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
cf.read('ls_config.ini')
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
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()


from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
candidate = pd.read_sql("""
select px.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.firstname
           , P.lastname
           , p.middlename
           , t.value as title
           , g.Value as gender
           , ms.Value as marital
           , P.createdon
           , p.emailother
           , p.emailwork
           , p.phonehome
           , p.defaultphone
           , p.directlinephone
           , p.mobileprivate
           , p.urlprivate
           , p.jobtitle
           , p.nationalityvalue_string
           , p.salary
           , p.package
           , p.knownas
           , p.phonehome2
           , p.phoneother
           , l.Value as location
           , p.dateofbirth
           , p.qualificationvalue_string
           , p.defaulturl, p.CompanyName, p.PreviousCompany, p.PreviousJobTitle
from personx P
left join Gender g on g.idGender = P.idGender_String
left join title t on t.idtitle = p.idtitle_string
left join MaritalStatus ms on ms.idMaritalStatus = p.idMaritalStatus_String
left join Location l on p.idLocation_String = l.idLocation
where isdeleted = '0') px on c.idperson = px.candidate_externalid
""", engine_sqlite)

cand1 = pd.read_sql("""
select idPerson as candidate_externalid, ea.CommValue as value, cct.Value as type
from Person_EAddress pe
left join PersonCommunicationType cct on cct.idPersonCommunicationType = pe.idPersonCommunicationType
left join EAddress ea on ea.idEAddress = pe.idEAddress
""", engine_sqlite)
cand1['type'].unique()
assert False
# %% owner
owner = pd.read_sql("""
select px.idperson as candidate_externalid
     , px.useremail
     , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
from candidate c
join (select personx.idperson, personx.firstname, personx.lastname, emailprivate, personx.createdon, u.useremail from personx left join "user" u on u.iduser = personx.iduser where isdeleted = '0') px on c.idperson = px.idperson
""", engine_sqlite)
owner['email'] = owner['useremail']
tem2 = owner[['candidate_externalid', 'email']]
tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid, candidate_owner_json from candidate", vcand.ddbconn), on=['candidate_externalid'])
# tem2.loc[tem2['candidate_owner_json']=='']
tem2.candidate_owner_json.fillna('', inplace=True)
tem2 = tem2.merge(pd.read_sql("select id as user_account_id, email from user_account", vcand.ddbconn), on='email')
tem2['candidate_owner_json'] = tem2.user_account_id.map(lambda x: '{"ownerId":"%s"}' % x)

tem2 = tem2.groupby('id').apply(lambda subdf: list(set(subdf.candidate_owner_json))).reset_index().rename(columns={0: 'candidate_owner_json'})
tem2.candidate_owner_json = tem2.candidate_owner_json.map(lambda x: '[%s]' % ', '.join(x))
tem2.candidate_owner_json = tem2.candidate_owner_json.astype(str).map(lambda x: x.replace("'", ''))
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, connection, ['candidate_owner_json', ], ['id', ], 'candidate', mylog)

# vcand.update_owner(owner, mylog)

# %% location name/address
tem = candidate[['candidate_externalid', 'location']].dropna()
tem['location_name'] = tem['location']
tem['address'] = tem.location_name
tem1 = tem[['candidate_externalid', 'address', 'location_name']].drop_duplicates()
cp2 = vcand.insert_common_location_v2(tem1, dest_db, mylog)

# %% country
tem = candidate[['candidate_externalid', 'location']].dropna()
tem['country_code'] = tem['location'].map(vcand.get_country_code)
cp2 = vcand.update_location_country_code(tem, mylog)
# %% home phones
home_phone = candidate[['candidate_externalid', 'phonehome', 'phonehome2', 'phoneother']].drop_duplicates()
home_phone['home_phone'] = home_phone[['phonehome', 'phonehome2', 'phoneother']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
home_phone = home_phone.loc[home_phone['home_phone'] != '']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)
# %% work phones
tem = cand1.loc[cand1['type'] == 'Business']
tem = tem.dropna()
tem.rename(columns={'value': 'business'}, inplace=True)
tem2 = cand1.loc[cand1['type'] == 'Direct Line']
tem2 = tem2.dropna()
tem2.rename(columns={'value': 'd_line'}, inplace=True)
tem3 = cand1.loc[cand1['type'] == 'Switchboard']
tem3 = tem3.dropna()
tem3.rename(columns={'value': 'switch'}, inplace=True)

tem_cand = pd.concat([tem[['candidate_externalid']],tem2[['candidate_externalid']],tem3[['candidate_externalid']]])
tem_cand = tem_cand.drop_duplicates()
tem_cand = tem_cand.merge(tem, on='candidate_externalid', how='left')
tem_cand = tem_cand.merge(tem2, on='candidate_externalid', how='left')
tem_cand = tem_cand.merge(tem3, on='candidate_externalid', how='left')
wphone = tem_cand[['candidate_externalid','business','d_line','switch']]
wphone = wphone.where(wphone.notnull(), None)
wphone['work_phone'] = wphone[['business','d_line','switch']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
wphone['work_phone'] = wphone['work_phone'].apply(lambda x: x[:50])
wphone['count'] = wphone['work_phone'].apply(lambda x: len(x))
wphone.loc[wphone['count'] >= 50]
# tem = wphone.loc[wphone['candidate_externalid'] != '5e646e93-ae77-401b-b8e3-7276a5f6bb7b']
# 49
cp = vcand.update_work_phone(wphone, mylog)

# %% met notmet
cand = pd.read_sql("""
select distinct cand.idperson as candidate_externalid
from activitylogentity ale
join (select px.idperson
     , ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
from candidate c
join (select personx.idperson, personx.createdon from personx where isdeleted = '0') px on c.idperson = px.idperson
) cand on cand.idperson = ale.idPerson
join activitylog al on al.idactivitylog = ale.idactivitylog
LEFT JOIN activitylogcontacttype alct ON alct.idactivitylogcontacttype = al.idactivitylogcontacttype
LEFT JOIN (select * from candidateprogress where isactive = '1') cp ON cp.idcandidateprogress = al.progressid
LEFT JOIN tasklog tl ON al.idactivitylog = tl.idactivitylog
LEFT JOIN task t ON tl.idtask = t.idtask
LEFT JOIN taskstatus ts ON t.idtaskstatus = ts.idtaskstatus
LEFT JOIN taskpriority tp ON t.idtaskpriority = tp.idtaskpriority
left join "user" u on u.fullname = al.createdby
where al.subject like '%F2F Meeting%'
""", engine_sqlite)
cand['status'] = 1
cp = vcand.update_met_notmet(cand, mylog)

# %% met/not met
indt = candidate[['candidate_externalid', 'mobileprivate']].dropna().drop_duplicates()
indt['mobile_phone'] = indt['mobileprivate']
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)

# %% gender
tem = candidate[['candidate_externalid', 'gender']].dropna().drop_duplicates()
tem['gender'].unique()
tem.loc[tem['gender']=='Male', 'male'] = 1
tem.loc[tem['gender']=='Female', 'male'] = 0
tem['male'] = tem['male'].astype(int)
cp = vcand.update_gender(tem, mylog)

# %% primary phones
indt = candidate[['candidate_externalid', 'mobileprivate']].dropna().drop_duplicates()
indt['primary_phone'] = indt['mobileprivate']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)

# %% emails
mail = candidate[['candidate_externalid', 'emailother', 'emailwork']].drop_duplicates()
mail['work_email'] = mail[['emailother', 'emailwork']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
mail = mail.loc[mail['work_email'] != '']
cp = vcand.update_work_email(mail, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'CompanyName', 'jobtitle']]
cur_emp['current_employer'] = cur_emp['CompanyName']
cur_emp['current_job_title'] = cur_emp['jobtitle']
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

# %% current employer 2
cur_emp = candidate[['candidate_externalid', 'PreviousCompany', 'PreviousJobTitle']]
cur_emp['current_employer'] = cur_emp['PreviousCompany']
cur_emp['current_job_title'] = cur_emp['PreviousJobTitle']
cur_emp = cur_emp.loc[cur_emp['current_employer'].notnull()]
cur_emp.loc[cur_emp['candidate_externalid'] == 'ff771e56-7283-4519-91c1-dd7b8a3e5492']
vcand.update_candidate_current_employer2(cur_emp, dest_db, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'title']].dropna().drop_duplicates()
tem['gender_title'] = tem['title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% note
cand_info = pd.read_sql("""
select p.idperson as candidate_externalid
      , p.maidenname
     , ps.Value as status
      , l.Value as location
      , p.createdby
     , pc.value as previous_candidate
from personx p
left join previouscandidate pc on pc.idpreviouscandidate = p.idpreviouscandidate_string
left join personstatus ps on ps.idpersonstatus = p.idpersonstatus_string
left join Location l on l.idLocation = p.idLocation_String""", engine_sqlite)

cand_salary = pd.read_sql("""
select idPerson as candidate_externalid, salary.*
from (select p.idperson, cont.idCompany_Person
from personx p
join
(select cp.idperson, cp.idcompany, cp.idCompany_Person,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
where cont.rn = 1
and p.isdeleted = '0') cp1
join (select r.idCompany_Person
     , r.RemunerationYear
     , r.Salary
     , r.Package
     , r.Bonus
     , r.PackageNote
     , rb.benefit
from Remuneration r
left join (SELECT idRemuneration ,group_concat(benefit) as benefit
FROM (select idRemuneration, Benefit.Value as benefit
from RemunerationBenefit
join Benefit on Benefit.idBenefit = RemunerationBenefit.idBenefit)
GROUP BY idRemuneration) rb on rb.idRemuneration = r.idRemuneration) salary on salary.idCompany_Person = cp1.idCompany_Person
""", engine_sqlite)
cand_info = cand_info.merge(cand_salary, on='candidate_externalid',how ='left')
cand_info = cand_info.where(cand_info.notnull(),None)
cand_info['note'] = cand_info[['MaidenName', 'status'
    , 'RemunerationYear', 'benefit', 'previous_candidate'
    , 'CreatedBy']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Maiden Name', 'Status'
                                                        , 'Salary  ▶  Year', 'Salary  ▶  Benefits', 'Previous Candidate', 'Created By'], x) if e[1]]), axis=1)

cp7 = vcand.update_note2(cand_info, dest_db, mylog)
# %% preferred name
tem = candidate[['candidate_externalid', 'knownas']].dropna().drop_duplicates()
tem['preferred_name'] = tem['knownas']
vcand.update_preferred_name(tem, mylog)

# %% middle name
tem = candidate[['candidate_externalid', 'middlename']].dropna().drop_duplicates()
tem['middle_name'] = tem['middlename']
vcand.update_middle_name(tem, mylog)

# %% reg date
reg_date = candidate[['candidate_externalid', 'createdon']].dropna().drop_duplicates()
reg_date['reg_date'] = pd.to_datetime(reg_date['createdon'])
vcand.update_reg_date(reg_date, mylog)

# %% citizenship
tem = candidate[['candidate_externalid', 'nationalityvalue_string']].dropna().drop_duplicates()
tem['nationality'] = tem['nationalityvalue_string'].map(vcand.get_country_code)
vcand.update_nationality(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'dateofbirth']].dropna().drop_duplicates()
tem = tem.loc[tem['dateofbirth'] != '0001-01-01']
tem['date_of_birth'] = pd.to_datetime(tem['dateofbirth'])
vcand.update_dob(tem, mylog)

# %% other benefits
cand_salary = pd.read_sql("""
select idPerson as candidate_externalid, salary.*
from (select p.idperson, cont.idCompany_Person
from personx p
join
(select cp.idperson, cp.idcompany, cp.idCompany_Person,
       ROW_NUMBER() OVER(PARTITION BY cp.idperson
		ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
from company_person cp
join (select idcompany
from company) c on c.idcompany = cp.idcompany) cont on cont.idperson = p.idperson
left join "user" u on u.iduser = p.iduser
where cont.rn = 1
and p.isdeleted = '0') cp1
join (select r.idCompany_Person
     , r.RemunerationYear
     , r.Salary
     , r.Package
     , r.Bonus
     , r.PackageNote
     , rb.benefit
from Remuneration r
left join (SELECT idRemuneration ,group_concat(benefit) as benefit
FROM (select idRemuneration, Benefit.Value as benefit
from RemunerationBenefit
join Benefit on Benefit.idBenefit = RemunerationBenefit.idBenefit)
GROUP BY idRemuneration) rb on rb.idRemuneration = r.idRemuneration) salary on salary.idCompany_Person = cp1.idCompany_Person
""", engine_sqlite)
cand_salary['PackageNote'] = cand_salary['PackageNote'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
tem = cand_salary[['candidate_externalid', 'PackageNote']].dropna().drop_duplicates()
tem['other_benefits'] = tem['PackageNote']
vcand.update_other_benefits(tem, mylog)

# %% Gross per anum
tem = cand_salary[['candidate_externalid', 'Package']].dropna().drop_duplicates()
tem['total_p_a'] = tem['Package']
tem['total_p_a'] = tem['total_p_a'].astype(float)
vcand.update_total_gross_per_annum(tem, mylog)

# %% current salary
tem = cand_salary[['candidate_externalid', 'Salary']].dropna().drop_duplicates()
tem['current_salary'] = tem['Salary']
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
tem = cand_salary[['candidate_externalid', 'Bonus']].dropna().drop_duplicates()
tem['current_bonus'] = tem['Bonus']
tem['current_bonus'] = tem['current_bonus'].astype(float)
vcand.update_current_bonus(tem, mylog)

# %% linkedin
tem = cand1.loc[cand1['type'] == 'LinkedIn']
tem = tem.dropna()
tem3 = cand1.loc[cand1['type'] == 'URL']
tem3 = tem3.dropna()
tem2 = candidate[['candidate_externalid', 'urlprivate']].dropna().drop_duplicates().rename(columns={'urlprivate': 'value'})
a = tem2.loc[tem2['value'].str.contains('linkedin')]
b = tem.loc[tem['value'].str.contains('linkedin')]
c = tem3.loc[tem3['value'].str.contains('linkedin')]

lik = pd.concat([a,b,c])
lk = lik[['candidate_externalid','value']]
lk['linkedin'] = lk['value']
vcand.update_linkedin(lk, mylog)

# %% industry
sql = """
select P.idperson as candidate_externalid, idIndustry_String_List
               from personx P
where isdeleted = '0'
"""
contact_industries = pd.read_sql(sql, engine_sqlite)
contact_industries = contact_industries.dropna()

industry = contact_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_industries[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idIndustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idIndustry'] = industry['idIndustry'].str.lower()

industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()

industry_1 = industry.merge(industries, on='idIndustry')
industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact_industries = industry_1.merge(industries_csv, on='matcher')

contact_industries_2 = contact_industries[['candidate_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
contact_industries_2 = contact_industries_2.where(contact_industries_2.notnull(),None)


tem1 = contact_industries_2[['candidate_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
cp10 = vcand.insert_candidate_industry_subindustry(tem1, mylog)

tem2 = contact_industries_2[['candidate_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
cp10 = vcand.insert_candidate_industry_subindustry(tem2, mylog)

# %% industry
sql = """
select P.idperson as candidate_externalid, idIndustry_String_List
               from personx P
where isdeleted = '0'
"""
contact_industries = pd.read_sql(sql, engine_sqlite)
contact_industries = contact_industries.dropna()

industry = contact_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(contact_industries[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idIndustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idIndustry'] = industry['idIndustry'].str.lower()

industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()

industry_1 = industry.merge(industries, on='idIndustry')
industry_1['value'] = industry_1[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
contact_industries = industry_1.merge(industries_csv, on='matcher')

contact_industries_2 = contact_industries[['candidate_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
contact_industries_2 = contact_industries_2.where(contact_industries_2.notnull(),None)
tem1 = contact_industries_2[['candidate_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
sql = """
select distinct external_id from candidate c
join (select ci.*
from candidate_industry ci
join vertical v on ci.vertical_id = v.id
where v.parent_id is not null) ci1 on c.id = ci1.candidate_id
where external_id is not null

"""
cand = pd.read_sql(sql, engine_postgre_review)
tem1.loc[~tem1['candidate_externalid'].isin(cand['external_id'])]


cp10 = vcand.insert_candidate_industry_subindustry(tem1, mylog)




tem2 = contact_industries_2[['candidate_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
tem2.loc[~tem2['candidate_externalid'].isin(cand['external_id'])]
cp10 = vcand.insert_candidate_industry_subindustry(tem2, mylog)

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