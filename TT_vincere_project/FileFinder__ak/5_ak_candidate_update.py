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
cf.read('ak_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()


from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

# %%
candidate = pd.read_sql("""
select px.*, e.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.firstname
           , P.lastname
           , p.middlename
           , addressprivatefull
           , t.value as title
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
           , emt.value as emp_type
           , p.salary
           , p.package
           , p.knownas
           , p.phonehome2
           , p.phoneother
           , p.idlocation_string
           , p.dateofbirth
           , p.qualificationvalue_string
           , p.defaulturl
from personx P
left join "user" u on u.iduser = P.iduser
left join preferredemploymenttype emt on emt.idpreferredemploymenttype = P.idpreferredemploymenttype_string
left join title t on t.idtitle = p.idtitle_string
where isdeleted = '0') px on c.idperson = px.candidate_externalid
left join education e on e.idperson = c.idperson
""", engine_postgre_src)
assert False
# %% job type
jobtp = candidate[['candidate_externalid', 'emp_type']].dropna()
jobtp['emp_type'].unique()
jobtp.loc[jobtp['emp_type']=='Permanent', 'desired_job_type'] = 'permanent'
jobtp.loc[jobtp['emp_type']=='Flex', 'desired_job_type'] = 'contract'
jobtype = jobtp[['candidate_externalid', 'desired_job_type']].dropna()
jobtype['desired_job_type'].unique()
cp = vcand.update_desired_job_type_2(jobtype, mylog)

# %% location name/address
tem = candidate[['candidate_externalid', 'addressprivatefull']].dropna()
tem['location_name'] = tem['addressprivatefull'].apply(lambda x: x.replace('\\x0d\\x0a',', '))
tem['address'] = tem.location_name
tem1 = tem[['candidate_externalid', 'address', 'location_name']].drop_duplicates()
cp2 = vcand.insert_common_location_v2(tem1, dest_db, mylog)

# %% phones
home_phone = candidate[['candidate_externalid', 'phonehome', 'phonehome2', 'phoneother']].drop_duplicates()
home_phone['home_phone'] = home_phone[['phonehome', 'phonehome2', 'phoneother']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
home_phone = home_phone.loc[home_phone['home_phone'] != '']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

indt = candidate[['candidate_externalid', 'directlinephone']].dropna()
indt['work_phone'] = indt['directlinephone']
cp = vcand.update_work_phone(indt, mylog)

indt = candidate[['candidate_externalid', 'mobileprivate']].dropna()
indt['mobile_phone'] = indt['mobileprivate']
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)

indt = candidate[['candidate_externalid', 'defaultphone', 'mobileprivate']].drop_duplicates()
indt['primary_phone'] = indt[['defaultphone', 'mobileprivate']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
indt = indt.loc[indt['primary_phone'] != '']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)

# %% emails
mail = candidate[['candidate_externalid', 'emailother', 'emailwork']].drop_duplicates()
mail['work_email'] = mail[['emailother', 'emailwork']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
mail = mail.loc[mail['work_email'] != '']
cp = vcand.update_work_email(mail, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'companyname', 'jobtitle']]
cur_emp['current_employer'] = cur_emp['companyname']
cur_emp['current_job_title'] = cur_emp['jobtitle']
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'title']].dropna().drop_duplicates()
tem['gender_title'] = tem['title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% note
note = pd.read_sql("""
select px.*, po.* , rl.value as relocate
     , pr.value as rating ,ctr.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.firstname
           , P.lastname
           , p.maidenname
           , personreference
           , p.isofflimit
           ,p.idrelocate_string
      , p.idpersonrating_string, p.iscomplete
      , p.originaltraining
      , p.biography
      , p.personcomment
      , p.note as cand_note
from personx P
left join "user" u on u.iduser = P.iduser
left join preferredemploymenttype emt on emt.idpreferredemploymenttype = P.idpreferredemploymenttype_string
left join title t on t.idtitle = p.idtitle_string
where isdeleted = '0') px on c.idperson = px.candidate_externalid
left join (select pol.idperson as id_3, pol.isactive, offlimitdatefrom, offlimitdateto, offlimitnote,  olt.value as offlimittype
from personofflimit pol
left join offlimittype olt on pol.idofflimittype = olt.idofflimittype) po on po.id_3 = px.candidate_externalid
left join relocate rl on rl.idrelocate = px.candidate_externalid
left join personrating pr on pr.idpersonrating = px.candidate_externalid
left join (
select idperson
     , nextavailableon
     , contractoravailabilitycomment
     , cuar.value as unavai_reason, marketrate, rateinformationon, contractorpaymentcomment
from contractor c
left join contractorunavailablereason cuar on c.idcontractorunavailablereason = cuar.idcontractorunavailablereason) ctr on ctr.idperson = px.candidate_externalid
""", engine_postgre_src)

note['biography'] = note['biography'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['personcomment'] = note['personcomment'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['cand_note'] = note['cand_note'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['contractoravailabilitycomment'] = note['contractoravailabilitycomment'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note['contractorpaymentcomment'] = note['contractorpaymentcomment'].apply(lambda x: x.replace('\\x0d\\x0a',', ') if x else x)
note.loc[note['isofflimit'] == '0', 'isofflimit'] = 'No'
note.loc[note['isofflimit'] == '1', 'isofflimit'] = 'Yes'
note.loc[note['isactive'] == '0', 'isactive'] = 'No'
note.loc[note['isactive'] == '1', 'isactive'] = 'Yes'
note.loc[note['iscomplete'] == '0', 'iscomplete'] = 'No'
note.loc[note['iscomplete'] == '1', 'iscomplete'] = 'Yes'

note['note'] = note[['personreference'
    , 'maidenname', 'isofflimit', 'isactive'
    , 'offlimittype', 'offlimitdatefrom', 'offlimitdateto'
    , 'offlimitnote', 'iscomplete', 'relocate', 'rating'
    , 'originaltraining', 'personcomment', 'biography', 'cand_note'
    , 'nextavailableon', 'contractoravailabilitycomment', 'unavai_reason', 'marketrate', 'rateinformationon', 'contractorpaymentcomment']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Reference'
                                                        'Maiden Name', 'Off Limits', ' Off Limits Active'
                                                            , 'Off Limits Type', 'Off Limits Date From', 'Off Limits Date To', 'Off Limits Note'
                                                            , 'Completed', 'Relocate', 'Rating', 'Original Training', 'Internal Comment'
                                                            , 'Biography', 'Notes', 'Next available on (date)', 'Availability Comment', 'Unavailable Reason', 'Market Rate', 'Rate information on (date)'
                                                            , 'Contractor Payment Comment'], x) if e[1]]), axis=1)

cp7 = vcand.update_note2(note, dest_db, mylog)
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

# %% current salary
tem = candidate[['candidate_externalid', 'salary']].dropna().drop_duplicates()
tem['current_salary'] = tem['salary']
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# %% other benefits
tem = candidate[['candidate_externalid', 'package']].dropna().drop_duplicates()
tem['other_benefits'] = tem['package']
vcand.update_other_benefits(tem, mylog)

# %% linkedin
tem = candidate[['candidate_externalid', 'urlprivate']].dropna().drop_duplicates()
tem['website'] = tem['urlprivate']
vcand.update_website(tem, mylog)

# %% industry
sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.idindustry_string_list
from personx P
left join "user" u on u.iduser = P.iduser
left join preferredemploymenttype emt on emt.idpreferredemploymenttype = P.idpreferredemploymenttype_string
left join title t on t.idtitle = p.idtitle_string
where isdeleted = '0') px on c.idperson = px.candidate_externalid
"""
candidate_industries = pd.read_sql(sql, engine_postgre_src)
candidate_industries = candidate_industries.dropna()
industry = candidate_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate_industries[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idindustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idindustry'] = industry['idindustry'].str.lower()

industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)
industry_1 = industry.merge(industries_value, on='idindustry')
industry_1['matcher'] = industry_1['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['INDUSTRY'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
candidate_industries = industry_1.merge(industries_csv, on='matcher')

candidate_industries['name'] = candidate_industries['INDUSTRY']
candidate_industries = candidate_industries.drop_duplicates().dropna()

cp8 = vcand.insert_candidate_industry(candidate_industries, mylog)

# %% education
edu = pd.read_sql("""
select px.*, edu.*
from candidate c
join (select P.idperson as candidate_externalid, p.firstname, p.lastname
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
left join (select e.*, q.value from education e left join qualification q on e.idqualification = q.idqualification) edu on edu.idperson = px.candidate_externalid
""", engine_postgre_src)
edu['schoolName'] = edu['educationestablishment']
edu['degreeName'] = edu['educationsubject']
edu['graduationDate'] = edu['educationto']
edu['startDate'] = edu['educationfrom']
edu['qualification'] = edu['value']
cp9 = vcand.update_education(edu, mylog)

# %% languages
sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid, idlanguage_string_list
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
where idlanguage_string_list is not null
"""
cand_languages = pd.read_sql(sql, engine_postgre_src)
languages = cand_languages.idlanguage_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand_languages[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idlanguage_string') \
    .drop('variable', axis='columns') \
    .dropna()
languages['idlanguage_string'] = languages['idlanguage_string'].str.lower()
idlanguage = pd.read_sql("""
select idlanguage, value from language
""", engine_postgre_src)

language = languages.merge(idlanguage, left_on='idlanguage_string', right_on='idlanguage', how='left')
language['language'] = language['value'].apply(lambda x: x.split(' ')[0])
language['language'].unique()
language['level'] = ''

df = language
logger = mylog

tem2 = df[['candidate_externalid', 'language', 'level']]
tem2.loc[tem2['language']=='Portugese', 'language'] = 'Portuguese'
tem2.loc[tem2['language']=='Chinese Mandarin', 'language'] = 'Chinese (Mandarin/Putonghua)'
tem2.loc[tem2['language']=='Chinese Wu', 'language'] = 'Chinese (Shanghainese)'

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