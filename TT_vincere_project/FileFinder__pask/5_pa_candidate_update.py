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
cf.read('pa_config.ini')
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
select px.*
from candidate c
join (select P.idperson as candidate_externalid
           , P.firstname
           , P.lastname, p.middlename, addressprivatefull, t.value as title
           , P.createdon
           , p.emailother
           , p.emailwork
           , p.phonehome, defaultphone
           , p.directlinephone
           , p.mobileprivate
           , p.urlprivate
           , p.jobtitle, p.nationalityvalue_string, p.companyname, emt.value as emp_type, p.salary, p.package
               from personx P
               left join "user" u on u.iduser = P.iduser
               left join preferredemploymenttype emt on emt.idpreferredemploymenttype = P.idpreferredemploymenttype_string
               left join title t on t.idtitle = p.idtitle_string
where isdeleted = '0') px on c.idperson = px.candidate_externalid
""", engine_postgre_src)
assert False
# %% job type
jobtp = candidate[['candidate_externalid', 'emp_type']].dropna()
jobtp['emp_type'].unique()
jobtp.loc[jobtp['emp_type']=='Permanent', 'desired_job_type'] = 'permanent'
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
indt = candidate[['candidate_externalid', 'phonehome']].dropna()
indt['home_phone'] = indt['phonehome']
cp = vcand.update_home_phone2(indt, dest_db, mylog)
indt = candidate[['candidate_externalid', 'directlinephone']].dropna()
indt['work_phone'] = indt['directlinephone']
cp = vcand.update_work_phone(indt, mylog)
indt = candidate[['candidate_externalid', 'mobileprivate']].dropna()
indt['mobile_phone'] = indt['mobileprivate']
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)
indt = candidate[['candidate_externalid', 'defaultphone']].dropna()
indt['primary_phone'] = indt['defaultphone']
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
select px.*, c2.*
from candidate c
join (select P.idperson as candidate_externalid, p.initials, package, biography, note, personid
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
left join (select idperson, processingreasonvalue, createdon, email, emailtemplate, result, errorcode, errordescription from compliancelog) c2 on c2.idperson = c.idperson
""", engine_postgre_src)

note['compliance'] = note[['processingreasonvalue', 'createdon', 'processingreasonvalue', 'email', 'emailtemplate', 'result', 'errorcode', 'errordescription']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Processing Reason - Current', 'Created on', 'Processing Reason Value',
                                                           'Email','Template','Result','GT Error Code','GT Error Description'], x) if e[1]]), axis=1)
note.loc[note['candidate_externalid'] == 'db002d59-65e8-4043-818e-7e066d450560']
note1 = note[['candidate_externalid', 'initials', 'package', 'biography', 'note', 'personid', 'compliance']]
note2 = note1[['candidate_externalid','compliance']]
note2 = note2.groupby('candidate_externalid')['compliance'].apply('\n\n'.join).reset_index()
note1 = note1.merge(note2, on='candidate_externalid')
note1 = note1.drop_duplicates()

note1['note1'] = note1[['personid', 'initials', 'package', 'biography', 'note']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Pask ID', 'Initials', 'Package',
                                                           'Biography','Note'], x) if e[1]]), axis=1)

note1['note'] = note1['note1'] + '\n\n\n:::::: Compliance ::::::\n' + note1['compliance_y']
cp11 = vcand.update_note2(note1, dest_db, mylog)

internal_note = note1[['candidate_externalid', 'note']]
internal_note['insert_timestamp'] = datetime.datetime.now()
internal_note['title'] = 'PASK INTERNAL NOTES'
vcand.insert_internal_note(internal_note, mylog)
# %% preferred name
tem = candidate[['candidate_externalid', 'KnownAs']].dropna().drop_duplicates()
tem['preferred_name'] = tem['KnownAs']
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
tem['linkedin'] = tem['urlprivate']
vcand.update_linkedin(tem, mylog)

# %% industry
sql = """
select px.*
from candidate c
join (select P.idperson as candidate_externalid, p.idindustry_string_list
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
"""
cand_industries = pd.read_sql(sql, engine_postgre_src)
industry = cand_industries.idindustry_string_list.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(cand_industries[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='idindustry_string') \
    .drop('variable', axis='columns') \
    .dropna()
industries_value = pd.read_sql("""
select idindustry, value from industry
""", engine_postgre_src)

industry_1 = industry.merge(industries_value, left_on='idindustry_string', right_on='idindustry', how='left')

industry_1['name'] = industry_1['value']
industry_1 = industry_1.drop_duplicates().dropna()
cp8 = vcand.insert_candidate_industry(industry_1, mylog)

# %% education
edu = pd.read_sql("""
select px.*, educationto, educationestablishment, educationsubject
from candidate c
join (select P.idperson as candidate_externalid, p.firstname, p.lastname
               from personx P
where isdeleted = '0') px on c.idperson = px.candidate_externalid
left join (select idperson, educationto, educationestablishment, educationsubject from education) e on e.idperson = px.candidate_externalid
""", engine_postgre_src)
edu['schoolName'] = edu['educationestablishment']
edu['degreeName'] = edu['educationsubject']
edu['graduationDate'] = edu['educationto']
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
language['language'] = language['value'].apply(lambda x: x.split('-')[0])
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