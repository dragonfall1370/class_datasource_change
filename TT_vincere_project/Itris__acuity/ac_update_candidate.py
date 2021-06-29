# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
import datetime
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ac_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_candidate
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())

# %% funs

def html_to_text(html):
    from bs4 import BeautifulSoup
    # url = "http://news.bbc.co.uk/2/hi/health/2284783.stm"
    # html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html)

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text

candidate = pd.read_sql("""
select
cand.APP_ID as candidate_externalid
, cand.FIRST_NAME as candidate_firstname
, cand.LAST_NAME as candidate_lastname
, cand.MIDDLE_NAME as candidate_middlename
, cand.EMAIL  as candidate_email
, u.EmailAddress as candidate_owner
, cand.CREATED_ON as reg_date
, cand.NEXT_CALL
, cand.TITLE
, cand.HOME_TEL
, cand.ADDRESS
, cand.POST_CODE
, cand.MOBILE_TEL
, cand.WORK_TEL
, cand.OTHER_TEL
, cand.BIRTH_DATE
, cand.CURRENT_POSITION
, cand.WEB_ADDR
, cand.ABOUT
, cand.APP_TYPE
, cand.NATIONALITY
, cand.NOTICE
, cand.NOTICE_TYP
, cand.CURRENT_EMPLOYER
, cand.CURRENCY
, cand.SALARY
, cand.RATE
, cand.RATE_INTVL
, cand.LAST_CONTACTED
, cand.WARNING as alert
from Applicants cand
left join [User] u on cand.CREATED_BY = u.Id
where cand.DELETED = 0;
""", engine_mssql)

assert False

# %% reg date
vcand.update_reg_date(candidate, mylog)

# %% gender title and gender
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'TITLE']].dropna().drop_duplicates()
tem['gender_title'] = tem['TITLE']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

tem['gender'] = tem.gender_title.map(lambda x: 'female' if x in ['Mrs.', 'Miss.', 'Ms.',] else ('male' if x in ['Mr.',] else None))

tem = tem.loc[tem.gender.notnull()]
tem.gender_title.unique()
tem.gender.unique()
cp = vcand.update_gender(tem, mylog)

# %% address
tem = candidate[['candidate_externalid', 'ADDRESS', 'POST_CODE']]
tem['candidate_externalid'] = tem['candidate_externalid'].astype(str)
tem['address'] = tem[['ADDRESS', 'POST_CODE']].apply(lambda x: ', '.join([e for e in x if e]), axis=1) \
    .map(lambda x: html_to_text(x)).map(lambda x: x.replace('\n', ', ').replace(',,', ',').replace(', ,', ','))
tem['location_name'] = tem['address']
tem['post_code'] = tem['POST_CODE']

cp = vcand.insert_common_location(tem, mylog)

tem2 = candidate[['candidate_externalid', 'POST_CODE']].dropna()
tem2['post_code'] = tem2['POST_CODE']

cp2 = vcand.update_location_post_code(tem2, mylog)

tem3 = candidate[['candidate_externalid']]
tem3['country_code'] = 'ZA'
vcand.update_location_country_code(tem3, mylog)

# %% phones
tem = candidate[['candidate_externalid', 'HOME_TEL', 'MOBILE_TEL', 'WORK_TEL', 'OTHER_TEL']] \
    .rename(columns={'HOME_TEL': 'home_phone', 'MOBILE_TEL': 'mobile_phone', 'WORK_TEL': 'work_phone'})

vcand.update_home_phone(tem, mylog)
vcand.update_mobile_phone(tem, mylog)
tem['primary_phone'] = tem['mobile_phone']
vcand.update_primary_phone(tem, mylog)
vcand.update_work_phone(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'BIRTH_DATE']].rename(columns={'BIRTH_DATE': 'date_of_birth'})
tem['date_of_birth'] = pd.to_datetime(tem['date_of_birth'], errors='coerce').dropna()
vcand.update_date_of_birth(tem, mylog)

# %% current position
tem = candidate[['candidate_externalid', 'CURRENT_POSITION', 'CURRENT_EMPLOYER']].rename(columns={'CURRENT_POSITION': 'current_job_title', 'CURRENT_EMPLOYER': 'current_employer'})
tem.to_csv('cand_pos.csv')
tem = tem.dropna()
tem['current_job_title'] = tem.apply(lambda x: x['current_job_title'].replace('\\', '/'), axis=1)
tem.to_csv('cand_pos.csv')
tem['current_employer'] = tem.apply(lambda x: x['current_employer'].replace('\\', '/'), axis=1)
vcand.update_candidate_current_employer_title(tem, mylog)

# %% website
tem = candidate[['candidate_externalid', 'WEB_ADDR']].rename(columns={'WEB_ADDR': 'website'})
vcand.update_website(tem, mylog)

# %% nation
tem = candidate[['candidate_externalid', 'NATIONALITY']].rename(columns={'NATIONALITY': 'nationality'})
tem.nationality = tem.nationality.map(lambda x: vcand.get_country_code(x))
tem.nationality.unique()
vcand.update_nationality(tem, mylog)

# %% currency
tem = candidate[['candidate_externalid', 'CURRENCY']]
tem['currency_of_salary'] = tem.CURRENCY.map(vcand.map_currency_code)
tem['currency_of_salary'] = 'zar'
vcand.update_currency_of_salary(tem, mylog)

# %% current annual salary
tem = candidate[['candidate_externalid', 'SALARY']]
tem['current_salary'] = tem['SALARY']
vcand.update_current_salary(tem, mylog)

# %% contract interval, contract rate
tem = candidate[['candidate_externalid', 'RATE_INTVL', 'RATE']]
tem.loc[tem.RATE_INTVL==0, 'contract_interval'] = 'hourly'
tem.loc[tem.RATE_INTVL==1, 'contract_interval'] = 'daily'
tem.loc[tem.RATE_INTVL==2, 'contract_interval'] = 'weekly'
tem.loc[tem.RATE_INTVL==3, 'contract_interval'] = 'monthly'
tem.loc[tem.RATE_INTVL==6, 'contract_interval'] = 'fixedfee'
tem.loc[tem.RATE_INTVL==7, 'contract_interval'] = 'annually'
tem.loc[tem.RATE_INTVL == 7]
tem.RATE_INTVL.unique()

tem['contract_rate'] = tem['RATE']
cp1 = vcand.update_contract_rate(tem, mylog)
cp2 = vcand.update_contract_interval(tem, mylog)

# %% email
email = candidate[['candidate_externalid', 'candidate_email']].dropna().rename(columns={'candidate_email': 'email'})
email['email'] = email.apply(lambda x: re.findall(vincere_common.regex_pattern_email, x['email']), axis=1)

email = email.email \
    .apply(pd.Series) \
    .merge(email, left_index=True, right_index=True) \
    .drop('email', axis=1) \
    .melt(id_vars=['candidate_externalid'], value_name='email') \
    .drop('variable', axis=1) \
    .dropna()
email.loc[email.candidate_externalid=='HQ00000251']
email.loc[email.candidate_externalid=='HQ00043445']
email.loc[email.candidate_externalid=='HQ00043478']
email = email.loc[email.email != '']

used_email = pd.read_sql("select id, external_id as candidate_external_id, email from candidate", engine_postgre)
email = email.loc[~email.email.isin(used_email.email)]
email = email.drop_duplicates()
email = email.groupby('candidate_externalid')['email'].apply(', '.join).reset_index().rename(columns={'email': 'work_email'})
vcand.update_work_email(email, mylog)

# %% work type / job type
candidate = pd.read_sql("""
select Id as candidate_externalid, ContractWorkRequired, TemporaryWorkRequired, PermanentWorkRequired from tblvwApplicant;
""", engine_mssql)

candidate = candidate.melt(id_vars=['candidate_externalid'], value_name='value')
candidate.loc[(candidate.variable=='ContractWorkRequired') & (candidate.value==1), 'desired_job_type'] = 'contract'
candidate.loc[(candidate.variable=='PermanentWorkRequired') & (candidate.value==1), 'desired_job_type'] = 'permanent'
candidate.loc[(candidate.variable=='TemporaryWorkRequired') & (candidate.value==1), 'desired_job_type'] = 'temporary'
candidate = candidate.dropna()
cp = vcand.update_desired_job_type(candidate, mylog)

# %% email subscribe
# candidate = pd.read_sql("""
# select RecordId as candidate_externalid, c.EMAIL as email
# from KeywordRecordLink k
# join Applicants c on k.RecordId = c.APP_ID
# where KeywordId in (
#           select DICT_ID from Keywords where KEYWORD like '%UNSUB%'
#           ) and c.EMAIL is not null;
# """, engine_mssql)
# candidate = candidate.email.map(lambda x: x.split('\n')) \
#     .apply(pd.Series) \
#     .merge(candidate, left_index=True, right_index=True) \
#     .drop('email', axis=1) \
#     .melt(id_vars=['candidate_externalid'], value_name='email') \
#     .drop('variable', axis=1) \
#     .dropna()
# candidate['email'] = candidate.apply(lambda x: ','.join(set(re.findall(vincere_common.regex_pattern_email, x['email']))), axis=1)
# candidate.loc[candidate.candidate_externalid=='HQ00000251']
# candidate = candidate.loc[candidate.email != '']
# candidate['subscribed'] = 0
#
# cp3 = vcand.email_subscribe(candidate, mylog)

# %% compliance
# candidate = pd.read_sql("""
# select
# APP_ID as candidate_externalid
# , NI_NUMBER as ni_number
# , COMP_REG_NO as company_number
# , VAT_REGISTERED as vat_registered
# , VAT_REG_NO as vat_number
# , TRADE_NAME as company_name
# , TRADE_ADDRESS
# , TRADE_POST_CODE
# , CreatedDateTime as date_data
# from PersonalDetails;
# """, engine_mssql)
#
# candidate['address'] = candidate[['TRADE_ADDRESS', 'TRADE_POST_CODE']].apply(lambda x: ', '.join([e for e in x if e]), axis=1) \
#     .map(lambda x: html_to_text(x)).map(lambda x: x.replace('\n', ', ').replace(',,', ',').replace(', ,', ','))

# %% National Insurance number
# candidate['country_code'] = 'GB'
# cp4 = vcand.update_onboarding_choose_country(candidate, mylog)
# cp5 = vcand.update_national_insurance_number(candidate)
# cp6 = vcand.insert_onboarding_company_details__company_name(candidate, mylog)
# cp6 = vcand.insert_onboarding_company_details__date_of_incorporation(candidate, mylog)
# cp6 = vcand.insert_onboarding_company_details__vat_registered(candidate, mylog)
# cp6 = vcand.insert_onboarding_company_details__vat_number(candidate, mylog)
# cp6 = vcand.insert_onboarding_company_details__company_number(candidate, mylog)
# cp6 = vcand.insert_onboarding_company_details__address(candidate, mylog)

# %% media to source
# candidate = pd.read_sql("""
# SELECT
# a.ApplicantId as candidate_externalid, m.NAME as source
# FROM tblvwApplicantMedia a
# join Media m on a.MediaId = m.ID
# """, engine_mssql)
# vcand.insert_source(candidate)

# %% note
note = pd.read_sql("""
select a.APP_ID as candidate_externalid, a.OTHER_TEL, a.LAST_CONTACTED, a.MODIFY_ON, a.MODIFY_USER, a.CREATED_ON, a.CREATE_USER, a.APP_TYPE
from Applicants a 
where a.DELETED = 0;
""", engine_mssql)

note.LAST_CONTACTED = note.LAST_CONTACTED.astype(object).where(note.LAST_CONTACTED.notnull(), None)
note.LAST_CONTACTED = note.LAST_CONTACTED.map(lambda x: datetime.datetime.strftime(x, '%d-%b-%Y %H:%M') if x else x)
note.MODIFY_ON = note.MODIFY_ON.dt.strftime('%d-%b-%Y %H:%M')
note.CREATED_ON = note.CREATED_ON.dt.strftime('%d-%b-%Y %H:%M')

note['note'] = note[['candidate_externalid', 'LAST_CONTACTED', 'MODIFY_ON', 'MODIFY_USER', 'CREATED_ON', 'CREATE_USER', 'APP_TYPE']] \
    .apply(lambda x: '\n\n'.join([': '.join(e) for e in zip(['Itris Candidate ID', 'Last Contacted', 'Modified On', 'Modified By',
                                                             'Created On', 'Created By', 'Type'], x) if e[1] and str(e[1]).strip() != '']), axis=1)
note = note[['candidate_externalid', 'note']]
vcand.update_note(note, mylog)

