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
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

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

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)


def get_w_email(exp):
    print(exp)
    exp = exp.replace('\'', '"')
    exp = exp.replace('""', '"')
    exp = exp.replace('Clinton.O"Brien@mixpanel.com', 'Clinton.O\'Brien@mixpanel.com')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Work')]
    if df.empty:
        return ''
    return df[['email']].iloc[0,0]

def get_p_phone(exp):
    print(exp)
    exp = exp.replace('\'', '"')
    exp = exp.replace('""', '"')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Main')]
    if df.empty:
        return ''
    return df[['phone']].iloc[0,0]

def get_m_phone(exp):
    print(exp)
    exp = exp.replace('\'', '"')
    exp = exp.replace('""', '"')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Mobile')]
    if df.empty:
        return ''
    return df[['phone']].iloc[0,0]

def get_w_phone(exp):
    print(exp)
    exp = exp.replace('\'', '"')
    exp = exp.replace('""', '"')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Work')]
    if df.empty:
        return ''
    return df[['phone']].iloc[0,0]

def get_h_phone(exp):
    print(exp)
    exp = exp.replace('\'', '"')
    exp = exp.replace('""', '"')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Home')]
    if df.empty:
        return ''
    return df[['phone']].iloc[0,0]


# sal = """{'comp': '120000.0', 'notes': 'OTE 250'}"""
# exp = sal
# exp = exp.replace('\'', '"')
# exp = exp.replace('""', '"')
# eval(exp)['comp']
# return df[['comp']].iloc[0,0]
def get_salary(exp):
    print(exp)
    sal = eval(exp)
    if 'comp' in sal:
        return eval(exp)['comp']
    else:
        return 0

def get_benefits(exp):
    print(exp)
    sal = eval(exp)
    if 'notes' in sal:
        return eval(exp)['notes']
    else:
        return ''

# %%
candidate = pd.read_sql("""
select id, name, desc, location, address as address_line1, city, state, zip, country, created, source, emails, phones, tags, social, comp, skills from people
where types like '%Candidate%'
""", engine_sqlite)
candidate['candidate_externalid'] = candidate['id'].astype(str)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','location', 'address_line1', 'city', 'state', 'zip', 'country']]
c_location['address'] = c_location[['address_line1', 'city', 'state', 'zip', 'country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['location']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid','address_line1', 'city', 'state', 'zip', 'country']].drop_duplicates()\
    .rename(columns={'zip': 'post_code'})

tem = comaddr[['candidate_externalid', 'address_line1']].dropna()
tem['count'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['count']<100]
cp3 = vcand.update_address_line1_2(tem, dest_db, mylog)

# tem = comaddr[['candidate_externalid', 'ADDRESS2','ADDRESS3']].dropna()
# tem['address_line2'] = tem[['ADDRESS2','ADDRESS3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# cp3 = vcand.update_address_line2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
# from common import vincere_company
# vcom = vincere_company.Company(connection)
tem = comaddr[['candidate_externalid','country']].dropna()
tem['country_code'] = tem['country']
tem.loc[(tem['country'] == 'United States'), 'country_code'] = 'US'
cp6 = vcand.update_location_country_code(tem, mylog)

# %% primary phone
cand_pphone = pd.read_sql("""
select * from people_phone where type= 'Main'
""", engine_sqlite)
cand_pphone['candidate_externalid'] = cand_pphone['id'].apply(lambda x: str(x) if x else x)
cand_pphone['rn'] = cand_pphone.groupby('id').cumcount()
cand_pphone = cand_pphone.loc[cand_pphone['rn']==0]
cand_pphone['primary_phone'] = cand_pphone['phone']
cand_pphone['primary_phone'] = cand_pphone['primary_phone'].astype(str)
vcand.update_primary_phone_v2(cand_pphone, dest_db, mylog)

# %% home phones
# tem = candidate[['candidate_externalid', 'phones']].drop_duplicates().dropna()
# tem['home_phone'] = tem['phones'].apply(lambda x: get_h_phone(x) if x else x)
# tem = tem.loc[tem['home_phone']!='']
# tem['home_phone'] = tem['home_phone'].astype(str)
# cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
cand_wphone = pd.read_sql("""
select * from people_phone where type= 'Work'
""", engine_sqlite)
cand_wphone['candidate_externalid'] = cand_wphone['id'].apply(lambda x: str(x) if x else x)
cand_wphone = cand_wphone.groupby('candidate_externalid')['phone'].apply(', '.join).reset_index()
cand_wphone['work_phone'] = cand_wphone['phone'].astype(str)
cp = vcand.update_work_phone(cand_wphone, mylog)

# %% mobile
cand_mphone = pd.read_sql("""
select * from people_phone where type= 'Mobile'
""", engine_sqlite)
cand_mphone['candidate_externalid'] = cand_mphone['id'].apply(lambda x: str(x) if x else x)
cand_mphone = cand_mphone.groupby('candidate_externalid')['phone'].apply(', '.join).reset_index()
cand_mphone['mobile_phone'] = cand_mphone['phone'].astype(str)
cp = vcand.update_mobile_phone(cand_mphone, mylog)

# %% work email
wemail = pd.read_sql("""
select * from people_emails where email_type = 'Work'
""", engine_sqlite)
wemail = wemail.drop_duplicates()
wemail['candidate_externalid'] = wemail['people_id'].apply(lambda x: str(x) if x else x)
wemail = wemail.groupby('candidate_externalid')['email'].apply(', '.join).reset_index()
wemail['work_email'] = wemail['email'].astype(str)
wemail['len'] = wemail['work_email'].apply(lambda x: len(x))
wemail = wemail.loc[wemail['len']<151]
cp = vcand.update_work_email(wemail, mylog)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = candidate[['candidate_externalid', 'TITLE']].dropna().drop_duplicates()
# tem['gender_title'] = tem['TITLE']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# tem2 = tem[['candidate_externalid','gender_title']].dropna().drop_duplicates()
# cp = vcand.update_gender_title(tem2, mylog)

# %% keywords
tem = candidate[['candidate_externalid', 'tags']].dropna().drop_duplicates()
tem['keyword'] = tem['tags']
tem['keyword'] = tem['keyword'].apply(lambda x: x.replace('[','').replace(']','').replace('\'',''))
vcand.update_keyword(tem, mylog)

# %% skills
tem = candidate[['candidate_externalid', 'skills']].dropna().drop_duplicates()
vcand.update_skills2(tem, dest_db, mylog)

# %% source
tem = candidate[['candidate_externalid', 'source']].dropna().drop_duplicates()
cp = vcand.insert_source(tem)

# %% reg date
tem = candidate[['candidate_externalid', 'created']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['created'])
vcand.update_reg_date(tem, mylog)

# %% current employer
exp = pd.read_sql("""select * from people_experience""",engine_sqlite)
exp = exp.loc[exp['id'].isin(candidate['candidate_externalid'])]
exp['candidate_externalid'] = exp['id'].astype(str)
exp['current_job_title'] = exp['title'].str.strip()
exp['current_employer'] = exp['company'].str.strip()
exp['company'] = exp['desc'].str.strip()
exp.loc[(exp['current'] == 'true'), 'cbEmployer'] = '1'
exp = exp.where(exp.notnull(),None)
# exp['from_month'] = exp['from_month'].fillna(1)
# exp['to_month'] = exp['to_month'].fillna(1)
# exp['from'] = exp['from_month'].astype(str)+'/1/'+exp['from_year'].astype(str)
# exp['to'] = exp['to_month'].astype(str)+'/1/'+exp['to_year'].astype(str)
# exp['dateRangeFrom'] = pd.to_datetime(exp['from'], format='%m/%d/%Y')
# exp['dateRangeTo'] = pd.to_datetime(exp['to'], format='%m/%d/%Y')

tem = exp[['candidate_externalid','current_job_title','current_employer','company','cbEmployer']]
vcand.update_candidate_current_employer_v3(tem, dest_db, mylog)

# tem2 = exp[['candidate_externalid','desc']].dropna()
# tem2['experience'] = tem2['desc']
# tem2['experience'] = '--------------------------\n'+tem2['experience']
# vcand.update_exprerience_work_history2(tem2, dest_db, mylog)

# %% education
edu = pd.read_sql("""
select * from people_education
""", engine_sqlite)
edu['candidate_externalid'] = edu['id'].astype(str)
edu['education_summary'] = edu[['type', 'desc']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Type', 'Description'], x) if e[1]]), axis=1)
cp9 = vcand.update_education_summary_v2(edu, dest_db, mylog)

# %% linkedin
tem = candidate[['candidate_externalid', 'social']].dropna().drop_duplicates()
tem['linkedin'] = tem['social'].apply(lambda x: x.replace('[','').replace(']','').replace('\'',''))

tem = tem.linkedin.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(candidate[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='linkedin') \
    .drop('variable', axis='columns') \
    .dropna()
tem = tem.loc[tem['linkedin'].str.contains('linkedin')]
vcand.update_linkedin(tem, mylog)

# %% salary
tem = candidate[['candidate_externalid', 'comp']].dropna().drop_duplicates()
tem['current_salary'] = tem['comp'].apply(lambda x: get_salary(x) if x else x)
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% benefits
tem = candidate[['candidate_externalid', 'comp']].dropna().drop_duplicates()
tem['other_benefits'] = tem['comp'].apply(lambda x: get_benefits(x) if x else x)
tem['other_benefits'] = tem['other_benefits'].astype(str)
tem = tem.loc[tem['other_benefits']!='']
vcand.update_other_benefits(tem, mylog)

# %% desired salary
tem = candidate[['candidate_externalid', 'comp']].dropna().drop_duplicates()
tem['other_benefits'] = tem['comp'].apply(lambda x: get_benefits(x) if x else x)
tem['desire_salary'] = tem['other_benefits'].astype(str)
tem = tem.loc[tem['other_benefits'].str.contains('Desired Salary')]
tem['desire_salary'] = tem['desire_salary'].apply(lambda x: x.replace('Desired Salary: ',''))
tem['desire_salary'] = tem['desire_salary'].astype(float)
vcand.update_desire_salary(tem, mylog)

# %% note
email = pd.read_sql("""select email from candidate where deleted_timestamp is null and email is not null""", engine_postgre_review)
phone = pd.read_sql("""select phone from candidate where deleted_timestamp is null and phone is not null""", engine_postgre_review)
tem = pd.read_sql("""
select * from people_emails
where email_type in ('Main'
,'Home'
,'Alternate'
,'Corporate'
,'Other')

""", engine_sqlite)
tem.loc[tem['people_id']=='24595570']
tem.loc[tem['candidate_externalid']=='24595570']
note.loc[note['candidate_externalid']=='24595570']
tem = tem.loc[~tem['email'].isin(email['email'])]
tem['candidate_externalid'] = tem['people_id'].apply(lambda x: str(x) if x else x)
tem['emails'] = tem['email_type']+': '+tem['email']
tem = tem.groupby(['candidate_externalid'])['emails'].apply(lambda x: ', '.join(x)).reset_index()

tem2 = pd.read_sql("""
select * from people_phone
where type in ('Main'
,'Alternate'
,'Personal'
,'Corporate (Direct)'
,'Other'
)
""", engine_sqlite)
tem2 = tem2.loc[~tem2['phone'].isin(phone['phone'])]
tem2['candidate_externalid'] = tem2['id'].apply(lambda x: str(x) if x else x)
tem2['phones'] = tem2['type']+': '+tem2['phone']
tem2 = tem2.groupby(['candidate_externalid'])['phones'].apply(lambda x: ', '.join(x)).reset_index()

note = candidate[['candidate_externalid', 'desc']].dropna()
note['desc'] = note['desc'].apply(lambda x: html_to_text(x))
note = note.merge(tem, on ='candidate_externalid', how='left')
note = note.merge(tem2, on ='candidate_externalid', how='left')
note = note.where(note.notnull(),None)
note['note'] = note[['desc', 'emails','phones']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Desc', 'Emails', 'Phones'], x) if e[1]]), axis=1)
# note['note'] = note[['candidate_externalid', 'NATIONALITY', 'NATIONALITY2','COUNTRY OF RESIDENCE','FIRCROFT PLACEMENT']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['RMSCANDIDATEID', 'NATIONALITY', 'NATIONALITY2','COUNTRY OF RESIDENCE','FIRCROFT PLACEMENT'], x) if e[1]]), axis=1)

cp7 = vcand.update_note2(note, dest_db, mylog)

# %% status
# tem = candidate[['candidate_externalid', 'STATUS']].dropna().drop_duplicates()
# tem['name'] = tem['STATUS']
# tem1 = tem[['name']]
# tem1['owner'] = ''
# vcand.create_status_list(tem1, mylog)
# vcand.add_candidate_status(tem, mylog)
tem = pd.read_sql("""select c.id, c2.id as contact_id from candidate c
join contact c2 on c.external_id = c2.external_id
where c.deleted_timestamp is null and c2.deleted_timestamp is null""", engine_postgre_review)
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcand.ddbconn, ['contact_id'], ['id'], 'candidate', mylog)

# %% append source
src = pd.read_csv('source.csv')
src['Source'] = src['Source'].str.strip()

df=src
df.drop_duplicates(inplace=True)
df['lname'] = df['Source'].map(lambda x: str(x).strip().lower() if (str(x) != 'nan') and (x != None) else x)
df['Source'] = df['Source'].map(lambda x: str(x).strip() if (str(x) != 'nan') and (x != None) else x)
# df[colname_source] = df[colname_source].map(lambda x: str(x).strip() if (str(x) != 'nan') and (x != None) else 'Data Import')
# df['lname'] = df[colname_source].map(lambda x: str(x).strip().lower())  # set default source by [Data Import]
# df_cand_source = df.merge(pd.read_sql("select lower(name) as lname, * from candidate_source  where location_id in (select id from location where name='All');", ddbconn), left_on='lname', right_on='lname', how='left')
df_cand_source = df.merge(pd.read_sql("select lower(name) as lname, * from candidate_source;", connection), left_on='lname', right_on='lname', how='left')
#
# only new source names will be inserted
src_names = df_cand_source[df_cand_source['id'].isnull() & df_cand_source['Source'].notnull()]['Source'].unique()
vincere_custom_migration.insert_candidate_source(src_names, connection)

# %% linkedin
internal_note = pd.read_sql("""select * from activities where type = 'Registration Interview'""", engine_sqlite)
internal_note['candidate_externalid'] = internal_note['person'].apply(lambda x: str(x).split('.')[0] if x else x)
internal_note['created'] = internal_note['created'].apply(lambda x: x.replace('T',' ').replace('Z',''))
internal_note['updated'] = internal_note['updated'].apply(lambda x: x.replace('T',' ').replace('Z',''))
internal_note['note'] = internal_note[['notes', 'created','createdBy','updated','updatedBy']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Notes', 'Created','Created By','Updated','Updated By'], x) if e[1]]), axis=1)
internal_note['insert_timestamp'] = datetime.datetime.now()
internal_note['title'] = '❗ Registration Interview'
cp = vcand.insert_internal_note(internal_note, mylog)