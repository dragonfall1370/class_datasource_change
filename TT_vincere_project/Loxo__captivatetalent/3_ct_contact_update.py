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

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)

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

def get_p_email(exp):
    exp = exp.replace('\'', '"')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Personal')]
    if df.empty:
        return ''
    return df[['email']].iloc[0,0]

def get_p_phone(exp):
    exp = exp.replace('\'', '"')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Main')]
    if df.empty:
        return ''
    return df[['phone']].iloc[0,0]

def get_m_phone(exp):
    exp = exp.replace('\'', '"')
    df = pd.read_json(exp)
    df = df.loc[(df['type']=='Mobile')]
    if df.empty:
        return ''
    return df[['phone']].iloc[0,0]

# %% info
contact_company = pd.read_csv('ct_contact.csv')
contact_company = contact_company[['id','company_id']].dropna()
contact_company['company_id'] = contact_company['company_id'].apply(lambda x: str(x).split('.')[0])
contact_company['id'] = contact_company['id'].astype(str)

contact = pd.read_sql("""
select id, name, location, address as address_line1, city, state, zip, country, created, source, emails, phones, comp, tags, desc
from people
where types like '%Contact%'
""", engine_sqlite)
contact = contact.merge(contact_company, on='id', how='left')

contact_info = pd.read_sql("""
select id, title from people_experience
""", engine_sqlite)
contact_info['rn'] = contact_info.groupby('id').cumcount()
contact_info = contact_info.loc[contact_info['rn']==0]
contact_info['id'] = contact_info['id'].astype(str)
contact = contact.merge(contact_info, on='id', how='left')

contact['contact_externalid'] = contact['id'].apply(lambda x: str(x) if x else x)
contact['company_externalid'] = contact['company_id'].apply(lambda x: str(x) if x else x)
assert False
# %% location name/address
contact['address'] = contact[['address_line1', 'city','zip','state','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['location_name'] = contact['location']

# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address','location_name','contact_externalid','address_line1', 'city','zip','state','country']].drop_duplicates()
comaddr = comaddr.loc[comaddr['company_externalid'].notnull()]
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %%
# assign the new addesses to contacts work location
tem2 = comaddr[['contact_externalid', 'company_externalid', 'address']]
tem2 = tem2.loc[tem2['address']!='']
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'address_line1']].dropna().drop_duplicates()
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'city']].dropna().drop_duplicates()
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'zip']].dropna().drop_duplicates()
tem['post_code'] = tem['zip']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'state']].dropna().drop_duplicates()
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'country']].dropna().drop_duplicates()
tem['country_code'] = tem['country']
tem.loc[(tem['country'] == 'United States'), 'country_code'] = 'US'
tem = tem.loc[tem['country_code']=='#<Country:0x0000555d741196b8>']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'title']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['title']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% source
tem = contact[['contact_externalid', 'source']].dropna().drop_duplicates()
cp = vcont.insert_source(tem)

# %% primary phone
contact_pphone = pd.read_sql("""
select * from people_phone where type= 'Main'
""", engine_sqlite)
contact_pphone['contact_externalid'] = contact_pphone['id'].apply(lambda x: str(x) if x else x)
contact_pphone['rn'] = contact_pphone.groupby('id').cumcount()
contact_pphone = contact_pphone.loc[contact_pphone['rn']==0]
contact_pphone['primary_phone'] = contact_pphone['phone']
contact_pphone['primary_phone'] = contact_pphone['primary_phone'].astype(str)
vcont.update_primary_phone(contact_pphone, mylog)

# %% mobile phone
contact_mphone = pd.read_sql("""
select * from people_phone where type= 'Mobile'
""", engine_sqlite)
contact_mphone['contact_externalid'] = contact_mphone['id'].apply(lambda x: str(x) if x else x)
contact_mphone = contact_mphone.groupby('contact_externalid')['phone'].apply(', '.join).reset_index()

contact_mphone['mobile_phone'] = contact_mphone['phone'].astype(str)
vcont.update_mobile_phone(contact_mphone, mylog)

# %% p mail
contact_pemail = pd.read_sql("""
select * from people_emails where email_type = 'Personal'
""", engine_sqlite)
contact_pemail['contact_externalid'] = contact_pemail['people_id'].apply(lambda x: str(x) if x else x)
contact_pemail = contact_pemail.groupby(['contact_externalid'])['email'].apply(lambda x: ', '.join(x)).reset_index()
contact_pemail['personal_email'] = contact_pemail['email'].astype(str)
vcont.update_personal_email(contact_pemail, mylog)

# %% reg date
tem = contact[['contact_externalid', 'created']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['created'])
vcont.update_reg_date(tem, mylog)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = contact[['contact_externalid', 'TITLE']].dropna().drop_duplicates()
# tem['gender_title'] = tem['TITLE']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
# cp = vcont.update_gender_title(tem2, mylog)

# %% note
email = pd.read_sql("""select email from contact where deleted_timestamp is null and email is not null""", engine_postgre_review)
phone = pd.read_sql("""select phone from contact where deleted_timestamp is null and phone is not null""", engine_postgre_review)
tem = pd.read_sql("""
select * from people_emails
where email_type in ('Main'
,'Work'
,'Home'
,'Alternate'
,'Corporate'
,'Other')
""", engine_sqlite)
tem = tem.loc[~tem['email'].isin(email['email'])]
tem['contact_externalid'] = tem['people_id'].apply(lambda x: str(x) if x else x)
tem['emails'] = tem['email_type']+': '+tem['email']
tem = tem.groupby(['contact_externalid'])['emails'].apply(lambda x: ', '.join(x)).reset_index()

tem2 = pd.read_sql("""
select * from people_phone
where type in ('Main'
,'Work'
,'Alternate'
,'Personal'
,'Corporate (Direct)'
,'Other'
)
""", engine_sqlite)
tem2 = tem2.loc[~tem2['phone'].isin(phone['phone'])]
tem2['contact_externalid'] = tem2['id'].apply(lambda x: str(x) if x else x)
tem2['phones'] = tem2['type']+': '+tem2['phone']
tem2 = tem2.groupby(['contact_externalid'])['phones'].apply(lambda x: ', '.join(x)).reset_index()

note = contact[['contact_externalid','desc','comp']]
note['comp'] = note['comp'].apply(lambda x: x.replace('[','').replace(']','').replace('\'','') if x else  x)
note['desc'] = note['desc'].apply(lambda x: html_to_text(x) if x else x)
note = note.merge(tem, on ='contact_externalid', how='left')
note = note.merge(tem2, on ='contact_externalid', how='left')
note = note.where(note.notnull(),None)
note['note'] = note[['desc','comp', 'emails','phones']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Desc','Compensation', 'Emails', 'Phones'], x) if e[1]]), axis=1)
# note['note'] = note['Notes']
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% skills
tem = contact[['contact_externalid', 'tags']].dropna().drop_duplicates()
tem['skills'] = tem['tags']
tem['skills'] = tem['skills'].apply(lambda x: x.replace('[','').replace(']','').replace('\'',''))
vcont.update_skills(tem, mylog)
