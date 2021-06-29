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
cf.read('dj_config.ini')
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

from common import vincere_company
vcom = vincere_company.Company(connection)
from common import vincere_contact
vcont = vincere_contact.Contact(connection)
# %% info
contact = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid
     , c1.CONTACT
     , LASTNAME
     , c.ID as company_externalid
     , nullif(ADDRESS1,'') as ADDRESS1
     , nullif(ADDRESS2,'') as ADDRESS2
     , nullif(ADDRESS3,'') as ADDRESS3
     , nullif(CITY,'') as CITY
     , nullif(STATE,'') as STATE
     , nullif(ZIP,'') as ZIP
     , nullif(COUNTRY,'') as COUNTRY
     , nullif(DEPARTMENT,'') as DEPARTMENT
     , nullif(TITLE,'') as TITLE
     , nullif(DEAR,'') as DEAR
     , nullif(PHONE1,'') as PrimaryPhone
     , nullif(PHONE2,'') as PHONE2
     , nullif(PHONE3,'') as PHONE3
     , CREATEON
     , nullif(SOURCE,'') as SOURCE
from CONTACT1 c1
left join company c on c.COMPANY = c1.COMPANY
where KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
contact['company_externalid'] = contact['company_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% location name/address
contact['location_name'] = contact[['CITY', 'STATE','ZIP','COUNTRY']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['address'] = contact[['ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'STATE','ZIP','COUNTRY']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)

# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address','location_name','contact_externalid','CITY', 'STATE','ZIP','COUNTRY']].drop_duplicates()
comaddr = comaddr.loc[comaddr['company_externalid'].notnull()]
comaddr = comaddr.loc[comaddr['address']!='']


cp1 = vcom.insert_company_location(comaddr, mylog)

# %%
# assign the new addesses to contacts work location
tem2 = comaddr[['contact_externalid', 'company_externalid', 'address']]
cp2 = vcont.insert_contact_work_location(tem2, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'CITY']].dropna().drop_duplicates()
tem['city'] = tem.CITY
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'ZIP']].dropna().drop_duplicates()
tem['post_code'] = tem.ZIP
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'STATE']].dropna().drop_duplicates()
tem['state'] = tem.STATE
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'COUNTRY']].dropna().drop_duplicates()
tem['country_code'] = tem.COUNTRY.map(vcom.get_country_code)
tem['country'] = tem.COUNTRY
tem.loc[tem['country_code']=='']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% location name/address
vcont.set_work_location_by_company_location(mylog)

# %% owner
owner = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid
     ,  c1.KEY4 as owner
from CONTACT1 c1
where KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
owner = owner.drop_duplicates().dropna()
owner = owner.loc[owner['owner']!='']
user = pd.read_csv('user.csv')
owner = owner.merge(user, left_on='owner', right_on='NAME', how='left')
vcont.update_owner(owner, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'TITLE']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['TITLE']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'PrimaryPhone']].dropna().drop_duplicates()
tem['primary_phone'] = tem['PrimaryPhone']
# tem['def'] = tem['primary_phone'].apply(lambda x: x[0:2])
# tem['def2'] = tem['primary_phone'].apply(lambda x: x[0:5])
# tem['def3'] = tem['primary_phone'].apply(lambda x: x[0:6])
# tem1 = tem.loc[tem['def']=='07']
# tem2 = tem.loc[tem['def2']=='+4407']
# tem3 = tem.loc[tem['def3']=='+44 07']
# tem.loc[tem['primary_phone'].str.contains('44')]
vcont.update_primary_phone(tem, mylog)

# %% mobile phone
tem_p = contact[['contact_externalid', 'PrimaryPhone']].dropna().drop_duplicates()
tem_p['mobile_phone'] = tem_p['PrimaryPhone']
tem_p['def'] = tem_p['mobile_phone'].apply(lambda x: x[0:2])
tem_p['def2'] = tem_p['mobile_phone'].apply(lambda x: x[0:5])
tem_p['def3'] = tem_p['mobile_phone'].apply(lambda x: x[0:6])
tem_p1 = tem_p.loc[tem_p['def']=='07']
tem_p2 = tem_p.loc[tem_p['def2']=='+4407']
tem_p3 = tem_p.loc[tem_p['def3']=='+44 07']
tem1 = tem_p1[['contact_externalid','mobile_phone']]

tem_ph2 = contact[['contact_externalid', 'PHONE2']].dropna().drop_duplicates()
tem_ph2['mobile_phone'] = tem_ph2['PHONE2']
tem_ph2['def'] = tem_ph2['mobile_phone'].apply(lambda x: x[0:2])
tem_ph2['def2'] = tem_ph2['mobile_phone'].apply(lambda x: x[0:5])
tem_ph2['def3'] = tem_ph2['mobile_phone'].apply(lambda x: x[0:6])
tem_ph21 = tem_ph2.loc[tem_ph2['def']=='07']
tem_ph22 = tem_ph2.loc[tem_ph2['def2']=='+4407']
tem_ph23 = tem_ph2.loc[tem_ph2['def3']=='+44 07']
tem2 = tem_ph21[['contact_externalid','mobile_phone']]

tem_ph3 = contact[['contact_externalid', 'PHONE3']].dropna().drop_duplicates()
tem_ph3['mobile_phone'] = tem_ph3['PHONE3']
tem_ph3['def'] = tem_ph3['mobile_phone'].apply(lambda x: x[0:2])
tem_ph3['def2'] = tem_ph3['mobile_phone'].apply(lambda x: x[0:5])
tem_ph3['def3'] = tem_ph3['mobile_phone'].apply(lambda x: x[0:6])
tem_ph31 = tem_ph3.loc[tem_ph3['def']=='07']
tem_ph32 = tem_ph3.loc[tem_ph3['def2']=='+4407']
tem_ph33 = tem_ph3.loc[tem_ph3['def3']=='+44 07']
tem3 = tem_ph31[['contact_externalid','mobile_phone']]
tem = pd.concat([tem1, tem2, tem3])
tem = tem.drop_duplicates()
tem = tem.groupby('contact_externalid')['mobile_phone'].apply(','.join).reset_index()
vcont.update_mobile_phone(tem, mylog)

# %% home phone
tem = contact[['contact_externalid', 'PHONE2','PHONE3']].drop_duplicates()
tem['home_phone'] = tem[['PHONE2','PHONE3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
tem = tem.loc[tem['home_phone']!='']
vcont.update_home_phone(tem, mylog)

# %% personal email
# tem = contact[['contact_externalid', 'PersonHomeEMail']].dropna().drop_duplicates()
# tem['personal_email'] = tem['PersonHomeEMail']
# vcont.update_personal_email(tem, mylog)

# %% board
tem = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid
from CONTACT1 c1
where KEY1 in (
'Prospect')
""", engine_mssql)
tem['board'] = 2
tem['status'] = 1
vcont.update_board(tem, mylog)

# %% primary email
email = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid, CONTSUPREF from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where c2.CONTACT = 'E-mail Address'
and KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
email['rn'] = email.groupby('contact_externalid').cumcount()
email = email.loc[email['rn']==0]
email['email'] = email[['CONTSUPREF']]
vcont.update_email(email, mylog)

# %% reg date
tem = contact[['contact_externalid', 'CREATEON']].dropna().drop_duplicates()
tem['reg_date'] = pd.to_datetime(tem['CREATEON'])
vcont.update_reg_date(tem, mylog)

# %% source
tem = contact[['contact_externalid', 'SOURCE']].dropna().drop_duplicates()
# tem['source'] = tem['SOURCE']
src = pd.read_csv('source.csv')
tem = tem.merge(src, left_on='SOURCE', right_on='GM value')
tem['source'] = tem['Vincere value']
vcont.insert_source(tem)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = contact[['contact_externalid', 'DEAR']].dropna().drop_duplicates()
# tem['gender_title'] = tem['DEAR']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
# cp = vcont.update_gender_title(tem2, mylog)

# %% note
note = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid
     , c1.CONTACT
     , LASTNAME
     , nullif(USERDEF01,'') as division
     , nullif(UTWITTER,'') as UTWITTER
     , nullif(ULINKEDIN,'') as ULINKEDIN
     , nullif(KEY1,'') as KEY1
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)

web = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid, CONTSUPREF from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where c2.CONTACT = 'Web Site'
and KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
note =note.merge(web,on='contact_externalid',how='left')
note = note.where(note.notnull(),None)
note['note'] = note[['division','CONTSUPREF','KEY1']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Division','Web Site','Contact Type'], x) if e[1]]), axis=1)
cp7 = vcont.update_note_2(note, dest_db, mylog)

# %% twitter
tem = note[['contact_externalid', 'UTWITTER']].dropna().drop_duplicates()
tem['twitter'] = tem['UTWITTER']
tem['twitter'] = tem['twitter'].apply(lambda x: x.split('/')[-1])
tem['twitter'] = tem['twitter'].apply(lambda x: x.replace('@',''))
tem['twitter'] = 'https://twitter.com/' + tem['twitter']
vcont.update_twitter(tem, mylog)

# %% linkedin
tem = note[['contact_externalid', 'ULINKEDIN']].dropna().drop_duplicates()
tem['linkedin'] = tem['ULINKEDIN']
tem = tem.loc[tem['linkedin']!='YES']
tem = tem.loc[tem['linkedin']!='Yes']
tem = tem.loc[tem['linkedin']!='yes']
tem = tem.loc[tem['linkedin']!='ON SITE REP FOR OMNI']
tem = tem.loc[tem['linkedin']!='Li']
tem = tem.loc[tem['linkedin']!='Lexine Sentance']
tem = tem.loc[tem['linkedin']!='Nicolas Zibell\tChief Commercia']
tem['linkedin'].unique()

tem1 = tem[~tem['linkedin'].str.contains('https://')]
tem2 = tem[tem['linkedin'].str.contains('https://')]
tem3 = tem1[~tem1['linkedin'].str.contains('http://')]
tem4 = tem1[tem1['linkedin'].str.contains('http://')]

tem3['linkedin'] = tem3['linkedin'].apply(lambda x: x.replace('ttps://',''))
tem3['linkedin'] = 'https://' + tem3['linkedin']
tem = pd.concat([tem2,tem3,tem4])
vcont.update_linkedin(tem, mylog)

# %% last activity date
tem = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid,
max(CombinedDate) as last_date
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
CROSS APPLY ( VALUES ( LASTCONTON ), ( LASTDATE ),(LASTATMPON)) AS x ( CombinedDate )
where KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
group by c1.ACCOUNTNO
""", engine_mssql)
tem = tem.drop_duplicates().dropna()
tem['last_activity_date'] = pd.to_datetime(tem['last_date'])
vcont.update_last_activity_date(tem, mylog)


# %% industry
industries = pd.read_csv('industry.csv')
ind = pd.read_sql("""select c1.ACCOUNTNO as contact_externalid, nullif(U_KEY2,'') as industry
from CONTACT1 c1
where KEY1 in (
'Vendor'
,'Suspect'
,'Supplier'
,'Prospect'
,'Moved on'
,'Internal'
,'Community'
,'Closed'
,'Client'
,'Accounts'
,'6 Soul Mate'
,'5 Raving Fan'
,'4 Client'
,'3 Customer'
,'2 Prospect'
,'1 Suspect')
""", engine_mssql)
ind = ind.dropna().drop_duplicates()
ind = ind.merge(industries, left_on='industry', right_on='GM Value')
ind['name'] = ind['Vincere Industry']
cp10 = vcont.insert_contact_industry_subindustry(ind, mylog)