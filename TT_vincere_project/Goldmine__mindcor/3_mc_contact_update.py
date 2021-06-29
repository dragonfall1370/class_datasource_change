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
cf.read('mc_config.ini')
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
     , nullif(PHONE3,'') as Mobile
from CONTACT1 c1
left join company c on c.COMPANY = c1.COMPANY
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')
""", engine_mssql)
contact['company_externalid'] = contact['company_externalid'].apply(lambda x: str(x) if x else x)
assert False
# %% location name/address
contact['address'] = contact[['ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'STATE','ZIP','COUNTRY']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
contact['location_name'] = contact['address']

# %%
# assign contacts's addresses to their companies
comaddr = contact[['company_externalid', 'address','location_name','contact_externalid','CITY', 'STATE','ZIP','COUNTRY']].drop_duplicates()
comaddr = comaddr.loc[comaddr['company_externalid'].notnull()]
comaddr = comaddr.loc[comaddr['address']!='']


cp1 = vcom.insert_company_location(comaddr, mylog)

# %%
# assign the new addesses to contacts work location
# tem2 = comaddr[['contact_externalid', 'company_externalid', 'address']]
# cp2 = vcont.insert_contact_work_location(tem2, mylog)

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

# %% insert department company
tem = contact[['company_externalid', 'DEPARTMENT']].drop_duplicates().dropna()
tem['department_name'] = tem['DEPARTMENT']
cp7 = vcom.insert_department(tem, mylog)

# %% insert department contact
# assign the new addesses to contacts work location
tem2 = contact[['contact_externalid', 'company_externalid', 'DEPARTMENT']].drop_duplicates().dropna()
tem2['department_name'] = tem2['DEPARTMENT']
cp8 = vcont.insert_contact_department(tem2, mylog)

# %% job title
jobtitle = contact[['contact_externalid', 'TITLE']].dropna().drop_duplicates()
jobtitle['job_title'] = jobtitle['TITLE']
vcont.update_job_title2(jobtitle, dest_db, mylog)

# %% primary phone
tem = contact[['contact_externalid', 'PrimaryPhone']].dropna().drop_duplicates()
tem['primary_phone'] = tem['PrimaryPhone']
vcont.update_primary_phone(tem, mylog)

# %% mobile phone
mobile_phone = contact[['contact_externalid', 'Mobile']].dropna().drop_duplicates()
mobile_phone['mobile_phone'] = mobile_phone['Mobile']
vcont.update_mobile_phone(mobile_phone, mylog)

# %% home phone
tem = contact[['contact_externalid', 'PHONE2']].dropna().drop_duplicates()
tem['home_phone'] = tem['PHONE2']
vcont.update_home_phone(tem, mylog)

# %% personal email
# tem = contact[['contact_externalid', 'PersonHomeEMail']].dropna().drop_duplicates()
# tem['personal_email'] = tem['PersonHomeEMail']
# vcont.update_personal_email(tem, mylog)

# %% primary email
external_id = pd.read_sql("""
select external_id from contact where deleted_timestamp is null and external_id is not null and external_id  not like '%DEFAULT%'
""", connection)
external_id['contact_externalid'] = external_id['external_id'].apply(lambda x: x.split('_Mindcor_')[0])


email = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid, CONTSUPREF from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where c2.CONTACT = 'E-mail Address' 
and KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')
""", engine_mssql)
email['rn'] = email.groupby('contact_externalid').cumcount()
email = email.loc[email['rn']==0]
email['email'] = email[['CONTSUPREF']]

tem = email.merge(external_id, on='contact_externalid')
tem['contact_externalid'] = tem['external_id']
tem = tem.loc[tem['contact_externalid'].str.contains('_Mindcor_')]
vcont.update_email(email, mylog)

# %% reg date
# tem = contact[['contact_externalid', 'PersonCreateDate']].dropna().drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['PersonCreateDate'])
# vcont.update_reg_date(tem, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = contact[['contact_externalid', 'DEAR']].dropna().drop_duplicates()
tem['gender_title'] = tem['DEAR']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['contact_externalid','gender_title']].dropna().drop_duplicates()
cp = vcont.update_gender_title(tem2, mylog)

# %% note
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

external_id = pd.read_sql("""
select external_id from contact where deleted_timestamp is null and external_id is not null and external_id  not like '%DEFAULT%'
""", connection)
external_id['contact_externalid'] = external_id['external_id'].apply(lambda x: x.split('_')[0])

note = pd.read_sql("""
select c1.ACCOUNTNO as contact_externalid
     , c1.CONTACT
     , LASTNAME
     , nullif(UCONDIV,'') as UCONDIV
     , nullif(UPAPHONE1,'') as UPAPHONE1
     , nullif(KEY2,'') as KEY2
     , nullif(convert(nvarchar(max),NOTES),'') as NOTES
     , nullif(UAGE,'') as UAGE
     , nullif(SECR,'') as SECR
     , nullif(UID,'') as UID
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'BANKING'
,'CANDIDATE / CLIENT'
,'CANDIDATE/CLIENT'
,'CLIENT'
,'CLIENT /'
,'CLIENT/CANDIDATE'
,'CLIENT/EXTRAORDINARY'
,'COMPETITOR'
,'CONSULTING CLIENT'
,'CONTRACTOR'
,'EXTRAORDINARY CLIEN'
,'EXTRAORDINARY CLIENT'
,'FINANCE'
,'HOTEL'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'POSSIBLE BUSINESS PA'
,'POTENTIAL AQUISITION'
,'POTENTIAL BUSINESS P'
,'POTENTIAL CLIENT'
,'SOURCE'
,'SOURCING')
""", engine_mssql)
# note['contact_externalid'] = note['contact_externalid'].apply(lambda x: x.strip())
tem1 = note[['contact_externalid','UID']].dropna()
tem1['check'] = tem1['UID'].apply(lambda x: x.isnumeric())
tem1['len_check'] = tem1['UID'].apply(lambda x: len(x))
tem1=tem1.loc[tem1['len_check']==13]
tem1 = tem1.loc[tem1['check']==True]
tem1['dob'] = tem1['UID'].apply(lambda x: x[0:6])
tem1['gender'] = tem1['UID'].apply(lambda x: x[6:10])
tem1.loc[tem1['gender'].astype(int)<5000, 'gender_text'] = 'Female'
tem1.loc[tem1['gender'].astype(int)>=5000, 'gender_text'] = 'Male'
tem = tem1[['contact_externalid','dob','gender_text']]
note = note.merge(tem, on='contact_externalid',how='left')
note = note.where(note.notnull(),None)
note['NOTES'] = note['NOTES'].apply(lambda x: html_to_text(x) if x else x)
note['note'] = note[['UCONDIV','UPAPHONE1','KEY2','NOTES','UAGE','SECR','UID','gender_text']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Division','PA Office','Industry','Note','Age','Assistant','National ID','Gender'], x) if e[1]]), axis=1)

tem = note.merge(external_id, on='contact_externalid',how='left')
tem2 = tem[['external_id','note']]
tem2 = tem2.dropna().drop_duplicates()
tem2['contact_externalid'] = tem2['external_id']
tem2 = tem2.loc[tem2['note']!='']
note.loc[note['contact_externalid']=='B5111948979!+_.+_San']
tem2.loc[tem2['contact_externalid']=='B5111948979!+_.+_San']
# note = note.groupby('contact_externalid')['note'].apply(lambda x: '\n--------------------\n '.join(x)).reset_index()
note = note.loc[note['note']!='']
cp7 = vcont.update_note_2(note, dest_db, mylog)

# B5111948979!+_.+_San

from datetime import datetime
dob = tem1[['contact_externalid','dob']]
dob['dob'] = '19'+dob['dob']
dob['date_of_birth'] = dob['dob'].apply(lambda x: datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8])))
vcont.update_dob(dob,mylog)

# %%
cont = pd.read_sql("""select id, company_id from contact where deleted_timestamp is null""", engine_postgre_review)
comp = pd.read_sql("""select id as company_id, insert_timestamp from company where deleted_timestamp is null""", engine_postgre_review)
tem = cont.merge(comp, on='company_id')
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, vcont.ddbconn, ['insert_timestamp'], ['id'], 'contact', mylog)

# %% distribution list
# d_list = pd.read_sql("""
# select concat('', si.Reference) as contact_externalid
#      , nullif(trim(Description),'') as Description
#      , nullif(trim(Email),'') as owner
#      , CASE WHEN Share_Type=1 THEN 'Private' WHEN Share_Type=2 THEN 'Planner' ELSE 'Global' END AS Share_Type
# from Savelist_Item si
# left join Savelist_Header sh on sh.Reference = si.Header_Reference
# left join (
#     SELECT a.Reference,
#            a.Person_Reference,
#            UserName,
#            Password,
#            + ISNULL(NULLIF(RTRIM(Forename) + ' ', ' '), '')
#                + ISNULL(RTRIM(Surname), '') AS Full_Name,
#            Forename,
#            Surname,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Main Business')  as Telephone,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Fax')   as Fax,
#            dbo.GetcontactDetail(a.person_reference, 9, 'Business Email') as Email,
#            SuperUser,
#            a.active
#     FROM Consultant a
#              INNER JOIN Person b
#                         ON a.person_reference = b.reference) o1 on o1.Reference = Consultant_Reference
# where Reference_Type = 5
# and Description is not null
# """, engine_mssql)
# tem = d_list[['Description','owner','Share_Type']].drop_duplicates()
# tem['name'] = tem['Description']
# tem.loc[tem['Share_Type']=='Global', 'share_permission'] = 1
# tem.loc[tem['Share_Type']=='Private', 'share_permission'] = 2
# tem = tem.fillna('')
# vcont.create_distribution_list(tem,mylog)
#
# tem1 = d_list[['Description','contact_externalid']].drop_duplicates()
# tem1['group_name'] = tem1['Description']
# vcont.add_contact_distribution_list(tem1,mylog)