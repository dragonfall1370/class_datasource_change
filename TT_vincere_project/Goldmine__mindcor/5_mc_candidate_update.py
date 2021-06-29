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

from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
assert False
# %%
candidate = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     , c1.CONTACT, c1.COMPANY
     , trim(LASTNAME) as LASTNAME
     , nullif(ADDRESS1,'') as ADDRESS1
     , nullif(ADDRESS2,'') as ADDRESS2
     , nullif(ADDRESS3,'') as ADDRESS3
     , nullif(CITY,'') as CITY
     , nullif(STATE,'') as STATE
     , nullif(ZIP,'') as ZIP
     , nullif(COUNTRY,'') as COUNTRY
     , nullif(SOURCE,'') as SOURCE
     , nullif(TITLE,'') as TITLE
     , nullif(DEAR,'') as DEAR
     , nullif(PHONE1,'') as PHONE1
     , nullif(PHONE2,'') as PHONE2
     , nullif(PHONE3,'') as Mobile
     , nullif(UDISLIKES,'') as UDISLIKES
from CONTACT1 c1
left join USERS u on u.USERNAME = c1.CREATEBY
left join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')
""", engine_mssql)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'STATE', 'ZIP', 'COUNTRY']]
c_location['address'] = c_location[['ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'STATE', 'ZIP', 'COUNTRY']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['address']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid', 'ADDRESS1', 'ADDRESS2', 'ADDRESS3', 'CITY', 'STATE', 'ZIP', 'COUNTRY']].drop_duplicates()\
    .rename(columns={'CITY': 'city', 'STATE': 'state', 'ZIP': 'post_code'})
tem = comaddr[['candidate_externalid', 'city']].dropna()
cp3 = vcand.update_location_city2(tem, dest_db, mylog)
# update state
tem = comaddr[['candidate_externalid', 'state']].dropna()
cp4 = vcand.update_location_state2(tem, dest_db, mylog)
# update postcode
tem = comaddr[['candidate_externalid', 'post_code']].dropna()
cp5 = vcand.update_location_post_code2(tem, dest_db, mylog)
#  update country
from common import vincere_company
vcom = vincere_company.Company(connection)
tem = comaddr[['candidate_externalid','COUNTRY']].dropna()
tem['country_code'] = tem.COUNTRY.map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)

# %%
email = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid, CONTSUPREF from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where c2.CONTACT = 'E-mail Address' 
and KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')
""", engine_mssql)
email['rn'] = email.groupby('candidate_externalid').cumcount()
email = email.loc[email['rn']==0]
email['primary_email'] = email[['CONTSUPREF']]
vcand.update_primary_email(email, mylog)

# %% home phones
home_phone = candidate[['candidate_externalid', 'PHONE2']].drop_duplicates().dropna()
home_phone['home_phone'] = home_phone['PHONE2']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'PHONE1']].drop_duplicates().dropna()
wphone['work_phone'] = candidate['PHONE1']
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile
indt = candidate[['candidate_externalid', 'Mobile']].dropna().drop_duplicates()
indt['mobile_phone'] = indt['Mobile']
cp = vcand.update_mobile_phone(indt, mylog)

# %% primary phones
indt = candidate[['candidate_externalid', 'Mobile']].dropna().drop_duplicates()
indt['primary_phone'] = indt['Mobile']
cp = vcand.update_primary_phone(indt, mylog)

# %% job type
tem1 = candidate[['candidate_externalid', 'SOURCE']].dropna().drop_duplicates()
tem1['SOURCE'].unique()
tem1.loc[tem1['SOURCE']=='Perm', 'desired_job_type'] = 'permanent'
tem1.loc[tem1['SOURCE']=='perm', 'desired_job_type'] = 'permanent'
tem1.loc[tem1['SOURCE']=='Interim', 'desired_job_type'] = 'contract'
tem1.loc[tem1['SOURCE']=='interim', 'desired_job_type'] = 'contract'
tem  = tem1[['candidate_externalid', 'desired_job_type']].dropna().drop_duplicates()
cp = vcand.update_desired_job_type(tem, mylog)

# %% gender
# tem = pd.read_sql("""
# select concat('', Reference) as candidate_externalid
#      , Gender
# from Candidate_Report_1
# """, engine_mssql)
# tem = tem.drop_duplicates()
# tem['Gender'].unique()
# tem.loc[tem['Gender']=='Male', 'male'] = 1
# tem.loc[tem['Gender']=='Female', 'male'] = 0
# tem2 = tem[['candidate_externalid','male']].dropna()
# tem2['male'] = tem2['male'].astype(int)
# cp = vcand.update_gender(tem2, mylog)

# %% source
tem = candidate[['candidate_externalid', 'UDISLIKES']].dropna().drop_duplicates()
tem['source'] = tem['UDISLIKES']
cp = vcand.insert_source(tem)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'COMPANY', 'TITLE']].dropna().drop_duplicates()
cur_emp['current_employer'] = cur_emp['COMPANY'].str.strip()
cur_emp['current_job_title'] = cur_emp['TITLE'].str.strip()
cur_emp['current_job_title'] = cur_emp['current_job_title'].apply(lambda x: x.replace('\\','|'))
cur_emp['current_employer'] = cur_emp['current_employer'].apply(lambda x: x.replace('\\','|'))
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)
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


jobapp = pd.read_sql("""
select ACCOUNTNO as candidate_externalid, name as application_stage from OPMGR where nullif(PRODUCTNAME, '') is null and nullif(NAME, '') is not null
""", engine_mssql)
jobapp = jobapp.groupby('candidate_externalid')['application_stage'].apply(lambda x: ', '.join(x)).reset_index()

note = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     , c1.CONTACT
     , LASTNAME
     , nullif(UPOSTGRAD,'') as UPOSTGRAD
     , nullif(UQUALIFICA,'') as Undergrad
     , nullif(UAGE,'') as UAGE
, nullif(UFEECAT,'') as currentCTC, nullif(UFEERATE,'') as Shares
, nullif(UFEEGUARAN,'') as bonus, nullif(UFEEPAYMEN,'') as other
, nullif(USACITZEN,'') as USACITZEN, nullif(URACE,'') as URACE, nullif(UID,'') as UID
, nullif(UCOMPANY,'') as P_Employer, nullif(UCOMPANY1,'') as P_Employer1, nullif(KEY1,'') as KEY1, nullif(USECBDAY,'') as placed_day
     , nullif(convert(nvarchar(max),NOTES),'') as NOTES

from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')
""", engine_mssql)
tem1 = note[['candidate_externalid','UID']].dropna()
tem1['check'] = tem1['UID'].apply(lambda x: x.isnumeric())
tem1['len_check'] = tem1['UID'].apply(lambda x: len(x))
tem1=tem1.loc[tem1['len_check']==13]
tem1 = tem1.loc[tem1['check']==True]
tem1['dob'] = tem1['UID'].apply(lambda x: x[0:6])
tem1['gender'] = tem1['UID'].apply(lambda x: x[6:10])
tem1.loc[tem1['gender'].astype(int)<5000, 'gender_text'] = 'Female'
tem1.loc[tem1['gender'].astype(int)>=5000, 'gender_text'] = 'Male'
tem = tem1[['candidate_externalid','dob','gender_text']]
note = note.merge(tem, on='candidate_externalid',how='left')

note = note.merge(jobapp, on='candidate_externalid',how='left')
note['placed_day'] = note['placed_day'].astype(str)
note['placed_day'] = note['placed_day'].apply(lambda x: x.replace('NaT',''))
note = note.where(note.notnull(),None)
note['NOTES'] = note['NOTES'].apply(lambda x: html_to_text(x) if x else x)
# note['note'] = note[['UPOSTGRAD', 'Undergrad','UAGE','currentCTC','Shares','bonus','other','USACITZEN','URACE','UID','dob','gender_text','P_Employer','P_Employer1','KEY1', 'placed_day','NOTES','application_stage']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Post Grad Deg', 'Undergrad Deg','Age','Current CTC','Shares','Bonus','Other','SA Citizenship','Race','National ID','DOB','Gender','Perevious Employer','Perevious Employer 1.','Record Type', 'Placed Date','Notes','Job Application Stage'], x) if e[1]]), axis=1)
note['note'] = note[['UID','KEY1','NOTES']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['National ID','Record Type','Notes'], x) if e[1]]), axis=1)

cp7 = vcand.update_note2(note, dest_db, mylog)


tem = tem1[['candidate_externalid','gender_text']].dropna()
tem.loc[tem['gender_text']=='Male', 'male'] = 1
tem.loc[tem['gender_text']=='Female', 'male'] = 0
tem2 = tem[['candidate_externalid','male']].dropna()
tem2['male'] = tem2['male'].astype(int)
cp = vcand.update_gender(tem2, mylog)


from datetime import datetime
dob = tem1[['candidate_externalid','dob']]
dob['dob'] = '19'+dob['dob']

dob['m'] = dob['dob'].apply(lambda x: (x[4:6]))
dob['m'] = dob['m'].astype(int)
tem = dob.loc[dob['m']>=12]
dob = dob.loc[~dob['candidate_externalid'].isin(tem['candidate_externalid'])]
dob = dob.loc[dob['m']>0]

dob['d'] = dob['dob'].apply(lambda x: (x[6:8]))
dob['d'] = dob['d'].astype(int)

te2 = dob.loc[dob['d']>31]
dob = dob.loc[~dob['candidate_externalid'].isin(te2['candidate_externalid'])]

dob['date_of_birth'] = dob['dob'].apply(lambda x: datetime(year=int(x[0:4]), month=int(x[4:6]), day=int(x[6:8])))
vcand.update_dob(dob,mylog)
# %% education
edu = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     , c1.CONTACT
     , LASTNAME
     , nullif(UPOSTGRAD,'') as UPOSTGRAD
     , nullif(UQUALIFICA,'') as Undergrad
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')
""", engine_mssql)
edu['degreeName'] = edu[['UPOSTGRAD', 'Undergrad']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
edu = edu.loc[edu['degreeName']!='']
cp9 = vcand.update_education(edu, mylog)
edu['education_summary'] = edu['degreeName']
vcand.update_education_summary(edu, mylog)

# %% work history
w_his = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid
     , c1.CONTACT
     , LASTNAME
, nullif(UCOMPANY,'') as P_Employer, nullif(UCOMPANY1,'') as P_Employer1
, nullif(UPROLE,'') as UPROLE, nullif(UPROLE1,'') as UPROLE1
from CONTACT1 c1
join CONTACT2 c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
where KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')
""", engine_mssql)
w_his['experience'] = w_his[['P_Employer','UPROLE', 'P_Employer1','UPROLE1']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Previous Employer','Previous Role','Previous Employer1','Previous Role1'], x) if e[1]]), axis=1)
w_his = w_his.loc[w_his['experience']!='']
vcand.update_exprerience_work_history(w_his, mylog)

# %% marital
# tem = candidate_info.query('Detail == "Marital Status"')
# tem.loc[tem['marital']=='Single', 'maritalstatus'] = 1
# tem.loc[tem['marital']=='Married', 'maritalstatus'] = 2
# tem.loc[tem['marital']=='Widowed', 'maritalstatus'] = 4
# tem2 = tem[['candidate_externalid','maritalstatus']].dropna().drop_duplicates()
# tem2['maritalstatus'] = tem2['maritalstatus'].apply(lambda x: int(str(x).split('.')[0]))
# vcand.update_marital_status(tem2, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'DEAR']].dropna().drop_duplicates()
tem['gender_title'] = tem['DEAR']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['candidate_externalid','gender_title']].dropna().drop_duplicates()
cp = vcand.update_gender_title(tem2, mylog)

# %% reg date
# tem = pd.read_sql("""
# select concat('', c.Person_Reference) as candidate_externalid
# ,Diary_Date
# from
# DB_Candidate_Diary_Details c
# where Event_Description = 'Candidate registered                              '
# """, engine_mssql)
# tem = tem.drop_duplicates()
# tem['reg_date'] = pd.to_datetime(tem['Diary_Date'])
# vcand.update_reg_date(tem, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# %% last activity date
# tem = pd.read_sql("""
# select concat('', Person_Reference) as candidate_externalid
#      , max(Created) as Created
# from DB_Candidate_Diary_Details
# group by Person_Reference
# """, engine_mssql)
# tem = tem.drop_duplicates()
# tem['last_activity_date'] = pd.to_datetime(tem['Created'])
# vcand.update_last_activity_date(tem, mylog)

# %% distribution list
data = {'name': ['Off Limit List'], 'owner': ['']}
df = pd.DataFrame(data)
vcand.create_talent_pool(df,mylog)

t_list = pd.read_sql("""
select ACCOUNTNO as candidate_externalid, name as group_name from OPMGR where nullif(PRODUCTNAME, '') is null and nullif(NAME, '') is not null and NAME = '29 Client Off Limits'
""", engine_mssql)
t_list['group_name'] = 'Off Limit List'
vcand.add_candidate_talent_pool(t_list,mylog)

# %% unsub
email = pd.read_sql("""
select c1.ACCOUNTNO as candidate_externalid, CONTSUPREF, UNOMAIL from CONTACT1 c1
left join CONTSUPP c2 on c1.ACCOUNTNO = c2.ACCOUNTNO
left join CONTACT2 c3 on c1.ACCOUNTNO = c3.ACCOUNTNO
where c2.CONTACT = 'E-mail Address' 
and KEY1 in (
'CLIENT/CANDIDATE'
,'ADVERTISEMENT RESPON'
,'CAN'
,'CAND'
,'CANDIADATE'
,'CANDIADTE'
,'CANDIDATE'
,'CANDIDATE -'
,'CANDIDATE / CLIENT'
,'CANDIDATE FOR MINDCO'
,'CANDIDATE/CLIENT'
,'CANDIDATES'
,'CANDIDIDATE'
,'CLIENT/CANDIDATE'
,'CONTRACTOR'
,'EXTRAORDINARY CANDI'
,'EXTRAORDINARY CANDID'
,'FINANCE'
,'IT'
,'JOURNALIST'
,'MAP CANDIDATE'
,'NOT ON THE MARKET'
,'OUTSOURCED RESEARCHE'
,'P'
,'PLACED CANDIDATE'
,'POTENTIAL CANDIDATE'
,'PRIVATE'
,'REFEREE'
,'SOURCE'
,'SOURCING'
,'USEFUL NUMBERS'
,'VERONICA'
,'YCANDIDATE')
""", engine_mssql)
email['rn'] = email.groupby('candidate_externalid').cumcount()
email = email.loc[email['rn']==0]
email['email'] = email[['CONTSUPREF']]
email = email.loc[email['UNOMAIL']=='Yes']
email['subscribed'] = 0
vcand.email_subscribe(email, mylog)

# %% fix email
cand_email = pd.read_sql("""select id, external_id, email from candidate where email like '%@noemail.com%' and deleted_timestamp is null and external_id is not null""", connection)
cand_email['clean'] = cand_email['external_id'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
cand_email['email'] = cand_email['clean']+'@noemail.com'
vincere_custom_migration.psycopg2_bulk_update_tracking(cand_email, connection, ['email'], ['id'], 'candidate', mylog)

pay_email = pd.read_sql("""select id, email from candidate where deleted_timestamp is null and external_id is not null""", connection)
pay_email['payslip_email'] = pay_email['email']
vincere_custom_migration.psycopg2_bulk_update_tracking(pay_email, connection, ['payslip_email'], ['id'], 'candidate', mylog)