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
cf.read('rr_config.ini')
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

# %%
candidate = pd.read_sql("""
select concat('RSS',[Record ID]) as candidate_externalid
     , nullif(Forenames,'') as Forenames
     , nullif(Surname,'') as Surname
     , Title
     , DOB
     , Nationality
     , [Driving Licence]
     , [Address 1]
     , [Address 2]
     , Town
     , County
     , Postcode
     , Country
     , [Phone Home]
     , [Phone Work]
     , [Phone Mobile]
     , [Email (Alternate)]
     , Twitter
     , LinkedIn
     , Status
     , Source
     , [Required Jobs]
     , [Current Full-time/Part-time]
     , [Preferred Contact Time]
from Candidates
""", engine_mssql)
assert False
# %% location name/address
c_location = candidate[['candidate_externalid','Address 1', 'Address 2','Town','County','Postcode','Country']]
c_location['address'] = c_location[['Address 1', 'Address 2','Town','County','Postcode','Country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
c_location['location_name'] = c_location['address']
c_location = c_location.loc[c_location['address']!='']
cp2 = vcand.insert_common_location_v2(c_location, dest_db, mylog)
# update city
vcand = vincere_candidate.Candidate(engine_postgre_review.raw_connection())
comaddr = c_location[['candidate_externalid','Address 1', 'Address 2','Town','County','Postcode','Country']].drop_duplicates()\
    .rename(columns={'Town': 'city', 'County': 'state', 'Postcode': 'post_code', 'Address 1': 'address_line1', 'Address 2': 'address_line2'})

tem = comaddr[['candidate_externalid', 'address_line1']].dropna()
tem['len'] = tem['address_line1'].apply(lambda x: len(x))
tem = tem.loc[tem['len']<101]
cp3 = vcand.update_address_line1_2(tem, dest_db, mylog)

tem = comaddr[['candidate_externalid', 'address_line2']].dropna()
tem['len'] = tem['address_line2'].apply(lambda x: len(x))
tem = tem.loc[tem['len']<101]
cp3 = vcand.update_address_line2_2(tem, dest_db, mylog)

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
tem = comaddr[['candidate_externalid','Country']].dropna()
tem['country_code'] = tem['Country'].map(vcom.get_country_code)
# tem['CandidateCountry'].unique()
cp6 = vcand.update_location_country_code(tem, mylog)
#
# tem = comaddr[['candidate_externalid']]
# tem['location_type'] = 'PERSONAL_ADDRESS'
# vcand.update_location_type(tem, mylog)
# %% home phones
home_phone = candidate[['candidate_externalid', 'Phone Home']].dropna().drop_duplicates()
home_phone['home_phone'] = home_phone['Phone Home']
cp = vcand.update_home_phone2(home_phone, dest_db, mylog)

# %% work phones
wphone = candidate[['candidate_externalid', 'Phone Work']].dropna().drop_duplicates()
wphone['work_phone'] = wphone['Phone Work']
cp = vcand.update_work_phone(wphone, mylog)

# %% mobile/primary phones
mphone = candidate[['candidate_externalid', 'Phone Mobile']].dropna().drop_duplicates()
mphone['mobile_phone'] = mphone['Phone Mobile']
mphone['primary_phone'] = mphone['Phone Mobile']
cp = vcand.update_mobile_phone(mphone, mylog)
cp = vcand.update_primary_phone(mphone, mylog)

# %% knownas
# tem = candidate[['candidate_externalid', 'peo_known']].dropna().drop_duplicates()
# tem['preferred_name'] = tem['peo_known']
# cp = vcand.update_preferred_name(tem, mylog)

# %% web
# tem = candidate[['candidate_externalid', 'peo_www']].dropna().drop_duplicates()
# tem['website'] = tem['peo_www']
# cp = vcand.update_website(tem, mylog)

# %% reg_date
# tem = candidate[['candidate_externalid','peo_reg_date']].dropna().drop_duplicates()
# tem['peo_reg_date'] = tem['peo_reg_date'].astype(str)
# tem['peo_reg_date_1'] = tem['peo_reg_date'].apply(lambda x: x[0:4] if x else x)
# tem['peo_reg_date_2'] = tem['peo_reg_date'].apply(lambda x: x[4:6] if x else x)
# tem['peo_reg_date_3'] = tem['peo_reg_date'].apply(lambda x: x[6:9] if x else x)
# tem['peo_reg_date'] = tem['peo_reg_date_1'] +'-'+tem['peo_reg_date_2']+'-'+tem['peo_reg_date_3']
# tem['reg_date'] = pd.to_datetime(tem['peo_reg_date'], format='%Y/%m/%d %H:%M:%S')
# vcand.update_reg_date(tem, mylog)

# %% dob
tem = candidate[['candidate_externalid','DOB']].dropna().drop_duplicates()
tem['year'] = tem['DOB'].apply(lambda x: x.split('/')[-1])
tem = tem.loc[tem['year']>'1900']
tem = tem.loc[tem['year']<'2021']
# tem = tem.loc[tem['peo_date_birth']!='1085-11-01']
tem['date_of_birth'] = pd.to_datetime(tem['DOB'])
vcand.update_dob(tem, mylog)

# %% job type
# tem = candidate[['candidate_externalid', 'peo_tpb']].dropna().drop_duplicates()
# tem2['peo_tpb'].unique()
# tem1 = tem.loc[tem['peo_tpb']=='BOTH']
# tem2 = tem.loc[tem['peo_tpb']!='BOTH']
# tem2.loc[tem2['peo_tpb']=='TEMP', 'desired_job_type'] = 'temporary'
# tem2.loc[tem2['peo_tpb']=='PERM', 'desired_job_type'] = 'permanent'
# tem2.loc[tem2['peo_tpb']=='CONT', 'desired_job_type'] = 'contract'
#
# tem3 = tem1.copy()
# tem4 = tem1.copy()
# tem3['desired_job_type'] = 'temporary'
# tem4['desired_job_type'] = 'permanent'
# tem = pd.concat([tem2,tem3,tem4])
# cp = vcand.update_desired_job_type(tem, mylog)

# %% nationanlity
tem = candidate[['candidate_externalid', 'Nationality']].dropna().drop_duplicates()
tem['nationality'] = tem['Nationality'].map(vcom.get_country_code)
cp = vcand.update_nationality(tem, mylog)

# %% visa renew date
# tem = candidate[['candidate_externalid', 'peo_visa_expiry']].dropna().drop_duplicates()
# tem = tem.loc[tem['peo_visa_expiry']!=0]
# tem['peo_visa_expiry'] = tem['peo_visa_expiry'].astype(str)
# tem['peo_visa_expiry_1'] = tem['peo_visa_expiry'].apply(lambda x: x[0:4] if x else x)
# tem['peo_visa_expiry_2'] = tem['peo_visa_expiry'].apply(lambda x: x[4:6] if x else x)
# tem['peo_visa_expiry_3'] = tem['peo_visa_expiry'].apply(lambda x: x[6:9] if x else x)
# tem['peo_visa_expiry'] = tem['peo_visa_expiry_1'] +'-'+tem['peo_visa_expiry_2']+'-'+tem['peo_visa_expiry_3']
# tem = tem.loc[tem['peo_visa_expiry_1']>'1900']
# tem = tem.loc[tem['peo_visa_expiry_1']<'2021']
# tem['visa_renewal_date'] = pd.to_datetime(tem['peo_visa_expiry'], format='%Y/%m/%d %H:%M:%S')
# cp = vcand.update_visa_renewal_date(tem, dest_db, mylog)

# %% visa type
# tem = candidate[['candidate_externalid', 'peo_visa_type']].dropna().drop_duplicates()
# tem['visa_type'] = tem['peo_visa_type']
# cp = vcand.update_visa_type(tem, dest_db, mylog)

# %% current employer
cur_emp = pd.read_sql("""
select concat('RSS',Candidate) as candidate_externalid, * from Candidates_History
""", engine_mssql)
cur_emp['current_employer'] = cur_emp['Company'].str.strip()
cur_emp['current_job_title'] = cur_emp['Job Title'].str.strip()
cur_emp['dateRangeFrom'] = pd.to_datetime(cur_emp['From Date'],format='%Y/%m/%d')
cur_emp['dateRangeTo'] = pd.to_datetime(cur_emp['To Date'],format='%Y/%m/%d')
cur_emp['address'] = cur_emp['Address']
cur_emp.loc[cur_emp['Current']=='Yes', 'cbEmployer'] = 1
# cur_emp['company'] = cur_emp[['Phone', 'Email','Checked']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cur_emp['company'] = cur_emp[['Phone', 'Email','Checked']].apply(lambda x: ', '.join([': '.join(e) for e in zip(['Phone', 'Email','Placed by RE'], x) if e[1]]), axis=1)
cur_emp = cur_emp.where(cur_emp.notnull(), None)
vcand.update_candidate_current_employer_v3(cur_emp, dest_db, mylog)

# %% note
note = candidate[['candidate_externalid', 'Driving Licence','Email (Alternate)','Required Jobs','Preferred Contact Time']].drop_duplicates()
note['note'] = note[['Driving Licence','Email (Alternate)','Required Jobs','Preferred Contact Time']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip([' Driving Licence ',' Email Alternate','Related Jobs','Preferred Contact Time'], x) if e[1]]), axis=1)
note = note.loc[note['note']!='']
cp7 = vcand.update_note2(note, dest_db, mylog)

# %% education
# edu = pd.read_sql("""
# select CONCAT('',peo_no) as candidate_externalid
#          , edu_quals
#          , edu_school
#          , edu_from
#          , edu_to
#     from peo_educ
# """, engine_mssql)
#
# edu['edu_from'] = edu['edu_from'].astype(str)
# edu['edu_from_1'] = edu['edu_from'].apply(lambda x: x[0:4] if x else x)
# edu['edu_from_2'] = edu['edu_from'].apply(lambda x: x[4:6] if x else x)
# edu['edu_from_3'] = edu['edu_from'].apply(lambda x: x[6:9] if x else x)
# edu['edu_from'] = edu['edu_from_1'] +'-'+edu['edu_from_2']+'-'+edu['edu_from_3']
#
# edu['edu_to'] = edu['edu_to'].astype(str)
# edu['edu_to_1'] = edu['edu_to'].apply(lambda x: x[0:4] if x else x)
# edu['edu_to_2'] = edu['edu_to'].apply(lambda x: x[4:6] if x else x)
# edu['edu_to_3'] = edu['edu_to'].apply(lambda x: x[6:9] if x else x)
# edu['edu_to'] = edu['edu_to_1'] +'-'+edu['edu_to_2']+'-'+edu['edu_to_3']
# edu['edu_to'] = edu['edu_to'].apply(lambda x: x.replace('0--',''))
# edu['edu_from'] = edu['edu_from'].apply(lambda x: x.replace('0--',''))
#
# edu['education_summary'] = edu[[
#     'edu_from', 'edu_to'
#     ,'edu_school','edu_quals']]\
#      .apply(lambda x: '\n'.join([': '.join(e) for e in zip([
#     'From', 'To'
#     ,'Establihment','Qualifications'], x) if e[1]]), axis=1)
#
# vcand.update_education_summary(edu, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'Title']].dropna().drop_duplicates()
tem['gender_title'] = tem['Title']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
tem2 = tem[['candidate_externalid','gender_title']].dropna().drop_duplicates()
cp = vcand.update_gender_title(tem2, mylog)

# %% currency salary
tem = candidate[['candidate_externalid']]
tem['currency_of_salary'] = 'pound'
vcand.update_currency_of_salary(tem, mylog)

# # %% desire annual salary
# tem = candidate[['candidate_externalid', 'Salary_Amt']].dropna().drop_duplicates()
# tem['desire_salary'] = tem['Salary_Amt'].astype(float)
# vcand.update_desire_salary(tem, mylog)
#
# # %% desire contract rate
# tem = candidate[['candidate_externalid', 'OTE_Amt']].dropna().drop_duplicates()
# tem['desired_contract_rate'] = tem['OTE_Amt'].astype(float)
# vcand.update_desired_contract_rate(tem, mylog)

# %% wh
# wh = pd.read_sql("""
# select CONCAT('',pc.peo_no) as candidate_externalid
#          , emp_from
#          , emp_to
#          , emp_cli_name
#          , emp_job_title
#          , emp_salary
#          , emp_ote
#          , CONCAT(p.peo_forename,' ',p.peo_surname) as report_to
#          , emp_benefits
#     from peo_career pc
#     left join people p on pc.emp_reporting_to_peo_no = p.peo_no
# """, engine_mssql)
# wh['emp_from'] = wh['emp_from'].astype(str)
# wh['emp_from_1'] = wh['emp_from'].apply(lambda x: x[0:4] if x else x)
# wh['emp_from_2'] = wh['emp_from'].apply(lambda x: x[4:6] if x else x)
# wh['emp_from_3'] = wh['emp_from'].apply(lambda x: x[6:9] if x else x)
# wh['emp_from'] = wh['emp_from_1'] +'-'+wh['emp_from_2']+'-'+wh['emp_from_3']
#
# wh['emp_to'] = wh['emp_to'].astype(str)
# wh['emp_to_1'] = wh['emp_to'].apply(lambda x: x[0:4] if x else x)
# wh['emp_to_2'] = wh['emp_to'].apply(lambda x: x[4:6] if x else x)
# wh['emp_to_3'] = wh['emp_to'].apply(lambda x: x[6:9] if x else x)
# wh['emp_to'] = wh['emp_to_1'] +'-'+wh['emp_to_2']+'-'+wh['emp_to_3']
# wh['emp_to'] = wh['emp_to'].apply(lambda x: x.replace('0--',''))
# wh['emp_from'] = wh['emp_from'].apply(lambda x: x.replace('0--',''))
#
# wh['emp_salary'] = wh['emp_salary'].apply(lambda x: str(x))
# wh['emp_ote'] = wh['emp_ote'].apply(lambda x: str(x))
#
# wh['experience'] = wh[['emp_from', 'emp_to','emp_cli_name','emp_job_title','emp_salary','emp_ote','report_to','emp_benefits']]\
#     .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['From', 'To','Company','Position','Salary','OTE','Reporting to','Benefits'], x) if e[1]]), axis=1)
# cp7 = vcand.update_exprerience_work_history2(wh, dest_db, mylog)

# %% status
tem = candidate[['candidate_externalid','Status']].dropna().drop_duplicates()
tem['name'] = tem['Status']
tem1 = tem[['name']].drop_duplicates()
tem1['owner'] =''
vcand.create_status_list(tem1, mylog)
vcand.add_candidate_status(tem, mylog)

# %% full part time
tem = candidate[['candidate_externalid', 'Current Full-time/Part-time']].dropna().drop_duplicates()
tem['Current Full-time/Part-time'].unique()
tem.loc[tem['Current Full-time/Part-time']=='Full-time', 'employment_type'] = 'fulltime'
tem2 = tem[['candidate_externalid','employment_type']].dropna()
vcand.update_employment_type(tem2, mylog)

# %% source
tem = candidate[['candidate_externalid', 'Source']].dropna().drop_duplicates()
tem['source'] = tem['Source']
cp = vcand.insert_source(tem)

# %% twitter
tem = candidate[['candidate_externalid', 'Twitter']].dropna().drop_duplicates()
tem['twitter'] = tem['Twitter']
cp = vcand.update_twitter(tem, mylog)

# %% linkedin
tem = candidate[['candidate_externalid', 'LinkedIn']].dropna().drop_duplicates()
tem['linkedin'] = tem['LinkedIn']
cp = vcand.update_linkedin(tem, mylog)

