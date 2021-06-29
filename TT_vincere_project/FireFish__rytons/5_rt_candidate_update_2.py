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
cf.read('rt_config.ini')
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
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')

conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
candidate = pd.read_sql("""
select p.ID as candidate_externalid
     , p.FirstName
     , p.MiddleName
     , p.Surname
     , p.KnownAs
     , c.Address1
     , c.Address2
     , c.Address3
     , c.Town
     , c.PostCode
     , c.County
     , c.Country
     , c.WebAddress
     , c.PhoneMobile
     , c.PhoneHome
     , c.DateOfBirth
     , c.DrivingLicenceWSIID
     , p.JobTitle
     , com.company_name
     , com.DateFrom
     , com.JobTitle as current_job_title
     , rn.ID as citizenship
     , crt.Name as rate_type
     , c.Reference1
     , c.Reference2
     , c.LookingForRateMin
     , c.RequiredPackage
     , c.CurrentSalary
     , c.LookingForSalaryMin
     , c.PersonalEMail
     , o.WorkEMail as owner
from Person p
join Candidate c on p.ID = c.ID
left join Person o on c.OwnerPersonID = o.ID
left join (select ce.*, comp.Name as company_name from CandidateEmployment ce left join Company comp on ce.CompanyID = comp.ID) com on p.ID = com.CandidateID
left join rytons_mapping_nationalities rn on(lower(rn.Name)) = (lower(c.NationalityWSIID))
left join ContractRateType crt on crt.ID = c.ContractRateTypeID

""", engine_sqlite)
candidate.sort_values('DateFrom', inplace=True, ascending=False)
candidate['rn'] = candidate.groupby('candidate_externalid').cumcount()
# contact = contact.loc[contact['rn'] == 0]
candidate['candidate_externalid'] = candidate['candidate_externalid'].apply(lambda x: str(x) if x else x)
cand_more = pd.read_sql("""
select external_id from "rytonsassociates.vincere.io".public.candidate where insert_timestamp >= '2020-01-01'
""", engine_postgre)
candidate = candidate.merge(cand_more,left_on='candidate_externalid', right_on='external_id')
assert False
# %% job type
# jobtp = candidate[['candidate_externalid', 'ts2__Job_Type__c']].dropna()
# jobtp = jobtp.ts2__Job_Type__c.map(lambda x: x.split(';')) \
#    .apply(pd.Series) \
#    .merge(jobtp[['candidate_externalid']], left_index=True, right_index=True) \
#    .melt(id_vars=['candidate_externalid'], value_name='job_type') \
#    .drop('variable', axis='columns') \
#    .dropna()
#
# jobtp.loc[jobtp['job_type']=='Perm', 'desired_job_type'] = 'permanent'
# jobtp.loc[jobtp['job_type']=='Temp', 'desired_job_type'] = 'contract'
# jobtype = jobtp[['candidate_externalid', 'desired_job_type']].dropna()
# jobtype['desired_job_type'].unique()
# cp = vcand.update_desired_job_type_2(jobtype, mylog)

# %% location name/address
# candidate = candidate.loc[candidate['rn'] == 0]
candidate['location_name'] = candidate[['Address1', 'Address2', 'Address3', 'Town', 'PostCode', 'County', 'Country']] \
   .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate['address'] = candidate.location_name
tem = candidate[['candidate_externalid', 'address', 'location_name']].drop_duplicates()
cp2 = vcand.insert_common_location_v2(tem, dest_db, mylog)

# update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
comaddr = candidate[['candidate_externalid', 'Town', 'PostCode', 'County', 'Country', 'address']].drop_duplicates()\
    .rename(columns={'Town': 'city', 'County': 'state', 'PostCode': 'post_code'})

cp3 = vcand.update_location_city2(comaddr, dest_db, mylog)
# update state
cp4 = vcand.update_location_state2(comaddr, dest_db, mylog)
# update postcode
cp5 = vcand.update_location_post_code2(comaddr, dest_db, mylog)
#  update country
tem = comaddr[['candidate_externalid', 'Country', 'address']].dropna()
tem['country_code'] = tem.Country.map(vcand.get_country_code)
cp6 = vcand.update_location_country_code2(tem, dest_db, mylog)
vcand.set_all_current_candidate_address_as_mailling_address()

# %% phones
indt = candidate[['candidate_externalid', 'PhoneHome']].dropna().drop_duplicates()
indt['home_phone'] = indt['PhoneHome']
cp = vcand.update_home_phone2(indt, dest_db, mylog)
indt = candidate[['candidate_externalid', 'PhoneMobile']].dropna().drop_duplicates()
indt['primary_phone'] = indt['PhoneMobile']
indt['mobile_phone'] = indt['PhoneMobile']
cp = vcand.update_primary_phone_v2(indt, dest_db, mylog)
cp = vcand.update_mobile_phone_v2(indt, dest_db, mylog)

# %% emails
# indt = candidate[['candidate_externalid', 'E_mail_2__c']].dropna()
# indt['work_email'] = indt['E_mail_2__c']
# cp = vcand.update_work_email(indt, mylog)

# %% current employer
cur_emp = candidate[['candidate_externalid', 'company_name', 'current_job_title', 'rn']]
cur_emp['current_employer'] = cur_emp['company_name']
cur_emp = cur_emp.loc[cur_emp['rn'] == 0]
vcand.update_candidate_current_employer_title_v2(cur_emp, dest_db, mylog)

# %% salutation / gender title
# parse_gender_title = pd.read_csv('gender_title.csv')
# tem = candidate[['candidate_externalid', 'Salutation']].dropna().drop_duplicates()
# tem['gender_title'] = tem['Salutation']
# tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
# tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
# tem['gender_title'] = tem['gender_code']
# vcand.update_gender_title(tem, mylog)

# %% note
note = candidate[[
    'candidate_externalid'
    , 'YC_Comments__c'
    , 'YC_Executive_Summary__c'
    , 'ts2__Date_Available__c'
    ]]

note['ts2__Date_Available__c'] = pd.to_datetime(note['ts2__Date_Available__c'], errors='coerce')
note['ts2__Date_Available__c'] = note['ts2__Date_Available__c'].apply(lambda x: datetime.datetime.strftime(x, '%d/%m/%Y') if str(x) != 'NaT' else None)

prefixs = [
'YC ID'
, 'YC Comments'
, 'YC Executive Summary'
, 'Date Available'
]

note['note'] = note.apply(lambda x: '\n'.join([': '.join(str(e1) for e1 in e) for e in zip(prefixs, x) if e[1]]), axis='columns')
cp11 = vcand.update_note2(note, dest_db, mylog)


# %% dob
dob = candidate[['candidate_externalid', 'DateOfBirth']].dropna().drop_duplicates()
dob['date_of_birth'] = dob['DateOfBirth']
dob['date_of_birth'] = pd.to_datetime(dob['date_of_birth'])
vcand.update_date_of_birth(dob, mylog)

# %% preferred name
tem = candidate[['candidate_externalid', 'KnownAs']].dropna().drop_duplicates()
tem['preferred_name'] = tem['KnownAs']
vcand.update_preferred_name(tem, mylog)

# %% citizenship
tem = candidate[['candidate_externalid', 'citizenship']].dropna().drop_duplicates()
tem['nationality'] = tem['citizenship'].map(vcand.get_country_code)
vcand.update_nationality(tem, mylog)

# %% current salary
tem = candidate[['candidate_externalid', 'CurrentSalary']].dropna().drop_duplicates()
tem['current_salary'] = tem['CurrentSalary']
tem['current_salary'] = tem['current_salary'].astype(float)
vcand.update_current_salary(tem, mylog)

# %% desire salary
tem = candidate[['candidate_externalid', 'LookingForSalaryMin']].dropna().drop_duplicates()
tem['desire_salary'] = tem['LookingForSalaryMin']
tem['desire_salary'] = tem['desire_salary'].astype(float)
vcand.update_desire_salary(tem, mylog)

# %% other benefits
tem = candidate[['candidate_externalid', 'RequiredPackage']].dropna().drop_duplicates()
tem['other_benefits'] = tem['RequiredPackage']
vcand.update_other_benefits(tem, mylog)

# %% contract rate
tem = candidate[['candidate_externalid', 'LookingForRateMin']].dropna().drop_duplicates()
tem['contract_rate'] = tem['LookingForRateMin']
tem.loc[tem['contract_rate']=='Daily', 'contract_rate'] = '0.00'
tem['contract_rate'] = tem['contract_rate'].astype(float)
vcand.update_contract_rate(tem, mylog)

# %% contract interval
tem = candidate[['candidate_externalid', 'rate_type']].dropna().drop_duplicates()
# tem['contract_interval'] = tem['rate_type']
tem.loc[tem['rate_type']=='Daily', 'contract_interval'] = 'daily'
tem.loc[tem['rate_type']=='Hourly', 'contract_interval'] = 'hourly'
tem2 = tem[['candidate_externalid', 'contract_interval']].dropna()
vcand.update_contract_interval(tem2, mylog)

# %% contract interval
tem = candidate[['candidate_externalid', 'WebAddress']].dropna().drop_duplicates()
tem['website'] = tem['WebAddress']
vcand.update_website(tem, mylog)

# %% industry
# industries = pd.read_csv('industries.csv')
#
# industry = candidate[['candidate_externalid', 'Industry_Experience__c']].dropna()
# industry = industry.Industry_Experience__c.map(lambda x: x.split(';')) \
#    .apply(pd.Series) \
#    .merge(industry[['candidate_externalid']], left_index=True, right_index=True) \
#    .melt(id_vars=['candidate_externalid'], value_name='name') \
#    .drop('variable', axis='columns') \
#    .dropna()
#
# industry['name'] = industry['name'].apply(lambda x: 'Utility Services' if x == 'Utility_Services' else x)
# industry['matcher'] = industry['name']
# industry = industry.merge(industries, left_on='matcher', right_on='Industry Experience Candidates Jobscience')
# tem = industry[['candidate_externalid', 'Industries Vincere']].dropna()
# tem['name'] = tem['Industries Vincere']
# tem = tem.drop_duplicates()
# cp8 = vcand.insert_candidate_industry(tem, mylog)

