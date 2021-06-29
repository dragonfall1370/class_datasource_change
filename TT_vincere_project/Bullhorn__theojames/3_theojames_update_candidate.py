# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
import os
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
cf.read('theo_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')

mylog = log.get_info_logger(log_file)
# assert False
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% connect data
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

from common import vincere_candidate
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())

# %% candidate
candidate = pd.read_sql("""
select 
cand.candidateID as candidate_externalid
, cand.firstName as candidate_firstname
, cand.lastName as candidate_lastname
, cand.middleName as candidate_middlename
, cand.email as candidate_email
, cand.address1
, cand.address2
, cand.city
, cand.state
, cand.zip
, c.COUNTRY
, cand.dateAdded as reg_date
, cand.dateOfBirth as date_of_birth
, cand.email2
, cand.email3
, cand.namePrefix
, cand.companyURL as website

, cand.comments
, cand.customText4
, cand.dateAvailable
, cand.companyDescription
, cand.employmentPreference
, cand.dateLastComment
, cand.massMailOptOut
, cand.referredBy
, cand.referredByUserID
, cand.status
, cand.travelLimit
, cand.willRelocate
, cand.workAuthorized

, cand.businessSectorIDList

, cand.[source]
, cand.workPhone
, cand.salary
, cand.salaryLow
, cand.phone
, cand.phone2
, cand.mobile
, cand.occupation
, cand.companyName
, cand.hourlyRate
, cand.hourlyRateLow
, cand.employeeType
, cand.desiredLocations

, cand.secondaryAddress1
, cand.secondaryAddress2
, cand.secondaryCity
, cand.secondaryState
, cand.secondaryZip
, c1.COUNTRY as secondaryCounry
from bullhorn1.Candidate cand
left join tmp_country c on cand.countryID = c.CODE
left join tmp_country c1 on cand.secondaryCountryID = c1.CODE
where cand.isDeleted != 1 and cand.status != 'Archive'
""", engine_mssql)
candidate.candidate_externalid = candidate.candidate_externalid.astype(str)
assert False

# %% compliance onboarding choose country
# candidate['country_code'] = 'GB'
# vcand.update_onboarding_choose_country(candidate, mylog)
#
# tem = candidate[['candidate_externalid', 'companyName', 'secondaryAddress1', 'secondaryAddress2', 'secondaryCity', 'secondaryState', 'secondaryZip', 'secondaryCounry']]
# tem['address'] = candidate[['secondaryAddress1', 'secondaryAddress2', 'secondaryCity', 'secondaryState', 'secondaryZip', 'secondaryCounry']].apply(lambda x: ', '.join([e for e in x if e and e.strip()!='']), axis='columns')
# tem.rename(columns={'companyName':'company_name'}, inplace=True)
# cp6 = vcand.insert_onboarding_company_details__company_name(tem, mylog)
# cp6 = vcand.insert_onboarding_company_details__address(tem, mylog)

# %% desire location custom field
# field_key = 'cfae0e97f4967d452e0610ab133c77b2'
# vincere_custom_migration.insert_candidate_text_field_values(candidate, 'candidate_externalid', 'desiredLocations', field_key, engine_postgre.raw_connection())
# candidate.loc[candidate.desiredLocations.notnull()][['candidate_externalid', 'desiredLocations']]

# %% jobtype
tem = candidate[['candidate_externalid', 'employeeType']].dropna()
tem.employeeType.unique()
tem.loc[tem.employeeType=='PAYE', 'desired_job_type'] = 'permanent'
tem.loc[tem.employeeType=='LTD', 'desired_job_type'] = 'contract'
tem.loc[tem.employeeType=='Umbrella', 'desired_job_type'] = 'contract'
tem.loc[tem.employeeType=='', 'desired_job_type'] = 'permanent'

vcand.update_desired_job_type(tem, mylog)

# %% middle name
tem_middle = candidate[['candidate_externalid', 'candidate_middlename']].dropna().rename(columns={'candidate_middlename': 'middle_name'})
vcand.update_middle_name(tem_middle, mylog)

# %% pay rate
tem = candidate[['candidate_externalid', 'hourlyRateLow']].dropna().rename(columns={'hourlyRateLow': 'contract_rate'})
vcand.update_contract_rate(tem, mylog)

# %% desire payrate
tem = candidate[['candidate_externalid', 'hourlyRate']].dropna().rename(columns={'hourlyRate': 'desired_contract_rate'})
vcand.update_desired_contract_rate(tem, mylog)

# %% primary phone
tem = candidate[['candidate_externalid', 'mobile']].rename(columns={'mobile': 'primary_phone'}).dropna()
vcand.update_primary_phone(tem, mylog)

# %% current jobtitle
tem = candidate[['candidate_externalid', 'companyName', 'occupation']].rename(columns={'occupation': 'current_job_title', 'companyName': 'current_employer'})
# tem = tem.where(tem.notnull(),'')
tem['current_job_title'] = tem.apply(lambda x: x['current_job_title'].replace('\\', '/') if x['current_job_title'] else x['current_job_title'], axis=1)
tem['current_employer'] = tem.apply(lambda x: x['current_employer'].replace('\\', '/')if x['current_employer'] else x['current_employer'], axis=1)
vcand.update_candidate_current_employer_title(tem, mylog)


# %% home phone
tem = candidate[['candidate_externalid', 'phone']].dropna().rename(columns={'phone': 'home_phone'})
vcand.update_home_phone(tem, mylog)

# %% mobile phone
tem = candidate[['candidate_externalid', 'mobile', 'phone2']]
tem['mobile_phone'] = tem[['mobile', 'phone2']].apply(lambda x: ', '.join([e for e in x if e]), axis='columns')
vcand.update_mobile_phone(tem, mylog)

# %% source
tem = candidate[['candidate_externalid', 'source']].dropna()
vcand.insert_source(tem)

# %% work phone
tem = candidate[['candidate_externalid', 'workPhone']].dropna().rename(columns={'workPhone': 'work_phone'})
vcand.update_work_phone(tem, mylog)

# %% desired annual salary
tem = candidate[['candidate_externalid', 'salary']].dropna().rename(columns={'salary': 'desire_salary'})
vcand.update_desire_salary(tem, mylog)

# %% current annual salary
tem = candidate[['candidate_externalid', 'salaryLow']].dropna().rename(columns={'salaryLow': 'current_salary'})
vcand.update_current_salary(tem, mylog)

# %% industry
industry = candidate[['candidate_externalid', 'businessSectorIDList']]

industry = industry.businessSectorIDList.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(industry[['candidate_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['candidate_externalid'], value_name='businessSectorID') \
    .drop('variable', axis='columns') \
    .dropna()
industry = industry.loc[industry.businessSectorID.str.strip() != '']
industry.businessSectorID = industry.businessSectorID.astype(int)
industry = industry.merge(pd.read_sql("select businessSectorID, name from bullhorn1.BH_BusinessSectorList;", engine_mssql), on='businessSectorID')

import datetime
tem = industry[['name']].drop_duplicates()
tem['insert_timestamp'] = datetime.datetime.now()
vcand.append_industry(tem, mylog)
cp8 = vcand.insert_candidate_industry(industry, mylog)

# %% salutation / gender title
parse_gender_title = pd.read_csv('gender_title.csv')
tem = candidate[['candidate_externalid', 'namePrefix']].dropna().drop_duplicates()
tem['gender_title'] = tem['namePrefix']
tem['gender_display_lower'] = tem['gender_title'].apply(lambda x: ''.join([e for e in re.findall('\w*', x) if e]).lower())
tem = tem.merge(parse_gender_title, on='gender_display_lower', how='outer')
tem['gender_title'] = tem['gender_code']
vcand.update_gender_title(tem, mylog)

# %% note
note = candidate[[
    'candidate_externalid'
    , 'comments'
    ]]

note.info()
# note.dateAvailable = note.dateAvailable.astype(object).where(note.dateAvailable.notnull(), None)
# note.dateAvailable.loc[note.dateAvailable.notnull()] = note.dateAvailable.loc[note.dateAvailable.notnull()].dt.strftime('%d/%m/%Y')
#
# note.dateLastComment = note.dateLastComment.astype(object).where(note.dateLastComment.notnull(), None)
# note.dateLastComment.loc[note.dateLastComment.notnull()] = note.dateLastComment.loc[note.dateLastComment.notnull()].dt.strftime('%d/%m/%Y')
#
# note.massMailOptOut = note.massMailOptOut.replace({0: False, 1: True})
#
# note.referredByUserID = note.referredByUserID.astype(object).where(note.referredByUserID.notnull(), None)
#
# note.travelLimit = note.travelLimit.replace({0: False, 1: True})
# note.willRelocate = note.willRelocate.replace({0: False, 1: True})
# note.workAuthorized = note.workAuthorized.replace({0: False, 1: True})

prefixs = [
'Candidate BH ID'
, 'General Comments'
]

note['note'] = note.apply(lambda x: '\n'.join([': '.join(str(e1) for e1 in e) for e in zip(prefixs, x) if e[1]]), axis='columns')
cp11 = vcand.update_note(note, mylog)

# %% website
cp10 = vcand.update_website(candidate, mylog)

# %% reg date
cp1 = vcand.update_reg_date(candidate, mylog)

# %% location name/address
candidate['location_name'] = candidate[['address1', 'address2', 'city', 'state', 'zip', 'COUNTRY']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
candidate['address'] = candidate.location_name
candidate.info()
cp2 = vcand.insert_common_location(candidate, mylog)

# %% update city
vcand = vincere_candidate.Candidate(engine_postgre.raw_connection())
cp3 = vcand.update_location_city(candidate, mylog)

# %% update state
cp4 = vcand.update_location_state(candidate, mylog)

# %% update postcode
candidate['post_code'] = candidate['zip']
cp5 = vcand.update_location_post_code(candidate, mylog)

# %% update country
candidate['country_code'] = candidate.COUNTRY.map(vcand.get_country_code)
cp6 = vcand.update_location_country_code(candidate, mylog)

# %% dob
tem = candidate[['candidate_externalid', 'date_of_birth']].dropna()
tem.date_of_birth = pd.to_datetime(tem.date_of_birth)
cp7 = vcand.update_date_of_birth(tem, mylog)

# %% work email
candidate['work_email'] = candidate[['email2', 'email3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp8 = vcand.update_work_email(candidate, mylog)

# %% owner
owner = pd.read_sql('select candidateid as candidate_externalid, email from cand_owner', engine_sqlite)
cp9 = vcand.update_owner(owner, mylog)

