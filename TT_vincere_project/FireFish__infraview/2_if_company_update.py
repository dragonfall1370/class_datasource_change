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
cf.read('if_config.ini')
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
# %%
sql = """
select c.ID as company_externalid
     , c.Name
     , c.Address1
     , c.Address2
     , c.Address3
     , c.Town
     , c.PostCode
     , c.County
     , c.Country
     , c.Phone
     , c.WebAddress
     , c.LtdRegistration
     , LastActionDate
     , EMail
     , IsParentCompany
     , PermanentMarkupPercent
     , ContractMarkupPercent
from Company c
"""
company = pd.read_sql(sql, engine_sqlite)
company['company_externalid'] = company['company_externalid'].apply(lambda x: str(x) if x else x)
assert False

# %% billing address
company['address'] = company[['Address1', 'Address2', 'Address3', 'Town', 'PostCode','County','Country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
comaddr = company.loc[company['address']!='']
cp2 = vcom.insert_company_location_2(comaddr, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid','address','Town']].dropna().drop_duplicates()
tem['city'] = tem['Town']
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid','address','PostCode']].dropna().drop_duplicates()
tem['post_code'] = tem['PostCode']
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid','address','County']].dropna().drop_duplicates()
tem['state'] = tem['County']
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'Country', 'address']].dropna().drop_duplicates()
tem['country_code'] = tem.Country.map(vcom.get_country_code)
tem['country'] = tem.Country
tem['country_code'] = 'GB'
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% location type
comaddr['location_type'] = 'BILLING_ADDRESS'
tem = comaddr[['company_externalid','address','location_type']].dropna()
cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'Phone']].dropna()
tem['switch_board'] = tem['Phone']
vcom.update_switch_board(tem, mylog)
tem['phone'] = tem['Phone']
vcom.update_phone(tem, mylog)

# %% website
tem = company[['company_externalid', 'WebAddress']].dropna()
tem['website'] = tem['WebAddress']
tem['website'] = tem['website'].apply(lambda x: x[:100])
cp5 = vcom.update_website(tem, mylog)

# %% linkedin
linkedin = pd.read_sql("""
select Detail, CompanyID as company_externalid, Name from ContactDetail cd
left join ContactDetailType cdt on cd.TypeID = cdt.ID
where CompanyID is not null and Name = 'LinkedIn'
""", engine_sqlite)
linkedin['url_linkedin'] = linkedin['Detail']
linkedin['company_externalid'] = linkedin['company_externalid'].apply(lambda x: str(x) if x else x)
cp5 = vcom.update_linkedin(linkedin, mylog)

# %% last action date
tem = company[['company_externalid', 'LastActionDate']].dropna()
tem['last_activity_date'] = pd.to_datetime(tem['LastActionDate'])
vcom.update_last_activity_date(tem, mylog)

# %% note
note = company[['company_externalid', 'EMail', 'LtdRegistration', 'IsParentCompany', 'PermanentMarkupPercent', 'ContractMarkupPercent']]
note.loc[note['IsParentCompany']=='1', 'IsParentCompany'] = 'Yes'
note.loc[note['IsParentCompany']=='0', 'IsParentCompany'] = 'No'
profile = pd.read_sql("""select CompanyID as company_externalid, Background, Turnover from CompanyProfile""", engine_sqlite)
profile['company_externalid'] = profile['company_externalid'].apply(lambda x: str(x) if x else x)
benefit = pd.read_sql("""select * from CompanyBenefits""", engine_sqlite)
benefit['company_externalid'] = benefit['CompanyID'].apply(lambda x: str(x) if x else x)
note = note.merge(profile, on='company_externalid', how='outer')
note = note.merge(benefit, on='company_externalid', how='outer')
note = note.where(note.notnull(),None)
note['note'] = note[['company_externalid', 'EMail', 'LtdRegistration', 'IsParentCompany', 'PermanentMarkupPercent', 'ContractMarkupPercent','Background','Turnover'
    ,'Holidays','PensionScheme','TrainingCareerDevelopment','BonusScheme','HealthCare','OnCall','CommissionStructure','AdditionalBenefits']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Infraview ID', 'Email', 'Ltd Registration', 'This is a company headquarters', 'Permanent Agreed Rate %', 'ContractAgreed Rate %','Description','Turnover'
    ,'Holidays','Pension Scheme','Training & Career Development','Bonus Scheme','Health Care','On Call','Commission Structure','Additional Benefits'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(note, dest_db, mylog)


# %% company number
tem = company[['company_externalid', 'LtdRegistration']].dropna()
tem['company_number'] = tem['LtdRegistration']
vcom.update_company_number(tem, dest_db, mylog)

# %% industry
tem = pd.read_sql("""
select Value as name from Drop_Down___Sectors
""", engine_sqlite)
from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(connection)
tem['insert_timestamp'] = datetime.datetime.now()
vcand.insert_industry(tem, mylog)

sql = """
select c.ID as company_externalid, rmi.Value as industries
from Company c
left join Drop_Down___Sectors rmi on rmi.ID= c.PrimarySectorWSIID
where Value is not null
"""
company_industries = pd.read_sql(sql, engine_sqlite)
company_industries['company_externalid'] = company_industries['company_externalid'].apply(lambda x: str(x) if x else x)

company_industries['name'] = company_industries['industries']
company_industries = company_industries.drop_duplicates().dropna()
cp10 = vcom.insert_company_industry(company_industries, mylog)

# %% fix brief
com = pd.read_sql("""select id, note from company where external_id is not null""", engine_postgre_review)
com['note'] = com['note'].apply(lambda x: x.replace('<p>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('</p>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('&nbsp;','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('<strong>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('</strong>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('<ul>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('<li>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('</u>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('<u>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('<br />','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('&rsquo;','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('&lsquo;','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('</ul>','') if x else x)
com['note'] = com['note'].apply(lambda x: x.replace('</li>','') if x else x)
# com['note'] = com['note'].apply(lambda x: x.replace('<u>','') if x else x)
com = com.dropna()
com.loc[com['note'].str.contains('<')]
vincere_custom_migration.load_data_to_vincere(com, dest_db, 'update', 'company', ['note', ], ['id'], mylog)
vincere_custom_migration.execute_sql_update(r"update company set note=replace(note, '\n', chr(10)) where note is not null;", vcom.ddbconn)
