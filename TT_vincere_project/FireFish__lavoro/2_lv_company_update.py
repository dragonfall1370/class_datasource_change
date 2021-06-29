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
cf.read('lv_config.ini')
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
company['address'] = company[['Address1', 'Address2', 'Address3', 'Town','County', 'PostCode','Country']] \
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

# %% facebook
fb = pd.read_sql("""
select Detail, CompanyID as company_externalid, Name from ContactDetail cd
left join ContactDetailType cdt on cd.TypeID = cdt.ID
where CompanyID is not null and Name = 'Facebook'
""", engine_sqlite)
fb['url_facebook'] = fb['Detail']
fb['company_externalid'] = fb['company_externalid'].apply(lambda x: str(x) if x else x)
cp5 = vcom.update_facebook(fb, mylog)

# %% last action date
tem = company[['company_externalid', 'LastActionDate']].dropna()
tem['last_activity_date'] = pd.to_datetime(tem['LastActionDate'])
vcom.update_last_activity_date(tem, mylog)

# %% note
sql = """
select c1.ID as company_externalid
     , c1.Name
     , c1.LastActionDate
     , c1.EMail
     , c1.IsParentCompany
     , c2.Name as parent
     , c1.PermanentMarkupPercent
     , c1.ContractMarkupPercent
     , ltd_registration
     , Background
     , Turnover, c1.DisplayID
from Company c1
left join Company c2 on c1.ParentCompanyID = c2.ID
left join (
select CompanyID, TextValue as ltd_registration
from CompanyCustomField ccf
left join CustomField cf on ccf.CustomFieldID = cf.id where DisplayName = 'Ltd Registration') cus on cus.CompanyID = c1.ID
left join (
select CompanyID, Background, Turnover from CompanyProfile) cp on cp.CompanyID = c1.ID
"""
note = pd.read_sql(sql, engine_sqlite)
note['company_externalid'] = company['company_externalid'].apply(lambda x: str(x) if x else x)
note['LastActionDate'] = company['LastActionDate'].apply(lambda x: str(x) if x else x)
note['LastActionDate'] = company['LastActionDate'].apply(lambda x: x.replace('T',' ') if x else x)
note.loc[note['IsParentCompany']=='1', 'IsParentCompany'] = 'Yes'
note.loc[note['IsParentCompany']=='0', 'IsParentCompany'] = 'No'
note = note.where(note.notnull(),None)
note['note'] = note[['DisplayID', 'EMail', 'ltd_registration', 'IsParentCompany', 'PermanentMarkupPercent', 'ContractMarkupPercent','Background','Turnover','parent','LastActionDate']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Lavoro ID', 'Email', 'Ltd Registration', 'This is a company headquarters', 'Permanent Agreed Rate %', 'ContractAgreed Rate %','Description','Turnover'
    ,'This is a company headquarters','Last Action Date'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(note, dest_db, mylog)


# %% company number
# tem = company[['company_externalid', 'LtdRegistration']].dropna()
# tem['company_number'] = tem['LtdRegistration']
# vcom.update_company_number(tem, dest_db, mylog)

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
