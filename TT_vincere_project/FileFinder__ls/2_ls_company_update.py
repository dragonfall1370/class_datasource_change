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
cf.read('ls_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
# src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
# conn_str_sdb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(src_db.get('user'), src_db.get('password'), src_db.get('server'), src_db.get('port'), src_db.get('database'))
# engine_postgre_src = sqlalchemy.create_engine(conn_str_sdb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()

from common import vincere_company
vcom = vincere_company.Company(connection)
# %%
sql ="""select c.idcompany as company_externalid, l.Value as location
from company c
left join companyx cx on c.idcompany = cx.idcompany
left join Location l on l.idLocation = c.idLocation"""

company = pd.read_sql(sql, engine_sqlite)
assert False

# %% billing address
company['address'] = company['location']
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)

# %% country
tem = company[['company_externalid', 'location', 'address']].dropna()
tem['country_code'] = tem.location.map(vcom.get_country_code)
tem['country'] = tem.location
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% company info
sql = """
select ce.idCompany as company_externalid, ea.CommValue as value, cct.Value as type
from Company_EAddress ce
left join CompanyCommunicationType cct on cct.idCompanyCommunicationType = ce.idCompanyCommunicationType
left join EAddress ea on ea.idEAddress = ce.idEAddress
"""
company = pd.read_sql(sql, engine_sqlite)

# %% phone / switchboard
tem = company.loc[company['type'] == 'Switchboard']
tem = tem.dropna()
tem['switch_board'] = tem['value']
tem['phone'] = tem['value']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %% website
tem = company.loc[company['type'] == 'URL']
tem = tem.dropna()
tem['website'] = tem['value']
cp5 = vcom.update_website(tem, mylog)

# %% employees_number
sql = """
select idcompany as company_externalid
, noofemployees, CreatedOn
from companyx
"""
company = pd.read_sql(sql, engine_sqlite)

tem = company[['company_externalid', 'NoOfEmployees']].dropna()
tem['employees_number'] = tem['NoOfEmployees']
tem['employees_number'] = tem['employees_number'].astype(int)
cp5 = vcom.update_employees_number(tem, mylog)

# %% note
company_location = pd.read_sql("""
select c.idcompany as company_externalid
,c.companyname, addr.*
from Company c
join (select idCompany, isdefault
, addressline1
, addressline2
, addressline3
, addressline4
, city
, postcode
, pa.country
from Company_PAddress cp
left join (select paddress.*, country.value as country
from paddress
join country on country.idcountry = paddress.idcountry) pa on cp.idpaddress = pa.idpaddress) addr on addr.idCompany = c.idCompany
""", engine_sqlite)
company_location['address'] = company_location[['addressline1', 'addressline2', 'addressline3', 'addressline4', 'city','postcode','country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company_location = company_location[['company_externalid','address']]
company_location = company_location.loc[company_location['address'] != '']

sql = """
select c.idcompany as company_externalid
, cx.DefaultEmail
from company c
left join companyx cx on c.idcompany = cx.idcompany
"""
company_email = pd.read_sql(sql, engine_sqlite)
company_email = company_email.dropna()


company_location['note'] = 'Mailing Addresses: ' + company_location['address']
company_email['note'] = 'Email: ' + company_email['DefaultEmail']
vcom.update_note_2(company_location, dest_db, mylog)
vcom.update_note_2(company_email, dest_db, mylog)

# %% industry
company_industries = pd.read_sql("""
select idcompany as company_externalid, idindustry_string_list from companyx
""", engine_sqlite)
company_industries = company_industries.dropna()
industry = company_industries.idIndustry_String_List.map(lambda x: x.split(',') if x else x) \
    .apply(pd.Series) \
    .merge(company_industries[['company_externalid']], left_index=True, right_index=True) \
    .melt(id_vars=['company_externalid'], value_name='idIndustry') \
    .drop('variable', axis='columns') \
    .dropna()
industry['idIndustry'] = industry['idIndustry'].str.lower()
industries = pd.read_sql("""
select i1.idIndustry, i2.Value as ind, i1.Value as sind
from Industry i1
left join Industry i2 on i1.ParentId = i2.idIndustry
""", engine_sqlite)
industries['idIndustry'] = industries['idIndustry'].str.lower()
company_industries = industry.merge(industries, on='idIndustry')
company_industries['value'] = company_industries[['ind', 'sind']].apply(lambda x: '-'.join([e for e in x if e]), axis=1)
company_industries['matcher'] = company_industries['value'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())

industries_csv = pd.read_csv('industries.csv')
industries_csv['matcher'] = industries_csv['Industry'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
company_industries = company_industries.merge(industries_csv, on='matcher')

company_industries_2 = company_industries[['company_externalid','Vincere Industry','Sub Industry']].drop_duplicates()
company_industries_2 = company_industries_2.where(company_industries_2.notnull(),None)
tem1 = company_industries_2[['company_externalid','Vincere Industry']].drop_duplicates().dropna()
tem1['name'] = tem1['Vincere Industry']
cp10 = vcom.insert_company_industry(tem1, mylog)

tem2 = company_industries_2[['company_externalid','Sub Industry']].drop_duplicates().dropna()
tem2['name'] = tem2['Sub Industry']
cp10 = vcom.insert_company_sub_industry(tem2, mylog)

# %% reg date
tem = company[['company_externalid', 'CreatedOn']]
tem['reg_date'] = pd.to_datetime(tem['CreatedOn'])
vcom.update_reg_date(tem, mylog)


# # %%
# sql = """
# select c.idcompany as company_externalid
# ,c.companyname, addr.*
# from Company c
# join (select idCompany, isdefault
# , addressline1
# , addressline2
# , addressline3
# , addressline4
# , city
# , postcode
# , pa.country
# from Company_PAddress cp
# left join (select paddress.*, country.value as country
# from paddress
# join country on country.idcountry = paddress.idcountry) pa on cp.idpaddress = pa.idpaddress) addr on addr.idCompany = c.idCompany
# """
# company = pd.read_sql(sql, engine_sqlite)
# company['address'] = company[['addressline1', 'addressline2', 'addressline3', 'addressline4', 'city','postcode','country']] \
#     .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# company = company.loc[company['address'] != '']
# sql = """
# select c.idcompany as company_externalid
# , l.Value as location
# from company c
# left join companyx cx on c.idcompany = cx.idcompany
# left join Location l on l.idLocation = c.idLocation
# """
# company_location = pd.read_sql(sql, engine_sqlite)
# company_location = company_location.loc[~company_location['company_externalid'].isin(company['company_externalid'])]
# company_location = company_location.dropna()
# assert False
#
# # %% billing address
# company_location['address'] = company_location['location']
# cp2 = vcom.insert_company_location_2(company_location, dest_db, mylog)
#
# # %% country
# company_location['country_code'] = company_location.location.map(vcom.get_country_code)
# company_location = company_location.loc[company_location['country_code'] != '']
# company_location['country'] = company_location.location
# cp6 = vcom.update_location_country_2(company_location, dest_db, mylog)