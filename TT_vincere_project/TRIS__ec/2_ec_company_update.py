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
cf.read('ec_config.ini')
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
# %%
sql ="""select ClientID as company_externalid,
       ClientName,
       nullif(convert(varchar,ClientTelephone),'') as ClientTelephone,
       nullif(convert(varchar,ClientFax),'') as ClientFax,
       nullif(convert(varchar,ClientWWWSite),'') as ClientWWWSite,
       nullif(convert(varchar,ClientCreateDate),'') as ClientCreateDate,
       nullif(convert(varchar,ClientAddress),'') as ClientAddress,
       nullif(convert(varchar,ClientAddress2),'') as ClientAddress2,
       nullif(convert(varchar,ClientCity),'') as ClientCity,
       nullif(convert(varchar,ClientState),'') as ClientState,
       nullif(convert(varchar,ClientPostcode),'') as ClientPostcode,
       nullif(convert(varchar,ClientCountry),'') as ClientCountry,
       nullif(convert(varchar,ClientPOAddress),'') as ClientPOAddress,
       nullif(convert(varchar,ClientPOAddress2),'') as ClientPOAddress2,
       nullif(convert(varchar,ClientPOCity),'') as ClientPOCity,
       nullif(convert(varchar,ClientPOState),'') as ClientPOState,
       nullif(convert(varchar,ClientPOPostcode),'') as ClientPOPostcode,
       nullif(convert(varchar,ClientPOCountry),'') as ClientPOCountry,
       nullif(convert(varchar,ClientOnHold),'') as ClientOnHold,
       nullif(convert(varchar,ClientReasonOnHold),'') as ClientReasonOnHold,
       nullif(convert(varchar,ClientInfo),'') as ClientInfo,
       t.CTName
from Client c
left join tblClientType t on t.ClientTypeID = c.ClientTypeID"""
company = pd.read_sql(sql, engine_mssql)
company['company_externalid'] =company['company_externalid'].apply(lambda x: str(x) if x else x)
assert False

# %% HQ address
hq_address = company[['company_externalid','ClientAddress', 'ClientAddress2', 'ClientCity', 'ClientState', 'ClientPostcode','ClientCountry']]
hq_address['address'] = hq_address[['ClientAddress', 'ClientAddress2', 'ClientCity', 'ClientState', 'ClientPostcode','ClientCountry']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
hq_address['location_name'] = hq_address['address']
hq_address = hq_address.loc[hq_address['address']!='']
cp2 = vcom.insert_company_location_2(hq_address, dest_db, mylog)
# %% city
hq_address['city'] = hq_address['ClientCity']
cp3 = vcom.update_location_city_2(hq_address, dest_db, mylog)

# %% postcode
hq_address['post_code'] = hq_address['ClientPostcode']
cp4 = vcom.update_location_post_code_2(hq_address, dest_db, mylog)

# %% state
hq_address['state'] = hq_address['ClientState']
cp5 = vcom.update_location_state_2(hq_address, dest_db, mylog)

# %% country
hq_address['country_code'] = hq_address.ClientCountry.map(vcom.get_country_code)
hq_address['country'] = hq_address.ClientCountry
cp6 = vcom.update_location_country_2(hq_address, dest_db, mylog)

# %% location type
hq_address['location_type']='HEADQUARTER'
cp7 = vcom.update_location_type(hq_address, dest_db, mylog)

# %% mailing address
m_address = company[['company_externalid','ClientPOAddress', 'ClientPOAddress2', 'ClientPOCity', 'ClientPOState', 'ClientPOPostcode','ClientPOCountry']]
m_address['address'] = m_address[['ClientPOAddress', 'ClientPOAddress2', 'ClientPOCity', 'ClientPOState', 'ClientPOPostcode','ClientPOCountry']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
m_address['location_name'] = m_address['address']
m_address = m_address.loc[m_address['address']!='']
cp2 = vcom.insert_company_location_2(m_address, dest_db, mylog)
# %% city
m_address['city'] = m_address['ClientPOCity']
cp3 = vcom.update_location_city_2(m_address, dest_db, mylog)

# %% postcode
m_address['post_code'] = m_address['ClientPOPostcode']
cp4 = vcom.update_location_post_code_2(m_address, dest_db, mylog)

# %% state
m_address['state'] = m_address['ClientPOState']
cp5 = vcom.update_location_state_2(m_address, dest_db, mylog)

# %% country
m_address['country_code'] = m_address.ClientPOCountry.map(vcom.get_country_code)
m_address['country'] = m_address.ClientPOCountry
cp6 = vcom.update_location_country_2(m_address, dest_db, mylog)

# %% location type
m_address['location_type']='MAILING_ADDRESS'
cp7 = vcom.update_location_type(m_address, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'ClientTelephone']].dropna()
tem['switch_board'] = tem['ClientTelephone']
tem['phone'] = tem['ClientTelephone']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %% website
tem = company[['company_externalid', 'ClientWWWSite']].dropna()
tem['website'] = tem['ClientWWWSite']
cp5 = vcom.update_website(tem, mylog)

# %% fax
tem = company[['company_externalid', 'ClientFax']].dropna()
tem['fax'] = tem['ClientFax']
cp5 = vcom.update_fax(tem, mylog)

# %% parent company
sql ="""select cg.ClientGroupHead as company_externalid
     , c.ClientID from ClientGroup cg
left join Client C on cg.ClientGroupName = C.ClientName"""
tem = pd.read_sql(sql, engine_mssql)
tem = tem.dropna()
tem['parent_externalid'] = tem['ClientID']
tem['parent_externalid'] =tem['parent_externalid'].apply(lambda x: str(x) if x else x)
tem['company_externalid'] =tem['company_externalid'].apply(lambda x: str(x).split('.')[0] if x else x)
cp5 = vcom.update_parent_company(tem, mylog)

# %% reg date
tem = company[['company_externalid', 'ClientCreateDate']]
tem['reg_date'] = pd.to_datetime(tem['ClientCreateDate'])
vcom.update_reg_date(tem, mylog)

# %% note
company.loc[company['ClientOnHold']=='1', 'ClientOnHold'] = 'Yes'
company.loc[company['ClientOnHold']=='0', 'ClientOnHold'] = 'No'
company['note'] = company[['company_externalid','CTName', 'ClientInfo', 'ClientOnHold', 'ClientReasonOnHold']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID', 'Client Rating', 'Information', 'On Hold', 'Reason On Hold'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(company, dest_db, mylog)

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