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
cf.read('psg_config.ini')
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
       ClientName, nullif(convert(varchar,ClientNameShort),'') as ClientNameShort,
       nullif(convert(varchar,ClientABN),'') as ClientABN,
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
       nullif(convert(varchar,BillingAddress),'') as BillingAddress,
       nullif(convert(varchar,BillingAddress2),'') as BillingAddress2,
       nullif(convert(varchar,BillingCity),'') as BillingCity,
       nullif(convert(varchar,BillingState),'') as BillingState,
       nullif(convert(varchar,BillingPostcode),'') as BillingPostcode,
       nullif(convert(varchar,BillingCountry),'') as BillingCountry,
       nullif(convert(varchar,BillingPhone),'') as BillingPhone,
       nullif(convert(varchar,ClientOnHold),'') as ClientOnHold,
       nullif(convert(varchar,ClientReasonOnHold),'') as ClientReasonOnHold,
       nullif(convert(nvarchar(max),ClientInfo),'') as ClientInfo,
       t.CTName, pt.Name as payment_term
from Client c
left join tblClientType t on t.ClientTypeID = c.ClientTypeID
left join PaymentTerm pt on pt.ID = c.PaymentTermID"""
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

# %% billing address
b_address = company[['company_externalid','BillingAddress', 'BillingAddress2', 'BillingCity', 'BillingState', 'BillingPostcode','BillingCountry','BillingPhone','payment_term']]
b_address['address'] = b_address[['BillingAddress', 'BillingAddress2', 'BillingCity', 'BillingState', 'BillingPostcode','BillingCountry']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
b_address['location_name'] = b_address['address']
b_address = b_address.loc[b_address['address']!='']
cp2 = vcom.insert_company_location_2(b_address, dest_db, mylog)
# %% city
b_address['city'] = b_address['BillingCity']
cp3 = vcom.update_location_city_2(b_address, dest_db, mylog)

# %% postcode
b_address['post_code'] = b_address['BillingPostcode']
cp4 = vcom.update_location_post_code_2(b_address, dest_db, mylog)

# %% state
b_address['state'] = b_address['BillingState']
cp5 = vcom.update_location_state_2(b_address, dest_db, mylog)

# %% phone
b_address['phone_number'] = b_address['BillingPhone']
cp5 = vcom.update_location_phone(b_address, dest_db, mylog)

# %% payment
b_address['company_payment_term'] = b_address['payment_term']
cp5 = vcom.update_payment_term(b_address, dest_db, mylog)

# %% country
b_address['country_code'] = b_address.BillingCountry.map(vcom.get_country_code)
b_address['country'] = b_address.BillingCountry
cp6 = vcom.update_location_country_2(b_address, dest_db, mylog)

# %% location type
b_address['location_type']='BILLING_ADDRESS'
cp7 = vcom.update_location_type(b_address, dest_db, mylog)

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
# tem = company[['company_externalid', 'ClientFax']].dropna()
# tem['fax'] = tem['ClientFax']
# cp5 = vcom.update_fax(tem, mylog)

# %% parent company
# sql ="""select cg.ClientGroupHead as company_externalid
#      , c.ClientID from ClientGroup cg
# left join Client C on cg.ClientGroupName = C.ClientName"""
# tem = pd.read_sql(sql, engine_mssql)
# tem = tem.dropna()
# tem['parent_externalid'] = tem['ClientID']
# tem['parent_externalid'] =tem['parent_externalid'].apply(lambda x: str(x) if x else x)
# tem['company_externalid'] =tem['company_externalid'].apply(lambda x: str(x).split('.')[0] if x else x)
# cp5 = vcom.update_parent_company(tem, mylog)

# %% reg date
tem = company[['company_externalid', 'ClientCreateDate']].dropna()
tem['reg_date'] = pd.to_datetime(tem['ClientCreateDate'])
vcom.update_reg_date(tem, mylog)

# %% note
company['note'] = company[['company_externalid','CTName', 'ClientInfo', 'ClientNameShort', 'ClientABN']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['TRIS ID', 'Type', 'Information', 'Short name', 'Financial Y/E'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(company, dest_db, mylog)

# %% name
c_name = pd.read_sql("""select id, name from company""", engine_postgre_review)
c_name['name'] = c_name['name'].apply(lambda x: x.split('_')[0])
vincere_custom_migration.psycopg2_bulk_update_tracking(c_name, vcom.ddbconn, ['name', ], ['id', ], 'company', mylog)

# %% industry
tem = pd.read_sql("""
select IndustryName as name from Industry
""", engine_mssql)
from common import vincere_candidate
import datetime
vcand = vincere_candidate.Candidate(connection)
tem['insert_timestamp'] = datetime.datetime.now()
vcand.insert_industry(tem, mylog)

sql = """
select ClientID as company_externalid,
       ClientName, i.IndustryName
from Client c
left join Industry i on c.IndustryID = i.IndustryID
"""
company_industries = pd.read_sql(sql, engine_mssql)
company_industries['company_externalid'] = company_industries['company_externalid'].apply(lambda x: str(x) if x else x)

company_industries['name'] = company_industries['IndustryName']
company_industries = company_industries.drop_duplicates().dropna()
cp10 = vcom.insert_company_industry(company_industries, mylog)