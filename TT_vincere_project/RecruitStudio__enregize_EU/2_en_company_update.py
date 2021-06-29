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
cf.read('en_config.ini')
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
sql ="""select c.CompanyId as company_externalid
     , nullif(c.CompanyName,'') as CompanyName
     , nullif(Address1,'') as Address1
     , nullif(Address2,'') as Address2
     , nullif(Address3,'') as Address3
     , nullif(a.City,'') as City
     , nullif(a.Postcode,'') as Add_Postcode
     , nullif(County,'') as County
     , nullif(a.Country,'') as Country
     , nullif(c.Postcode,'') as Postcode
     , nullif(c.Location,'') as Location
     , nullif(c.SubLocation,'') as SubLocation
     , nullif(c.Latitude,'') as Latitude
     , nullif(c.Longitude,'') as Longitude
     , nullif(a.TelNo,'') as location_tel
     , nullif(a.Email,'') as location_email
     , nullif(c.TelNo,'') as TelNo
     , nullif(c.Email,'') as Email
     , nullif(WebSite,'') as WebSite
     , nullif(ParentCompanyId,'') as ParentCompanyId
     , nullif(CompanyRegNo,'') as CompanyRegNo
     , nullif(HeadCount,'') as HeadCount
     , nullif(Description,'') as About
     , u.UserName as LastUser
     , t.DateAgreed
     , t.ReviewDate
     , t.Fee
     , t.PaymentTerms
     , t.RebatePeriod
     , t.ReferenceNo
     , t.Notes
from Companies c
left join dbo.addresses a on a.contactid = c.companyid
left join ClientTerms t on t.contactid = c.companyid
left join Users u on u.UserId = c.LastUser"""
company = pd.read_sql(sql, engine_mssql)
company['company_externalid'] = 'EUK'+company['company_externalid']
assert False

c_prod = pd.read_sql("""select id, name from company""", engine_postgre_review)
c_prod['name'] = c_prod['name'].apply(lambda x: x.replace('_Energize-UK',''))
vincere_custom_migration.psycopg2_bulk_update_tracking(c_prod, vcom.ddbconn, ['name'], ['id'], 'company', mylog)
# vincere_custom_migration.load_data_to_vincere(c_prod, vcom.ddbconn, ['name'], ['id'], 'company', mylog)

# %% billing address
company['address'] = company[['Address1', 'Address2', 'Address3', 'City', 'Add_Postcode','County','Country']] \
    .apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company['location_name'] = company[['Postcode', 'SubLocation', 'Location']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company.loc[df['company_externalid']=='786234-8214-10349']
cp2 = vcom.insert_company_location_2(company, dest_db, mylog)
# df=company
# logger=mylog
# conn_param=dest_db
# company_location = pd.read_sql("""
#                     select
#                     c.external_id as company_externalid
#                     , c.id as company_id
#                     , cl.id
#                     , cl.address
#                     from company c
#                     join company_location cl on c.id = cl.company_id;
#                     """, vcom.ddbconn)
# tem2 = df[['company_externalid','address']]
# tem2['address1']=tem2['address']
# tem2.address = tem2.address.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
# tem2.address = tem2.address.map(lambda x: ', '.join([e.strip() for e in x if e]))
# # transform data
# tem2 = tem2.merge(company_location, on=['company_externalid', 'address'])
# tem3 = tem2[['id','address1']].rename(columns={'address1':'address'})
# # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['city', ], ['id', ], 'company_location', logger)
# vincere_custom_migration.load_data_to_vincere(tem3, conn_param, 'update', 'company_location', ['address', ], ['id'], logger)

# %% city
company['city'] = company['City']
cp3 = vcom.update_location_city_2(company, dest_db, mylog)

# %% postcode
company['post_code'] = company['Add_Postcode']
cp4 = vcom.update_location_post_code_2(company, dest_db, mylog)

# %% state
# company['state'] = company['County']
# cp5 = vcom.update_location_state_2(company, dest_db, mylog)
company['district'] = company['County']
cp5 = vcom.update_location_district_2(company, dest_db, mylog)

# %% country
company['country_code'] = company.Country.map(vcom.get_country_code)
company['country'] = company.Country
cp6 = vcom.update_location_country_2(company, dest_db, mylog)

# %% location note
company['location_note'] = company[['location_tel', 'location_email']].apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Tel No', 'Email'], x) if e[1]]), axis=1)
tem = company[['company_externalid', 'address', 'location_note']]
tem = tem.loc[tem['location_note'] != '']
cp6 = vcom.update_location_note(tem, dest_db, mylog)

# %% location type
tem = company[['company_externalid', 'address']].dropna()
tem['location_type']='HEADQUARTER'
cp6 = vcom.update_location_type(tem, dest_db, mylog)

# %% latitude longitude
tem = company[['company_externalid', 'address', 'Latitude', 'Longitude']]
tem = tem.loc[tem['Latitude'].notnull()]
tem = tem.loc[tem['Longitude'].notnull()]
tem['latitude'] = tem['Latitude'].apply(lambda x: str(x).strip() if x else x)
tem['longitude'] = tem['Longitude'].apply(lambda x: str(x).strip() if x else x)
tem['latitude'] = tem['latitude'].astype(float)
tem['longitude'] = tem['longitude'].astype(float)
tem['longitude'].unique()
cp6 = vcom.update_location_latlong(tem, dest_db, mylog)

# %% phone / switchboard
tem = company[['company_externalid', 'TelNo']].dropna()
tem['switch_board'] = tem['TelNo']
tem['phone'] = tem['TelNo']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

# %% website
tem = company[['company_externalid', 'WebSite']].dropna()
tem['website'] = tem['WebSite']
cp5 = vcom.update_website(tem, mylog)

# %% employees_number
tem = company[['company_externalid', 'HeadCount']].dropna()
tem['employees_number'] = tem['HeadCount'].astype(int)
cp5 = vcom.update_employees_number(tem, mylog)

# %% parent company
tem = company[['company_externalid', 'ParentCompanyId']].dropna()
tem['parent_externalid'] = tem['ParentCompanyId']
tem['parent_externalid'] = 'EUK'+tem['parent_externalid']
cp5 = vcom.update_parent_company(tem, mylog)

# %% company number
tem = company[['company_externalid', 'CompanyRegNo']].dropna()
tem['company_number'] = tem['CompanyRegNo']
cp5 = vcom.update_company_number(tem, dest_db, mylog)

# %% company_payment_term
tem = company[['company_externalid', 'PaymentTerms']].dropna()
tem['company_payment_term'] = tem['PaymentTerms'].apply(lambda x: str(x) if x else x)
cp5 = vcom.update_company_payment_term(tem, dest_db, mylog)

# %% note
company = company.where(company.notnull(), None)
# company['PaymentTerms'] = company['PaymentTerms'].apply(lambda x: str(x) if x else x)
company['DateAgreed'] = company['DateAgreed'].apply(lambda x: str(x))
company['DateAgreed'] = company['DateAgreed'].apply(lambda x: x.replace('NaT',''))
company['note'] = company[['About','Email', 'LastUser', 'DateAgreed', 'ReviewDate', 'Fee','RebatePeriod','ReferenceNo','Notes']]\
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Description', 'Email', 'Last User', 'Terms-Date Agreed', 'Terms-Review Date'
                                                           , 'Terms-Fee', 'Terms-Rebate Period'
                                                           , 'Terms-ReferenceNo', 'Terms-Notes'], x) if e[1]]), axis=1)
# company.to_csv(os.path.join(standard_file_upload, 'dt_out.csv'), index=False)
vcom.update_note_2(company, dest_db, mylog)

# %% industry
sql ="""select skc.* from
(select sc.ObjectId, s.Sector from SectorInstances sc
left join Sectors s on s.SectorId = sc.SectorId) skc
join Companies c on c.CompanyId = skc.ObjectId"""
industry = pd.read_sql(sql, engine_mssql)
industry['company_externalid'] = 'EUK'+industry['ObjectId']
industry['name'] = industry['Sector']
cp10 = vcom.insert_company_industry_subindustry(industry, mylog)

# tem2 = company_industries_2[['company_externalid','Sub Industry']].drop_duplicates().dropna()
# tem2['name'] = tem2['Sub Industry']
# cp10 = vcom.insert_company_sub_industry(tem2, mylog)

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