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
cf.read('ca_config.ini')
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
company = pd.read_sql("""select Client_Id,addr.* from Client_Address ca
join (select Address_Id, Line_1, Line_2, Line_3, c.Description as county, co.Description as country, t.Description as town, Postcode
from Address a
left join County c on a.County_Id = c.County_Id
left join Country co on a.Country_Id = co.Country_Id
left join Town t on a.Town_Id = t.Town_Id) addr on addr.Address_Id = ca.Address_Id
where Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)""", engine_mssql)
company['company_externalid'] = company['Client_Id'].astype(str)
company.loc[company['company_externalid']=='4642']
assert False
# %% location name/address
company['address'] = company[['Line_1', 'Line_2','Line_3','town','county','Postcode','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
company['location_name'] = company[['town','country']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
# %%
# assign contacts's addresses to their companies
comaddr = company[['company_externalid', 'address','location_name','Line_1', 'Line_2','Line_3','town','Postcode','county','country']].drop_duplicates()
comaddr = comaddr.loc[comaddr['address']!='']
cp1 = vcom.insert_company_location(comaddr, mylog)

# %% addr 1
tem = comaddr[['company_externalid', 'address', 'Line_1']].dropna().drop_duplicates()
tem['address_line1'] = tem.Line_1
cp3 = vcom.update_location_address_line1(tem, dest_db, mylog)

# %% addr 2
tem = comaddr[['company_externalid', 'address', 'Line_2','Line_3']].drop_duplicates()
tem['address_line2'] = tem[['Line_2','Line_3']].apply(lambda x: ', '.join([e for e in x if e]), axis=1)
cp3 = vcom.update_location_address_line2(tem, dest_db, mylog)

# %% city
tem = comaddr[['company_externalid', 'address', 'town']].dropna().drop_duplicates()
tem['city'] = tem.town
cp3 = vcom.update_location_city_2(tem, dest_db, mylog)

# %% postcode
tem = comaddr[['company_externalid', 'address', 'Postcode']].dropna().drop_duplicates()
tem['post_code'] = tem.Postcode
cp4 = vcom.update_location_post_code_2(tem, dest_db, mylog)

# %% state
tem = comaddr[['company_externalid', 'address', 'county']].dropna().drop_duplicates()
tem['state'] = tem.county
cp5 = vcom.update_location_state_2(tem, dest_db, mylog)

# %% country
tem = comaddr[['company_externalid', 'address', 'country']].dropna().drop_duplicates()
tem['country_code'] = tem.country.map(vcom.get_country_code)
tem['country'] = tem.country
tem.loc[tem['country_code']=='']
cp6 = vcom.update_location_country_2(tem, dest_db, mylog)

# %% location type
tem = comaddr[['company_externalid','address']]
tem['location_type'] = 'HEADQUARTER'
cp7 = vcom.update_location_types_array(tem, dest_db, mylog)

# %% location type
tem = comaddr[['company_externalid','address']]
tem['rn'] = tem.groupby('company_externalid').cumcount()
tem = tem.loc[tem['rn']==0]
tem['location_type'] = 'BILLING_ADDRESS'
cp7 = vcom.update_location_types_array(tem, dest_db, mylog)


company_info = pd.read_sql("""select Client_Id, Company_Reg_No, VAT_Reg_No, Perm_Payment_Terms from Client where Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)""", engine_mssql)
company_info['company_externalid'] = company_info['Client_Id'].astype(str)
# company_info = company_info.merge(tem, on='company_externalid', how = 'left')
# company_info = company_info.loc[company_info['address'].notnull()]

tem = company_info[['company_externalid','Company_Reg_No']].dropna()
tem['company_number'] = tem['Company_Reg_No']
tem2 = tem.merge(vcom.company, on=['company_externalid'])
tem2['company_id'] = tem2['id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['company_number', ], ['company_id', ],'company_location', mylog)
# vcom.update_company_number_location(tem, mylog)

tem = company_info[['company_externalid','VAT_Reg_No']].dropna()
tem['business_number'] = tem['VAT_Reg_No']
tem2 = tem.merge(vcom.company, on=['company_externalid'])
tem2['company_id'] =  tem2['id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['business_number', ], ['company_id', ],'company_location', mylog)
# vcom.update_business_number_tax(tem, mylog)

tem = company_info[['company_externalid','Perm_Payment_Terms']].dropna()
tem['company_payment_term'] = 'Perm Days: '+tem['Perm_Payment_Terms']
tem2 = tem.merge(vcom.company, on=['company_externalid'])
tem2['company_id'] =  tem2['id']
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['company_payment_term', ], ['company_id', ],'company_location', mylog)
# vcom.update_payment_term(tem, mylog)

# %% phone / switchboard
phone = pd.read_sql("""
    with max_id as(select cc.Client_Id, max(cast(cc.Contact_Info_Id as INT)) as Contact_Info_Id
from Client_Contact cc
left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
where Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
group by Client_Id)
select cc.Client_Id, cc.Contact_Info_Id, cc.Client_Contact_Id, Phone_Code_1, Phone_Number_1,Extension_1, Phone_Code_2, Phone_Number_2,Extension_2, Fax_Code, Fax_Number, Web_Address
from Client_Contact cc
left join Contact_Info ci on cc.Contact_Info_Id = ci.Contact_Info_Id
join max_id  on cc.Contact_Info_Id = max_id.Contact_Info_Id

""", engine_mssql)
phone['company_externalid'] = phone['Client_Id'].astype(str)
tem = phone[['company_externalid', 'Phone_Code_1','Phone_Number_1','Extension_1']]
tem['switch_board'] = tem[['Phone_Code_1','Phone_Number_1','Extension_1']].apply(lambda x: ' '.join([e for e in x if e]), axis=1)
tem['phone'] = tem['switch_board']
vcom.update_switch_board(tem, mylog)
vcom.update_phone(tem, mylog)

#  website
tem = phone[['company_externalid', 'Web_Address']].dropna()
tem['website'] = tem['Web_Address']
tem['website'] = tem['website'].apply(lambda x: x[:100])
tem = tem.loc[tem['website']!='']
cp5 = vcom.update_website(tem, mylog)

# %% note
note = pd.read_sql("""
select Client_Id, Client_Code, Notes from Client where Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
note['company_externalid'] = note['Client_Id'].astype(str)
note['note'] = note['Notes']
tem = note[['company_externalid','note']].dropna()
vcom.update_note_2(tem, dest_db, mylog)

# %% board
board = pd.read_sql("""
select Client_Id, Description
from Client c
left join (select code, Description from Lookup where Table_Name in ('CLIENT_STATUS')) s on s.Code = c.Status_Code
where Description in ('Current Clients','Prospective Clients')
and Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
board['company_externalid'] = board['Client_Id'].astype(str)
board.loc[board['Description'] == 'Current Clients', 'board'] = 4
board.loc[board['Description'] == 'Prospective Clients', 'board'] = 2
board['status'] = 1
board.loc[board['company_externalid']=='5532']
vcom.update_status_board(board, mylog)

board2 = pd.read_sql("""
select Client_Id, Description
from Client c
left join (select code, Description from Lookup where Table_Name in ('CLIENT_STATUS')) s on s.Code = c.Status_Code
where Description in ('DO NOT USE','Business Contacts','Closed Clients')
and Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
board2['company_externalid'] = board2['Client_Id'].astype(str)
board2['board'] = 1
board2['status'] = 2
vcom.update_status_board(board2, mylog)

# %% status
tem = pd.read_sql("""
select Client_Id, Description
from Client c
left join (select code, Description from Lookup where Table_Name in ('CLIENT_STATUS')) s on s.Code = c.Status_Code
where Description in ('DO NOT USE','Business Contacts','Closed Clients')
and Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
tem['company_externalid'] = tem['Client_Id'].astype(str)
tem.loc[tem['Description'] == 'DO NOT USE', 'name'] = 'Blacklisted'
tem.loc[tem['Description'] == 'Business Contacts', 'name'] = 'Passive'
tem.loc[tem['Description'] == 'Closed Clients', 'name'] = 'Do not contact'
vcom.add_company_status(tem, mylog)

# %% delete
tem = pd.read_sql("""
select Client_Id, Description
from Client c
left join (select code, Description from Lookup where Table_Name in ('CLIENT_STATUS')) s on s.Code = c.Status_Code
where Description in ('Closed Clients')
and Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
tem['company_externalid'] = tem['Client_Id'].astype(str)
tem = tem.merge(vcom.company, on=['company_externalid'])

activity = pd.read_sql("""select max(insert_timestamp), company_id from activity_company group by company_id""", connection)
activity = activity.loc[activity['max']<'2020-01-01 00:00:00']

tem2 = tem.merge(activity, left_on='id', right_on='company_id')
tem2['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, vcom.ddbconn, ['deleted_timestamp', ], ['id', ], 'company', mylog)

# %% delete
tem = pd.read_sql("""
select Client_Id, Description
from Client c
left join (select code, Description from Lookup where Table_Name in ('CLIENT_STATUS')) s on s.Code = c.Status_Code
where Description in ('Do Not Transfer')
and Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
tem['company_externalid'] = tem['Client_Id'].astype(str)

tem2 = pd.read_sql("""
select Client_Id, Description
from Client c
left join Division d on c.Division_Id = d.Division_Id
where Description in ('yCASTLE HR & EMP LAW CLIENTS')
    and Client_Id not in (
4
,44
,61
,110
,294
,887
,1733
,1925
,2960
,3010
,4497
,4642)
""", engine_mssql)
tem2['company_externalid'] = tem2['Client_Id'].astype(str)
tem3 = pd.concat([tem, tem2])
tem4 = tem3[['company_externalid']].drop_duplicates()
tem4 = tem4.merge(vcom.company, on=['company_externalid'])
tem4['deleted_timestamp'] = datetime.datetime.now()
vincere_custom_migration.psycopg2_bulk_update_tracking(tem4, vcom.ddbconn, ['deleted_timestamp', ], ['id', ], 'company', mylog)