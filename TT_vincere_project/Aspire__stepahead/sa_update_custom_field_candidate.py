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
cf.read('sa_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
src_db = cf[cf['default'].get('src_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
# %%
candidate_info = pd.read_sql("""
with Further_detail AS (
Select b.Reference, a.Reference as Detail_Reference, Detail, Value, 'S' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from string_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 1
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'N' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Number_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 2
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, Convert(Char(10), Value, 103) as Value, 'D' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Date_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 3
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'M' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Money_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 4
-- and System = '0'

union

Select b.Reference, a.Reference as Detail_Reference, Detail, Case Value when 1 then 'True' else 'False' end as Value, 'B' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Boolean_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 5
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'F' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Float_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 8
-- and System = '0'
)
select concat('', Reference) as candidate_externalid
     , nullif(trim(Detail),'') as Detail
     , nullif(trim(Value),'') as Value
     , nullif(trim(FactoidGroup),'') as FactoidGroup
     , System
     , Reference_Type
from Further_detail f
join Candidate_Search_View csv on csv.Person_Reference = f.Reference
where 1=1""", engine_mssql)
# assert False

# %% DBS Check
tem = candidate_info.query('Detail == "Date of DBS check"')
tem = tem.drop_duplicates()
api='2996c0bdf440f17daa350d48f9ed80e5'
tem['Value'] = pd.to_datetime(tem['Value'])
vincere_custom_migration.insert_candidate_date_field_values(tem, 'candidate_externalid', 'Value', api, connection)

# %% DBS Expiry
tem = candidate_info.query('Detail == "DBS Expiry"')
tem = tem.drop_duplicates()
api='fdca0679310160fdf891f0030304d764'
tem['Value'] = pd.to_datetime(tem['Value'])
vincere_custom_migration.insert_candidate_date_field_values(tem, 'candidate_externalid', 'Value', api, connection)

# %% P45 Issued
tem = candidate_info.query('Detail == "P45 Issued"')
tem = tem.drop_duplicates()
api='ebf906afba8937644d2cde6574440149'
tem['Value'] = pd.to_datetime(tem['Value'])
vincere_custom_migration.insert_candidate_date_field_values(tem, 'candidate_externalid', 'Value', api, connection)

# %% Nature of Disability
tem = candidate_info.query('Detail == "Nature of Disability"')
tem = tem.drop_duplicates()
api='967c1ec6fe5bba38cb766b35e3d53b2f'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'Value', api, connection)

# %% Ethnic Origin
tem = candidate_info.query('Detail == "Ethnic Origin"')
tem = tem.drop_duplicates()
api='ec86c21ace8fbd312ede97ba4a519bd3'
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'Value', api, connection)

# %%
candidate_info = pd.read_sql("""
with Further_detail AS (
Select b.Reference, a.Reference as Detail_Reference, Detail, Value, 'S' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from string_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 1
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'N' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Number_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 2
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, Convert(Char(10), Value, 103) as Value, 'D' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Date_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 3
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'M' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Money_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 4
-- and System = '0'

union

Select b.Reference, a.Reference as Detail_Reference, Detail, Case Value when 1 then 'True' else 'False' end as Value, 'B' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Boolean_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 5
-- and System = '0'

Union

Select b.Reference, a.Reference as Detail_Reference, Detail, LTrim(RTrim(Convert(Char,Value))) as Value, 'F' as datatype, ISNULL(c.Description, 'Unknown') as FactoidGroup, System, Reference_Type from Float_details a
Inner join Details_Lookup b
on a.Reference = b.Detail_Reference
Left Outer Join Detail_group c
on b.Detail_Group = c.Reference
Where 1=1
-- and b.Reference_Type = 2
and b.Detail_Type = 8
-- and System = '0'
)
select concat('', Reference) as candidate_externalid
     , nullif(trim(Detail),'') as Detail
     , nullif(trim(Value),'') as Value
     , nullif(trim(FactoidGroup),'') as FactoidGroup
     , System
     , Reference_Type
from Further_detail f
where 1=1
and Detail like '%Conviction Details%'""", engine_mssql)
map = pd.read_csv('CONVICTION DETAILS MAPPING.csv')

cs_value = pd.read_csv('tem.csv')
# %% Conviction Details
tem = cs_value[['candidate_externalid','CUSTOM FIELD > Conviction Details > YES/NO']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api='d8992a8001927bb9adba0366a0a4fb1b'
tem['CUSTOM FIELD > Conviction Details > YES/NO'] = tem['CUSTOM FIELD > Conviction Details > YES/NO'].apply(lambda x: x.lower())
tem['candidate_externalid'] = tem['candidate_externalid'].apply(lambda x: str(x))
vincere_custom_migration.insert_candidate_drop_down_list_values(tem, 'candidate_externalid', 'CUSTOM FIELD > Conviction Details > YES/NO', api, connection)

# %% Conviction Spent / Unspent
# tem = candidate_info.query('Detail == "Contract Signed"')
# tem = tem.drop_duplicates()
# api='5ed1bb5794291849ad2b9413402ff9a1'
# tem.loc[tem['Value']=='False', 'Value'] = 'No'
# tem.loc[tem['Value']=='True', 'Value'] = 'Yes'
# vincere_custom_migration.insert_candidate_drop_down_list_values(tem, 'candidate_externalid', 'Value', api, connection)

# %% Conviction Description if UNSPENT
tem = cs_value[['candidate_externalid','CUSTOM FIELD > Conviction Description if UNSPENT > Free Text']].dropna().drop_duplicates()
tem = tem.drop_duplicates()
api='662ad25aad707e8960861e49db68ef36'
tem['candidate_externalid'] = tem['candidate_externalid'].apply(lambda x: str(x))
vincere_custom_migration.insert_candidate_text_field_values(tem, 'candidate_externalid', 'CUSTOM FIELD > Conviction Description if UNSPENT > Free Text', api, connection)



