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
cf.read('mc_config.ini')
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
job_info = pd.read_sql("""
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
select concat('', Reference) as job_externalid
     , nullif(trim(Detail),'') as Detail
     , nullif(trim(Value),'') as Value
     , nullif(trim(FactoidGroup),'') as FactoidGroup
     , System
     , Reference_Type
from Further_detail f
where 1=1""", engine_mssql)
# assert False
# %% AWR Basic Salary
tem = job_info.query('Detail == "Comp employee basic sal" and FactoidGroup=="AWR info - info re comparable employees"')
tem = tem.drop_duplicates()
api='1c678fb88620cdd0db17fee53ba73c4d'
tem['Value'] = tem['Value'].astype(str)
vincere_custom_migration.insert_job_text_field_values(tem, 'job_externalid', 'Value', api, connection)

# %%  AWR Other Renumeration
# tem = job_info.query('Detail == "Other remuneration details" and FactoidGroup=="AWR info - info re comparable employees"')
# tem = tem.drop_duplicates()
# api='d8e752c327c9e9e98f47dacd3330381b'
# vincere_custom_migration.insert_job_text_field_values(tem, 'job_externalid', 'Value', api, connection)

# %% AWR Overtime Payments & qualification criteria
tem = job_info.query('Detail == "Overtime payments + qual criteria" and FactoidGroup=="AWR info - info re comparable employees"')
tem = tem.drop_duplicates()
api='ce248e0b45e69041b88caf308deb92fc'
vincere_custom_migration.insert_job_text_field_values(tem, 'job_externalid', 'Value', api, connection)

# %% AWR Shift allowwance criteria
tem = job_info.query('Detail == "Shift allowance + criteria" and FactoidGroup=="AWR info - info re comparable employees"')
tem = tem.drop_duplicates()
api='9ff630115ffd9a7d61e80ab6ec6b1474'
vincere_custom_migration.insert_job_text_field_values(tem, 'job_externalid', 'Value', api, connection)

# %% AWR Standard Org Working Hours P.W.
tem = job_info.query('Detail == "standard org working hours p.w." and FactoidGroup=="AWR info - info re comparable employees"')
tem = tem.drop_duplicates()
api='4f6c7788ca59826bd4e53be72bc062ed'
vincere_custom_migration.insert_job_text_field_values(tem, 'job_externalid', 'Value', api, connection)

# %% AWR Total Number of Days A/L
tem = job_info.query('Detail == "Total number of days A/ L entitlement inc b/hols" and FactoidGroup=="AWR info - info re comparable employees"')
tem = tem.drop_duplicates()
api='359f0345a9b37b6418fd47503634eff4'
vincere_custom_migration.insert_job_text_field_values(tem, 'job_externalid', 'Value', api, connection)

# %% AWR Entitlement of Days A/L
tem = job_info.query('Detail == "Total number of days A/ L entitlement inc b/hols" and FactoidGroup=="AWR info - info re comparable employees"')
tem = tem.drop_duplicates()
api='7079b7c9b5e2ec1523a3566abb2db854'
vincere_custom_migration.insert_job_text_field_values(tem, 'job_externalid', 'Value', api, connection)


