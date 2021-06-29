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
cf.read('sa_config.ini')
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

# %% swift code
candidate = pd.read_sql("""
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
--join Candidate_Search_View csv on csv.Person_Reference = f.Reference
where 1=1""", engine_mssql)
assert False
# %% Swift code
api = '7a7dd5cff3727c23713f1df621c994c1'
tem = candidate.query('Detail == "Sort Code"')
cand = tem[['candidate_externalid', 'Value']]
cand = cand.drop_duplicates()
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'Value', api, connection)

# %% bank branch
api = '7dede9d4a5a3a221c9282f28fa2a1060'
tem = candidate.query('Detail == "Sort Code"')
cand = tem[['candidate_externalid', 'Value']]
cand = cand.drop_duplicates()
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'Value', api, connection)

# %% National Insurance number
api = '1ef7ecc7a33336f1f5845a9792da42a0'
tem = candidate.query('Detail == "NI Number"')
cand = tem[['candidate_externalid', 'Value']]
cand = cand.drop_duplicates()
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'Value', api, connection)

# %% Account number
api = 'fef66dceae1990e17b4aa82a9e83b4dd'
tem = candidate.query('Detail == "Account Number"')
cand = tem[['candidate_externalid', 'Value']]
cand = cand.drop_duplicates()
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'Value', api, connection)

# %% Account Name
api = '1d41afc718c0d6914a732c0d0a2e2c65'
tem = candidate.query('Detail == "Account Name"')
cand = tem[['candidate_externalid', 'Value']]
cand = cand.drop_duplicates()
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'Value', api, connection)

# %% BSB number
api = '26943cc360f9186c1cb40389a45428a5'
tem = candidate.query('Detail == "Building Society Number"')
cand = tem[['candidate_externalid', 'Value']]
cand = cand.drop_duplicates()
vincere_custom_migration.insert_candidate_text_field_values(cand, 'candidate_externalid', 'Value', api, connection)


