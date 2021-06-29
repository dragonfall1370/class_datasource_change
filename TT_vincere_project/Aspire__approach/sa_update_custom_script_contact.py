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
com_info = pd.read_sql("""
select concat('',com.Reference) as company_externalid from Legal_Requirement_Lookup lrl
join Legal_Requirement lr on lr.Reference = lrl.Reference
join (select
Reference
, Company_Name
 from (select Company_Name, Company.Reference, Client.Reference as client_ref from Company
    join Client on Client.Company_Reference = Company.Reference) c
join(
select distinct Company_Reference
From Diary_Entry a
Inner join diary_entry_blueprint d
on a.Blueprint_Reference = d.Reference
Inner join type_description e
on d.Diary_type = e.reference
Inner join Diary_Entry_Lookup f
on a.Reference = f.Reference
and f.Entity_Type = 4
Inner join DB_Client_Basic_Details g
on f.Entity_Reference = g.Company_Reference
Where consultant_reference is not null
and e.type = 'Diary'
and Created between '2016-10-01 00:00:00' and '2020-10-01 00:00:00'
and g.Active = 'Active') c1 on c.client_ref = c1.Company_Reference) com on Entity_reference = com.Reference
where Requirement = 'Signed TOBs received          '""", engine_mssql)

contact = pd.read_sql("""select com.external_id, c.id from company com
join contact c on com.id = c.company_id
where com.deleted_timestamp is null and c.deleted_timestamp is null and com.external_id is not null""", connection)


tem = contact.loc[contact['external_id'].isin(com_info['company_externalid'])]
tem['board'] = 4
tem['status'] = 1
vincere_custom_migration.psycopg2_bulk_update_tracking(tem, connection, ['board','status'], ['id'], 'contact', mylog)

