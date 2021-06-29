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
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %% candidate
company = pd.read_sql("""
select Client_Id, i.Description as name, Industry_Code
from Client c
left join Industry i on c.Industry_Id = i.Industry_Id
where i.Description is not null
""", engine_mssql)
company['company_externalid'] = company['Client_Id'].astype(str)
# assert False
from common import vincere_company
vcom = vincere_company.Company(connection)
cp1 = vcom.insert_company_industry(company, mylog)

# %% job
job_v = pd.read_sql("""
select Role_Id, i.Description as name
from Vacancy v
left join Vacancy_Role vr on vr.Vacancy_Id = v.Vacancy_Id
left join Industry i on v.Industry_Id = i.Industry_Id
where Role_Id is not null and i.Description is not null
""", engine_mssql)
job_v['Role_Id'] = 'VC'+job_v['Role_Id'].astype(str)

job_b = pd.read_sql("""
select Role_Id, i.Description as name
from Booking b
left join Booking_Role br on br.Booking_Id = b.Booking_Id
left join Industry i on b.Industry_Id = i.Industry_Id
where Role_Id is not null and i.Description is not null
""", engine_mssql)
job_b['Role_Id'] = 'BK'+job_b['Role_Id'].astype(str)
job = pd.concat([job_v,job_b])
job['job_externalid'] = job['Role_Id']
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
cp2 = vjob.insert_job_industry_subindustry(job, mylog, True)
