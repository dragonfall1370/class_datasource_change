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
cf.read('ca_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
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
brand = pd.read_csv('brand.csv')
assert False
# %% company
com_brand = pd.read_sql("""select Client_Id, d.Description
 from Client c
left join Division d on c.Division_Id = d.Division_Id
where d.Description is not null"""
,engine_mssql)
com_brand['company_externalid'] = com_brand['Client_Id'].astype(str)
com_brand = com_brand.merge(brand, left_on='Description', right_on='Gel value')
com_brand['name'] = com_brand['Vincere value']
# assert False
from common import vincere_company
vcon = vincere_company.Company(connection)
vcon.insert_company_brand(com_brand, mylog)

# %% candidate
cand_brand = pd.read_sql("""select Candidate_Id, d.Description
from Candidate c
left join Division d on c.Division_Id = d.Division_Id
where d.Description is not null
""",engine_mssql)
cand_brand['candidate_externalid'] = cand_brand['Candidate_Id'].astype(str)
cand_brand = cand_brand.merge(brand, left_on='Description', right_on='Gel value')
cand_brand = cand_brand.drop_duplicates()
cand_brand['name'] = cand_brand['Vincere value']
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_candidate_brand(cand_brand, mylog)

# %% job
booking_brand = pd.read_sql("""
select br.Role_Id, d.Description
from Booking_Role br
join Booking bo on br.Booking_Id = bo.Booking_Id
left join Division d on bo.Division_Id = d.Division_Id
where d.Description is not null
""",engine_mssql)
booking_brand['job_externalid'] = 'BK'+booking_brand['Role_Id'].astype(str)

vacancy_brand = pd.read_sql("""
select vr.Role_Id, d.Description
from Vacancy_Role vr
left join Vacancy v on vr.Vacancy_Id = v.Vacancy_Id
left join Division d on v.Division_Id = d.Division_Id
where d.Description is not null
""",engine_mssql)
vacancy_brand['job_externalid'] = 'VC'+vacancy_brand['Role_Id'].astype(str)
job_brand = pd.concat([booking_brand,vacancy_brand])
job_brand = job_brand.merge(brand, left_on='Description', right_on='Gel value')
job_brand = job_brand.drop_duplicates()
job_brand['name'] = job_brand['Vincere value']
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_job_brand(job_brand, mylog)