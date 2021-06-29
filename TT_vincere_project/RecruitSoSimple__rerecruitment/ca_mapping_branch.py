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
assert False
# %% company
com_branch = pd.read_sql("""select Client_Id, b.Description as name
 from Client c
left join Branch b on c.Branch_Id = b.Branch_Id
where b.Description is not null
and Client_Id in (
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
,4642)"""
,engine_mssql)
com_branch['company_externalid'] = com_branch['Client_Id'].astype(str)
# assert False
from common import vincere_company
vcon = vincere_company.Company(connection)
vcon.insert_company_branch(com_branch, mylog)

# %% candidate
cand_branch = pd.read_sql("""select Candidate_Id, b.Description as name from Candidate c
left join Branch b on c.Branch_Id = b.Branch_Id
where b.Description is not null
and Candidate_Id in (
39367
,2896
,37207
,22106
,25798
,35819
,50227
,48100
,46300
,47257)
""",engine_mssql)
cand_branch['candidate_externalid'] = cand_branch['Candidate_Id'].astype(str)
# assert False
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)
vcand.insert_candidate_branch(cand_branch, mylog)

# %% job
job_branch = pd.read_sql("""
select br.Role_Id, b.Description as name
from Booking_Role br
join Booking bo on br.Booking_Id = bo.Booking_Id
left join Branch b on bo.Branch_Id = b.Branch_Id
where b.Description is not null
and br.Role_Id in (
8547
,8467
,8696
,8473
,8490
,8678
,8710
,8849
,8508
,8626
,8627
,8655
,8656
,8669
,8708
,8514
,8544)
""",engine_mssql)
job_branch['job_externalid'] = 'BK'+job_branch['Role_Id'].astype(str)
# assert False
from common import vincere_job
vjob = vincere_job.Job(connection)
vjob.insert_job_branch(job_branch, mylog)