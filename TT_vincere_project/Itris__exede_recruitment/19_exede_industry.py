# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import pymssql
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
cf.read('exede_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
fr = cf[cf['default'].get('src_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre.raw_connection()

from common import vincere_placement_detail
import importlib
importlib.reload(vincere_placement_detail)
vpd = vincere_placement_detail.PlacementDetail(connection)

# %% get data
companyType = pd.read_sql("""
select 
* 
from ztungtem_clienttable_company_type
where val is not null
order by val
""", engine_postgre)


from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

from common import vincere_company
vcom = vincere_company.Company(connection)

from common import vincere_contact
vcont = vincere_contact.Contact(connection)

from common import vincere_job
vjob = vincere_job.Job(connection)

assert False
# %%
indus = pd.concat([
    companyType[['val']],
]).dropna().drop_duplicates()
indus['name'] = indus['val']
indus['insert_timestamp'] = datetime.datetime.now()
vcand.append_industry(indus, mylog)

companyType['name'] = companyType.val
vcom.insert_company_industry(companyType, mylog)
