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
cf.read('pa_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
src_db = cf[cf['default'].get('src_db')]
dest_db = cf[cf['default'].get('dest_db')]

mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
conn_str_ddb_bkup = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format('tony', 'Yz0t@d4Ta', '35.158.17.10', '25432', 'vivotalent.vincere.io')
engine_postgre_bkup = sqlalchemy.create_engine(conn_str_ddb_bkup, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_bkup.raw_connection()
# %%
from common import vincere_candidate
vcand = vincere_candidate.Candidate(connection)

from common import vincere_company
vcomp = vincere_company.Company(connection)

candidate_location = pd.read_csv(r'D:\Tony\project\vivotalent\vivotalent_candidate_location.csv', encoding='cp1252')
company_location = pd.read_csv(r'D:\Tony\project\vivotalent\vivotalent_company_location.csv')
assert False
candidate_location = candidate_location.loc[candidate_location['common_location_id'].notnull()]
candidate_location['id'] = candidate_location['common_location_id'].apply(lambda x: int(str(x).split('.')[0]) if x else x)
candidate_location['latitude'] = candidate_location['Latitude']
candidate_location['longitude'] = candidate_location['Longitude']
vincere_custom_migration.psycopg2_bulk_update_tracking(candidate_location, connection, ['latitude', 'longitude', ], ['id', ], 'common_location', mylog)

company_location['id'] = company_location['company_location_id']
company_location['latitude'] = company_location['Latitude']
company_location['longitude'] = company_location['Longitude']
vincere_custom_migration.psycopg2_bulk_update_tracking(company_location, connection, ['latitude', 'longitude'], ['id', ], 'company_location', mylog)
