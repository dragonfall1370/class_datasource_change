
# %% Configuration
# -*- coding: UTF-8 -*-
# import sys
# sys.path.append('D:\Tony\Working\DMvincere')
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import pandas as pd
import sqlalchemy
import dateutil
import common.vincere_standard_migration as vincere_standard_migration
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('pt_config.ini')
# file storage config
data_folder = cf['default'].get('data_folder')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
# db config
review_db = cf['review_db']
sqlite_url = cf['default'].get('sqlite_url')
# log config
log_file = cf['default'].get('log_file')
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% data connections
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_url, encoding='utf8')

# %% candidate
sql = """
select "Personnel ID
[link to folder]" as "candidate-externalId",
    "First Name" as "candidate-firstName",
    "Last Name" as "candidate-lastName",
    "Email" as "candidate-email",
    "Gender
[list]" as "candidate-gender",
    "DOB
[mm/dd/yyyy]" as "candidate-dob"
from CRM_v05___2020_04_06_12PM___Main_List
"""
candidate = pd.read_sql(sql, engine_sqlite)
candidate = vincere_standard_migration.process_vincere_cand(candidate, mylog)

# %% csv extract
# candidate
def validate_date(date_str):
    try:
        dateutil.parser.parse(date_str)
        return True
    except (ValueError, TypeError):
        return False
candidate['candidate-gender'] = candidate['candidate-gender'].apply(lambda x: 'FEMALE' if x == 'F' else ('MALE' if x == 'M' else x)) 
candidate['candidate-dob'] = candidate['candidate-dob'].apply(lambda x: x if validate_date(x) else None)
candidate.to_csv(os.path.join(standard_file_upload, '4_candidate.csv'), index=False)