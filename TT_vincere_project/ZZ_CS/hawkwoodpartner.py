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
from pandas.io.json import json_normalize
import json

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('dn_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect db
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
cand = pd.read_sql("""select id, experience_details_json from candidate where experience_details_json is not null""",engine_postgre_review)

def mark_current_employer(df):
    if len(df):
        df['cbEmployer'] = None
        df.loc[df.index[0], 'cbEmployer'] = '1'
    return df

assert False
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.replace('null','""'))
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.replace('""""','""'))
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: eval(x))
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: json_normalize(x))
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: mark_current_employer(x))
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.to_json(orient='records')[1:-1].replace('},{', '} {'))
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: '['+x+']')
cand['experience_details_json'] = cand['experience_details_json'].apply(lambda x: x.replace('} {','},{'))
vincere_custom_migration.load_data_to_vincere(cand, dest_db, 'update', 'candidate', ['experience_details_json', ], ['id', ], mylog)



# str = '[{"company":null,"jobTitle":null,"currentEmployer":null,"yearOfExperience":null,"industry":null,"functionalExpertiseId":null,"subFunctionId":null,"cbEmployer":null,"currentEmployerId":null,"dateRangeFrom":null,"dateRangeTo":null}]'
# str = str.replace('null','""')
# str = str.replace('""""','""')
# a = eval(str)
# b = json_normalize(a)
# x = mark_current_employer(b)
# x.to_json(orient='records')[1:-1].replace('},{', '} {')