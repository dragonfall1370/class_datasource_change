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
cf.read('dn_config.ini')
log_file = cf['default'].get('log_file')
# data_folder = cf['default'].get('data_folder')
data_folder = '/Users/truongtung/Desktop'

data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
dest_db = cf[cf['default'].get('dest_db_sin')]
sqlite_path = cf['default'].get('sqlite_path')
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

import requests

# url = 'https://login.chameleoni.com/ajaxpro/Recruiter.eTags,Recruiter.ashx'
# s = requests.session()
# #s.headers['User-Agent'] = 'Mozilla/5.0'

# r = s.post(url, data)
#
# print(r.url)
# print('Log out' in r.text)
# print(r)



import requests
url = "https://login.chameleoni.com/ajaxpro/Recruiter.eTags,Recruiter.ashx"
payload = " {\"parentid\":\"274232\"}"
data = {'username':'JackC01', 'password':'Pentonville25!'}
headers = {
    'content-type': "application/json",
    'x-ajaxpro-method': "GetTags",
    'cache-control': "no-cache",
    'postman-token': "7a6f80a1-032f-9d7a-2dd4-1dd16ecea72a"
    }
response = requests.request("POST", url, data=data, headers=headers)
print(response.text)