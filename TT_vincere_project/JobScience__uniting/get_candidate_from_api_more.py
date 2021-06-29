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
import requests
from pandas.io.json import json_normalize

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)


# %% connect db
url = "https://unitingambition.my.salesforce.com/services/data/v47.0/sobjects/Contact/003D000002Ej9JPIAZ"

headers = {
    'Authorization': "Bearer 00DD0000000nflo!AQkAQNQDG7gswcV3LyV7ZTpleMxnYtJ0e6EoqXE0r3PtB3x387Z6fc9X9oX2oGic5mhePYBoqszQk9NFmSYnsSFf_4TW2Swm",
    'User-Agent': "PostmanRuntime/7.19.0",
    'Accept': "*/*",
    'Cache-Control': "no-cache",
    'Postman-Token': "5d42ba28-0415-488f-bebf-afea2e5cebd0,483851b9-7907-4b54-a04b-29d5c4794273",
    'Host': "unitingambition.my.salesforce.com",
    'Accept-Encoding': "gzip, deflate",
    'Cookie': "BrowserId=_ocvsQnjEeqzzWHeO9ltNg; disco=",
    'Connection': "keep-alive",
    'cache-control': "no-cache"
}

response = requests.request("GET", url, headers=headers)
# print(response.text)
json = response.json()
json.to_csv('Cand_new_more.csv')
