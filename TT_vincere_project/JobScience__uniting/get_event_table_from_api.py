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
# engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
# conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
# engine_postgre = sqlalchemy.create_engine(conn_str_ddb, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
# connection = engine_postgre.raw_connection()

df = []
def get_data(index):
    url = "https://unitingambition.my.salesforce.com/services/data/v47.0/query/01g3z0000B4USkIAQW-"+str(index*1000)

    headers = {
        'Authorization': "Bearer 00DD0000000nflo!AQkAQIPcFUVenO4jHSxxDvnNbOS6itGy3GuVmz6BBWZrmv5Iyc6bFUwt8DVSwXBud.7X25oVXkT8TFCVlWVecck.NIR.9PsJ",
        'User-Agent': "PostmanRuntime/7.19.0",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Postman-Token': "3c90246c-7943-4b5c-ba52-62013ae4048c,672f9fff-3202-4168-b607-bd3e0dbb57a3",
        'Host': "unitingambition.my.salesforce.com",
        'Accept-Encoding': "gzip, deflate",
        'Cookie': "BrowserId=_ocvsQnjEeqzzWHeO9ltNg; disco=",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
        }

    response = requests.request("GET", url, headers=headers)
    json = response.json()
    df.append(json_normalize(json['records']))


for i in range(18):
    get_data(i)

event = pd.concat(df)
event.to_csv('Event.csv')