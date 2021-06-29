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
placement = pd.read_csv('placement_to_add.csv')

df = []
def get_data(name):
    url = "https://unitingambition.my.salesforce.com/services/data/v47.0/query/?q=SELECT+Id,ts2__Job__c,ts2__Employee__c,ts2__Start_Date__c,ts2__End_Date__c,Name,ts2__Related_Application__c,Eden_Candidate_Id__c,Contractor_Rate__c,Contractor_Margin__c,Length_of_Contract_Months__c,Payment_Terms__c,ts2__Status__c,Proof_of_ID__c,ts2__Salary__c,Fee_Percentage__c,ts2__Filled_Pct__c,ts2__Filled_Pct_2__c,CreatedDate,ts2__Filled_By__c,ts2__Filled_By_2__c+from+ts2__Placement__c+where+Name=%27"+name+"%27"

    headers = {
        'Authorization': "Bearer 00DD0000000nflo!AQkAQNQDG7gswcV3LyV7ZTpleMxnYtJ0e6EoqXE0r3PtB3x387Z6fc9X9oX2oGic5mhePYBoqszQk9NFmSYnsSFf_4TW2Swm",
        'User-Agent': "PostmanRuntime/7.19.0",
        'Accept': "*/*",
        'Cache-Control': "no-cache",
        'Postman-Token': "238fe158-d265-4a9c-bfcf-c015174c3773,8736187a-3a55-46d3-89ec-851a58dabfe0",
        'Host': "unitingambition.my.salesforce.com",
        'Accept-Encoding': "gzip, deflate",
        'Cookie': "BrowserId=_ocvsQnjEeqzzWHeO9ltNg; disco=",
        'Connection': "keep-alive",
        'cache-control': "no-cache"
    }
    response = requests.request("GET", url, headers=headers)
    json = response.json()
    df.append(json_normalize(json['records']))


for index, row in placement.iterrows():
    print(row['Placement'])
    print(index)
    get_data(row['Placement'])

placement = pd.concat(df)
placement.info()
placement.to_csv('Placement_new.csv')
