# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
import sqlalchemy
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('ct_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
#src_db = cf[cf['default'].get('src_db')]
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
assert False
# %% json
people = pd.read_json(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\people.json', lines=True)
people.to_csv('people.csv',index=False)

company = pd.read_json(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\companies.json', lines=True)
company.to_csv('company.csv',index=False)

jobs = pd.read_json(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\jobs.json', lines=True)
jobs.to_csv('jobs.csv',index=False)

agency = pd.read_json(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\agency.json', lines=True)
agency.to_csv('agency.csv',index=False)

activities = pd.read_json(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\activities.json', lines=True)
activities.to_csv('activities.csv',index=False)
#
# import json
#
# # Opening JSON file
# f = open('D:\\Tony\\project\\vincere_project\\Loxo__captivatetalent\\Database\\people.json',"r")
#
# # returns JSON object as
# # a dictionary
# data = json.loads(f.read())


# import json
#
# data = []
# with open(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\people.json') as f:
#     for line in f:
#         #print(line)
#         #d = json.load(line)
#         #print(d)
#         data.append(json.loads(line))
#
#
# import json
# import pandas as pd
# from pandas.io.json import json_normalize #package for flattening json in pandas df
#
# #load json object
# with open(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\people.json') as f:
#     d = json.load(f)
#     print(json_normalize(d))
# #lets put the data into a pandas df
# #clicking on raw_nyc_phil.json under "Input Files"
# #tells us parent node is 'programs'
# nycphil = json_normalize(d['programs'])
# nycphil.head(3)
#
# people = pd.read_json(r'D:\Tony\project\vincere_project\Loxo__captivatetalent\Database\New folder (2)\people.json', lines=True)
#
#
# df = pd.DataFrame(data)