# -*- coding: UTF-8 -*-
import configparser
import os
import pathlib
import urllib.request
import pandas as pd
import psycopg2
import sqlalchemy
import csv

import common.logger_config as log
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('saleexpert.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
db_source = cf[cf['default'].get('src_db')]
review_db = cf[cf['default'].get('review_db')]
mylog = log.get_info_logger(log_file)


# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% extract data
engine_sqlite = sqlalchemy.create_engine('sqlite:///salesexpert.db', encoding='utf8')

connection_str = "mysql+pymysql://{user}:{password}@{server}/{db}" \
    .format(user=db_source.get('user')
            , password=db_source.get('password')
            , server=db_source.get('server')
            , db=db_source.get('database'))
engine = sqlalchemy.create_engine(connection_str)

# %%
# photo = pd.read_sql("""
# select * from user_upload_photo
# """, engine)
# photo.to_csv('photo.csv')
# assert False
#
# for index, row in photo.iterrows():
#      print(row[2])
#      print('Beginning file download with urllib2...')
#      url = 'https://jobs.kloepfel-recruiting.de/i/B?filename='+str(row[2])+'&width=120&height=0'
#      urllib.request.urlretrieve(url, '/Users/truongtung/Desktop/database_csv/SalesExperts_Photos/'+str(row[2]))

with open('photo.csv', "rt", encoding='utf-8') as infile:
 read = csv.reader(infile)
 for i, line in enumerate(read):
     if i >= 1386:
         print(line[3])
         print('Beginning file download with urllib2...')
         url = 'https://jobs.kloepfel-recruiting.de/i/B?filename=' + str(line[3]) + '&width=120&height=0'
         print(url)
         try:
             urllib.request.urlretrieve(url, '/Users/truongtung/Desktop/database_csv/SalesExperts_Photos/' + str(line[3]))
         except:
             continue
         # urllib.request.urlretrieve(url, '/Users/truongtung/Desktop/database_csv/SalesExperts_Photos/' + str(line[3]))



# %% transform data

