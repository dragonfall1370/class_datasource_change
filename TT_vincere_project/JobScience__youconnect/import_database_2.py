# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
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
cf.read('yc_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
csv_path = r'D:\Tony\File\Just Legal JobScience'
sqlite_path = cf['default'].get('sqlite_path')

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% clean data
engine_sqlite = sqlalchemy.create_engine('sqlite:///justlegal.db', encoding='utf8')


def split_dataframe_to_chunks(df, n):
   df_len = len(df)
   count = 0
   dfs = []
   while True:
      if count > df_len - 1:
         break

      start = count
      count += n
      # print("%s : %s" % (start, count))
      dfs.append(df.iloc[start: count])
   return dfs

# %% read all csv files to first call db
temp_msg_metadata = vincere_common.get_folder_structure(csv_path)
for index, row in temp_msg_metadata.iterrows():
   print(row['file_fullpath'])
   tem = pd.read_csv(row['file_fullpath'], encoding='cp1252', dtype='unicode')
   tbl_name = row['alter_file1'].replace('.csv', '')
   split_df_to_chunks = split_dataframe_to_chunks(tem, 10000)
   print(len(split_df_to_chunks))
   for idx, val in enumerate(split_df_to_chunks):
      print(idx + 1)
      val.to_sql(name=tbl_name, con=engine_sqlite, if_exists='append', index=False)