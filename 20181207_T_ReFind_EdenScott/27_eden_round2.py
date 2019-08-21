# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import pymssql
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
from common import thread_pool as thread_pool
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

def main():
    #
    # loading configuration
    cf = configparser.RawConfigParser()
    cf.read('_edenscott_config.ini')
    log_file = cf['default'].get('log_file')
    data_folder = cf['default'].get('data_folder')
    data_input = os.path.join(data_folder, 'data_input')
    standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
    locator = os.path.join(data_folder, 'locator')
    to = cf[cf['default'].get('dest_db')]
    mylog = log.get_info_logger(log_file)
    #
    # create the data folder if not exist
    pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
    pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
    pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
    pathlib.Path(locator).mkdir(parents=True, exist_ok=True)

    df = pd.read_csv(r'd:\vincere\data_output\edenscott\data_file\round2\20181220 - Eden Scott - Candidates - Geotagging - Round 2.csv', low_memory=False)
    dfs = vincere_common.df_split_to_listofdfs(df, 25000)
    for _i, _d in enumerate(dfs):
        _d.to_csv(r'd:\vincere\data_output\edenscott\data_file\round2\20181220 - Eden Scott - Candidates - Geotagging - Round 2 - split{0}.csv'.format(_i), index=False)
