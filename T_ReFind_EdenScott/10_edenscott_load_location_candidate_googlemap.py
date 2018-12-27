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
import googlemaps
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)
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

if __name__ == '__main__':
    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')

    df = pd.read_sql(""" select id, address, post_code from common_location where id in (select current_location_id from candidate) and address is not null and longitude is null """, ddbconn)
    dfs = vincere_common.df_split_to_listofdfs(df, 1000)

    gmaps = googlemaps.Client(key="AIzaSyB3sfjY5f3S71g_5QOJbqpqaLB6zrcJBnQ")

    # Geocoding an address
    geocode_result = gmaps.geocode('17th Floor City Tower,40 Basinghall Street,London')

    geocode_result[0]['geometry']['location']['lat']
    geocode_result[0]['geometry']['location']['lng']


    # Instantiate a thread pool with 5 worker threads
    pool = thread_pool.ThreadPool(1)
    for index, d in enumerate(dfs):
        refn = os.path.join(locator, 'edenscott_cand_location_googlemap{}.csv'.format(index))
        pool.add_task(vincere_custom_migration.load_location_by_postcost_googlemap, d, 'id', 'address', 'post_code', refn, mylog)
    pool.wait_completion()

    # vincere_custom_migration.load_location_by_postcost(df, 'id', 'address', 'post_code', os.path.join(standard_file_upload, 'edenscott_cand_location.csv', mylog))


