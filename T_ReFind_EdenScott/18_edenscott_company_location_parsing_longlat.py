# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import pymssql
import datetime
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
from common import thread_pool as thread_pool
from common import geolocator_selenium
import pandas as pd
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
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
fr = cf[cf['default'].get('src_db')]
to = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
pathlib.Path(locator).mkdir(parents=True, exist_ok=True)

def conver_addr_to_latlong(df, find_by_colname):
    geolocator = Nominatim(user_agent="my-application", timeout=None)
    for ind, row in df.iterrows():
        if row[find_by_colname] != None:
            location = geolocator.geocode(row[find_by_colname])
            if location:
                df.loc[ind, 'latitude'] = location.latitude
                df.loc[ind, 'longitude'] = location.longitude




if __name__ == '__main__':
    logger = log.get_info_logger("edenscott.log")
    if False:
        sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
        ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
        ddbconn.set_client_encoding('UTF8')

        company_location = pd.read_sql("""
        select a.id, a.external_id, cl.address, cl.post_code, latitude, longitude
        from company a
          left join company_location cl on a.id = cl.company_id
        where a.external_id is not null
        and latitude is null
        """, ddbconn)
        company_location.to_csv('company_location.csv', index=False)
    if True:
        company_location = pd.read_csv('company_location.csv')
        company_location = company_location[company_location['latitude'].isnull()]
        conver_addr_to_latlong(company_location, 'address')
        company_location.to_csv('company_location_parsed.csv', index=False)


