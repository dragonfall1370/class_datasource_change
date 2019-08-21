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

def find_country_code(findc, vincere_countries_df):
    findc = findc.strip().lower().replace('(', '').replace(')', '').replace('/', '').replace('\\', '').replace('[', '').replace(']', '').replace('?', '')
    try:
        for _idx, _row in vincere_countries_df.iterrows():
            m1 = re.match(r'.*%s.*' % findc, r'%s' % _row['country_name_lc'])
            m2 = re.match(r'.*%s.*' % _row['country_name_lc'], r'%s' % findc)

            if m1 or m2:
                return _row.code
        return None
    except Exception as ex:
        print(findc)
        print(_row['country_name_lc'])



if __name__ == '__main__':
    sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)
    ddbconn = psycopg2.connect(host=to.get('server'), user=to.get('user'), password=to.get('password'), database=to.get('database'), port=to.get('port'))
    ddbconn.set_client_encoding('UTF8')

    cd = pd.read_csv(r'C:\Users\tungn\PycharmProjects\vincere\common\Country_code_list.txt', sep=';')
    cd['country_name_lc'] = cd['system_name'].map(lambda x: str(x).lower())

    cand_na = pd.read_sql("select CAnum as external_id, canat from Cand where canat is not null", sdbconn)
    cand_na['canat'] = cand_na['canat'].map(lambda x: x.strip().lower())
    cand_na = cand_na[cand_na['canat'] != '']
    # cand_na['nationality'] = None

    # try:
    #     for idx, row in cand_na.iterrows():
    #         for _idx, _row in cd.iterrows():
    #             findc = str(row['canat']).strip().lower().replace('(', '').replace('/','').replace('\\','')
    #             findc = re.sub('S', findc)
    #             # print(findc)
    #             m1 = re.match(r'.*%s.*' % findc, r'%s' % _row['country_name_lc'])
    #             m2 = re.match(r'.*%s.*' % _row['country_name_lc'], r'%s' % findc)
    #
    #             if m1 or m2:
    #                 cand_na.loc[idx, 'country_code'] = _row.code
    #                 break
    # except Exception as ex:
    #     print(str(row['canat']).lower())
    #     print(_row['country_name_lc'])

    cand_na['nationality'] = cand_na['canat'].map(lambda x: find_country_code(x, cd))
    cand_na['external_id'] = cand_na['external_id'].astype(np.int64)
    vcand = pd.read_sql("select external_id, id from candidate", ddbconn)
    vcand['external_id'] = vcand['external_id'].astype(np.int64)
    cand_na = cand_na.merge(vcand, on='external_id')
    vincere_custom_migration.psycopg2_bulk_update(cand_na, ddbconn, ['nationality',], ['id',], 'candidate')

    # import pycountry
    # for country in pycountry.countries:
    #     # print (country.name)
    #     if 'british' in country.name.lower():
    #         print(country.name)