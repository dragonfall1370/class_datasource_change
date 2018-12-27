# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import re
from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import pandas as pd
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
fr = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

def edenscott_country_name(x):
    country_name = re.search(r"\,\s(\w*)\s*$", x)
    country_name = '' if country_name is None else country_name.group(1)
    return vincere_common.map_country_name(country_name)
def edenscott_country_code(x):
    country_name = re.search(r"\,\s(\w*)\s*$", x)
    country_name = '' if country_name is None else country_name.group(1)
    return vincere_common.get_country_code(country_name)
def edenscott_city(x):
    city = re.search(r"(\w*)\sCity,", x) # insensitive search
    city = '' if city is None else city.group(1)
    if city is None:
        city = re.search(r"City\sof\s(\w*)", x) # insensitive search
        city = '' if city is None else city.group(1)
    return city

if __name__ == '__main__':
    ddbconn = psycopg2.connect(host=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), port=fr.get('port'))
    ddbconn.set_client_encoding('UTF8')

    df_source = pd.read_csv(os.path.join(standard_file_upload, 'company_geolocator.csv'))
    df_source['location_latitude'] = pd.to_numeric(df_source['location_latitude'], errors='coerce') # Converting a dataframe column to float
    df_source['location_longitude'] = pd.to_numeric(df_source['location_longitude'], errors='coerce') # Converting a dataframe column to float
    df_source['country'] = df_source['location_address'].apply(lambda x: edenscott_country_name(x))
    df_source['country_code'] = df_source['location_address'].apply(lambda x: edenscott_country_code(x))
    df_source['city'] = df_source['location_address'].apply(lambda x: edenscott_city(x))

    sql_get_vincere_company = "select a.id, a.external_id, cl.address, cl.post_code from company a left join company_location cl on a.id = cl.company_id where a.external_id is not null"
    df_vincere_company = pd.read_sql(sql_get_vincere_company, ddbconn)
    df_vincere_company['external_id'] = df_vincere_company['external_id'].astype(np.int64)

    df_source = df_source.merge(df_vincere_company, left_on=['COnum', 'COpc'], right_on=['external_id', 'post_code'])

if True:
    #-----------
    list_values = []
    for index, row in df_source.iterrows():
        a_record = list()
        a_record.append(row['id'])
        a_record.append(row['location_address'])
        a_record.append(row['location_latitude'])
        a_record.append(row['location_longitude'])
        a_record.append(row['country'])
        a_record.append(row['country_code'])
        a_record.append(row['city'])
        list_values.append(tuple(a_record))

    sql = '''
    update company_location set latitude=data.v2, longitude=data.v3, country=data.v4, country_code=data.v5, city=data.v6
    from (values %s) as data(id, v1, v2, v3, v4, v5, v6)
    where company_location.company_id=data.id  
    '''
    cur = ddbconn.cursor()
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    ddbconn.commit()
    cur.execute("update company_location set location_name=address where (location_name is null or trim(location_name)='')and address is not null")
    ddbconn.commit()
    cur.close()



