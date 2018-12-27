# -*- coding: UTF-8 -*-
import numpy as np
import pandas as pd
import vincere_custom_migration
import logger.logger
import connection_string
import pymssql
import psycopg2

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

mylog = logger.logger.get_logger('hojona.log')

fr_mentalhealth = connection_string.client_hojona_mentalhealth
fr_socialcare = connection_string.client_hojona_socialcare
to_mentalhealth = connection_string.production_hojona_mentalhealth
to_socialworkers = connection_string.production_hojona_socialworkers
sdbconn_men = pymssql.connect(server=fr_mentalhealth.get('server'), user=fr_mentalhealth.get('user'), password=fr_mentalhealth.get('password'), database=fr_mentalhealth.get('database'), as_dict=True)
ddbconn_men = psycopg2.connect(host=to_mentalhealth.get('server'), user=to_mentalhealth.get('user'), password=to_mentalhealth.get('password'), database=to_mentalhealth.get('database'), port=to_mentalhealth.get('port'))
sdbconn_soc = pymssql.connect(server=fr_socialcare.get('server'), user=fr_socialcare.get('user'), password=fr_socialcare.get('password'), database=fr_socialcare.get('database'), as_dict=True)
ddbconn_soc = psycopg2.connect(host=to_socialworkers.get('server'), user=to_socialworkers.get('user'), password=to_socialworkers.get('password'), database=to_socialworkers.get('database'), port=to_socialworkers.get('port'))

df = pd.read_csv('output_standard_files/men_cand_location.csv')
df['location_latitude'] = [float(x) if len(str(x).strip()) else None for x in df['location_latitude']]
df['location_longitude'] = [float(x) if len(str(x).strip()) else None for x in df['location_longitude']]
df.rename(columns={
    'location_longitude': 'longitude', 'location_latitude': 'latitude'
}, inplace=True)
df_updated = df[(df['longitude'].notnull()) & (df['latitude'].notnull())]
vincere_custom_migration.psycopg2_bulk_update(df_updated, ddbconn_men, up_cols=['longitude', 'latitude', ], wh_cols=['id', ], tblname='common_location')

df = pd.read_csv('output_standard_files/soc_cand_location.csv')
df['location_latitude'] = [float(x) if len(str(x).strip()) else None for x in df['location_latitude']]
df['location_longitude'] = [float(x) if len(str(x).strip()) else None for x in df['location_longitude']]
df.rename(columns={
    'location_longitude': 'longitude', 'location_latitude': 'latitude'
}, inplace=True)
df_updated = df[(df['longitude'].notnull()) & (df['latitude'].notnull())]
vincere_custom_migration.psycopg2_bulk_update(df_updated, ddbconn_soc, up_cols=['longitude', 'latitude', ], wh_cols=['id', ], tblname='common_location')
