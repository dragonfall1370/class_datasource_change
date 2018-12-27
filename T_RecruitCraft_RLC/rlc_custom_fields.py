# -*- coding: UTF-8 -*-

import psycopg2
import vincere.vincere_custom_migration as custom_migration
import common.connection_string as cs
import logger.logger as log
import pymssql
import pandas as pd
# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

fr = cs.client_rlc_prd
sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)


fr = cs.production_rlc_p35432
ddbconn = psycopg2.connect(host=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), port=fr.get('port'))
ddbconn.set_client_encoding('UTF8')

# add values for this field
stages = pd.read_sql("""
    select 
    concat('RLC', company_id) as company_external_id
    , ltrim(rtrim(company_stage)) as translate
    from tblCompany where company_stage is not null and company_stage<>'' order by ltrim(rtrim(company_stage));
    """, sdbconn)

sources = pd.read_sql("""
    select 
        concat('RLC', company_id) as company_external_id
        , ltrim(rtrim(company_source)) as translate
    from tblCompany where company_source is not null and company_source<>'' order by ltrim(rtrim(company_source));
    """, sdbconn)

field_key = '2ad1d498e06b17ec73666f148c79598b'
custom_migration.insert_company_drop_down_list_values(stages, 'company_external_id', 'translate', field_key, ddbconn)

field_key = '04c68fb86f04d05fdf7db520ee25a71e'
custom_migration.insert_company_drop_down_list_values(sources, 'company_external_id', 'translate', field_key, ddbconn)


