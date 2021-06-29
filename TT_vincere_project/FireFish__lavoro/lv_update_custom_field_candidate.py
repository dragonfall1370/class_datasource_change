# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
import datetime
from functools import reduce
from common import vincere_job_application
import pandas as pd

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('lv_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
sqlite_path = cf['default'].get('sqlite_path')
dest_db = cf[cf['default'].get('dest_db')]
mylog = log.get_info_logger(log_file)

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)
csv_path = r""

# %% data connection
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
conn_str_ddb_review = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
engine_postgre_review = sqlalchemy.create_engine(conn_str_ddb_review, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)
connection = engine_postgre_review.raw_connection()
assert False
# %% candidate
cand1 = pd.read_sql( """
select c.ID as candidate_externalid, LocationAreaWSIID, LocationWSIID, Location, Location_Area
from Candidate c
left join (select j1.id, j2.Value as Location, j1.Value as Location_Area from Drop_Down___Locations j1
left join Drop_Down___Locations j2 on j1.ParentID = j2.ID) l on c.LocationWSIID = l.ID
where LocationWSIID is not null
""", engine_sqlite)
cand1['candidate_externalid'] = cand1['candidate_externalid'].apply(lambda x: str(x) if x else x)
tem1 = cand1[['candidate_externalid','Location']]
tem2 = cand1[['candidate_externalid','Location_Area']]


cand2 = pd.read_sql( """
select c.ID as candidate_externalid, LocationAreaWSIID, LocationWSIID, l.Value as Location
from Candidate c
left join Drop_Down___Locations l on c.LocationAreaWSIID = l.ID
where LocationAreaWSIID is not null and LocationWSIID is null
""", engine_sqlite)
cand2['candidate_externalid'] = cand2['candidate_externalid'].apply(lambda x: str(x) if x else x)
tem3 = cand2[['candidate_externalid','Location']]
location = pd.concat([tem1,tem3])

parent_location = pd.read_sql( """
select Value from Drop_Down___Locations where ParentID is null
""", engine_sqlite)
parent_location = parent_location.drop_duplicates()

child_location = pd.read_sql( """
select Value from Drop_Down___Locations where ParentID is not null
""", engine_sqlite)
child_location = child_location.drop_duplicates()
child_location = child_location.loc[child_location['Value']!='New Item']

check1 = location.merge(parent_location, left_on='Location', right_on='Value', suffixes=['', '_y'], how='outer', indicator=True)
check1.loc[check1['_merge']=='right_only']
check1['_merge'].unique()

check2 = tem2.merge(child_location, left_on='Location_Area', right_on='Value', suffixes=['', '_y'], how='outer', indicator=True)
check2.loc[check2['_merge']=='right_only']
check2['_merge'].unique()

data1 = [['Australia / Oceania', 'Australia / Oceania']
    , ['Africa', 'Africa']
    , ['Antarctica', 'Antarctica']
    , ['South America', 'South America']
    , ['Isle of Man', 'Isle of Man']]
df1 = pd.DataFrame(data1, columns=['candidate_externalid', 'Location'])
f_location = pd.concat([location,df1])


data2 = [['Perth and Kinross',	'Perth and Kinross'],
    ['Falkirk',	'Falkirk',],
        [ 'Outer Hebrides',	         'Outer Hebrides',],
        ['Argyll and Bute',	        'Argyll and Bute',],
                  ['Moray',	                  'Moray',],
    ['West Dunbartonshire',	    'West Dunbartonshire',],
          ['East Ayrshire',	          'East Ayrshire',],
             ['Inverclyde',	             'Inverclyde',],
                  ['Angus',	                  'Angus',],
           ['North London',	           'North London',],
          ['Aberdeenshire',	          'Aberdeenshire',],
            ['Dundee City',	            'Dundee City',],
      ['East Renfrewshire',	      'East Renfrewshire',],
            ['West London',	            'West London',],
      ['City of Edinburgh',	      'City of Edinburgh',],
         ['South Ayrshire',	         'South Ayrshire',],
         ['Orkney Islands',	         'Orkney Islands',],
      ['South Lanarkshire',	      'South Lanarkshire',],
       ['Shetland Islands',	       'Shetland Islands',],
       ['Scottish Borders',	       'Scottish Borders',],
             [  'Highland',	               'Highland',],
             ['Midlothian',	             'Midlothian',],
    ['East Dunbartonshire',	    'East Dunbartonshire',],
            ['East London',	            'East London',],
  ['Dumfries and Galloway',	  'Dumfries and Galloway',],
           ['South London',	           'South London',],
         ['North Ayrshire',	         'North Ayrshire',]]
df2 = pd.DataFrame(data2, columns=['candidate_externalid', 'Location_Area'])
s_location = pd.concat([tem2,df2])

api1 = 'faf49ddb28ebe2c3a8352261cc147386'
vincere_custom_migration.insert_candidate_drop_down_list_values(f_location, 'candidate_externalid', 'Location', api1, connection)
api2 = 'b450cfd2f045ef92e7381d0b3138d0f4'
vincere_custom_migration.insert_candidate_drop_down_list_values(s_location, 'candidate_externalid', 'Location_Area', api2, connection)


