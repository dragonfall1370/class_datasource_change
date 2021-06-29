# -*- coding: UTF-8 -*-
import multiprocessing
import threading
import pymssql
import psycopg2
import psycopg2.extras
# import sqlalchemy
import time
import sqlalchemy
from common.country_code import *
import datetime
import uuid
from dateutil.relativedelta import relativedelta
import logging
import numpy as np
import re
import os
from os import path
import math
import shutil  # zip folder
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim
import re
import pandas as pd
import boto
import boto.s3.connection
import numpy as np
import sqlalchemy
from geopy.exc import GeocoderTimedOut
from geopy.geocoders import Nominatim

from common.country_code import *

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 2000)
pd.set_option('display.width', 1000)

# # create mylog
# logging.basicConfig(filename='edenscott.log', format='%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p', level=logging.DEBUG)
# mylog = logging.getLogger(__name__)
# # create console handler and set level to debug
# ch = logging.StreamHandler()
# ch.setLevel(logging.DEBUG)
# # create formatter
# formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s', datefmt='%Y/%m/%d %I:%M:%S %p')
# # add formatter to ch
# ch.setFormatter(formatter)
# # add ch to mylog
# mylog.addHandler(ch)
# # 'application' code
# """
# mylog.debug('debug message')
# mylog.info('info message')
# mylog.warn('warn message')
# mylog.error('error message')
# mylog.critical('critical message')
# """
# mylog.info('==================================================== PROCESS RUNNING ====================================================')
my_insert_timestamp ='2019-02-14 23:14:04.618870'
country_names = {
    'Vatican City State': ('vatican city state',),
    'French Guiana': ('french guiana',),
    'Congo': ('congo',),
    'Guernsey': ('guernsey',),
    'Svalbard and Jan Mayen': ('svalbard and jan mayen',),
    'United States Minor Outlying Islands': ('united states minor outlying islands',),
    'Virgin Islands, U.S.': ('virgin islands, u.s.',),
    'Guadeloupe': ('guadeloupe',),
    'Serbia': ('serbia',),
    'Spain': ('spain',),
    'Åland Islands': ('åland islands',),
    'Bouvet Island': ('bouvet island',),
    'French Southern Territories': ('french southern territories',),
    'Heard Island and McDonald Islands': ('heard island and mcdonald islands',),
    'Jersey': ('jersey',),
    'Palestinian Territory, Occupied': ('palestinian territory, occupied',),
    'Réunion': ('réunion',),
    'Martinique': ('martinique',),
    'Aruba': ('aruba',),
    'Jordan': ('jordan',),
    'Andorra': ('andorra',),
    'United Arab Emirates': ('united arab emirates',),
    'Afghanistan': ('afghanistan',),
    'Antigua and Barbuda': ('antigua and barbuda',),
    'Anguilla': ('anguilla',),
    'Albania': ('albania',),
    'Armenia': ('armenia',),
    'Angola': ('angola',),
    'Antarctica': ('antarctica',),
    'Argentina': ('argentina',),
    'American Samoa': ('american samoa',),
    'Austria': ('austria',),
    'Australia': ('australia',),
    'Azerbaijan': ('azerbaijan',),
    'Bosnia and Herzegovina': ('bosnia and herzegovina',),
    'Barbados': ('barbados',),
    'Bangladesh': ('bangladesh',),
    'Belgium': ('belgium',),
    'Burkina Faso': ('burkina faso',),
    'Bulgaria': ('bulgaria',),
    'Bahrain': ('bahrain',),
    'Burundi': ('burundi',),
    'Benin': ('benin',),
    'Saint Barthélemy': ('saint barthélemy',),
    'Bermuda': ('bermuda',),
    'Brunei Darussalam': ('brunei darussalam',),
    'Bolivia': ('bolivia',),
    'Brazil': ('brazil',),
    'Bahamas': ('bahamas',),
    'Bhutan': ('bhutan',),
    'Botswana': ('botswana',),
    'Belarus': ('belarus',),
    'Belize': ('belize',),
    'Canada': ('canada',),
    'Cocos (Keeling) Islands': ('cocos (keeling) islands',),
    'Central African Republic': ('central african republic',),
    'Switzerland': ('switzerland',),
    "Côte d'Ivoire": ("côte d'ivoire",),
    'Cook Islands': ('cook islands',),
    'Chile': ('chile',),
    'Cameroon': ('cameroon',),
    'China': ('china', 'pr of china'),
    'Colombia': ('colombia',),
    'Costa Rica': ('costa rica',),
    'Cuba': ('cuba',),
    'Cape Verde': ('cape verde',),
    'Christmas Island': ('christmas island',),
    'Cyprus': ('cyprus',),
    'Czech Republic': ('czech republic',),
    'Germany': ('germany',),
    'Djibouti': ('djibouti',),
    'Denmark': ('denmark',),
    'Dominica': ('dominica',),
    'Dominican Republic': ('dominican republic',),
    'Algeria': ('algeria',),
    'Ecuador': ('ecuador',),
    'Estonia': ('estonia',),
    'Egypt': ('egypt',),
    'Western Sahara': ('western sahara',),
    'Eritrea': ('eritrea',),
    'Ethiopia': ('ethiopia',),
    'Finland': ('finland',),
    'Fiji': ('fiji',),
    'Falkland Islands (Malvinas)': ('falkland islands (malvinas)',),
    'Micronesia': ('micronesia',),
    'Faroe Islands': ('faroe islands',),
    'France': ('france',),
    'Gabon': ('gabon',),
    'United Kingdom': ('united kingdom', 'london', 'derby', 'uk'),
    'Grenada': ('grenada',),
    'Georgia': ('georgia',),
    'Ghana': ('ghana',),
    'Gibraltar': ('gibraltar',),
    'Greenland': ('greenland',),
    'Gambia': ('gambia',),
    'Guinea': ('guinea',),
    'Equatorial Guinea': ('equatorial guinea',),
    'Greece': ('greece',),
    'Guatemala': ('guatemala',),
    'Guam': ('guam',),
    'Guinea-Bissau': ('guinea-bissau',),
    'Guyana': ('guyana',),
    'Hong Kong': ('hong kong', 'hongkong', 'wanchai, hong kong'),
    'Honduras': ('honduras',),
    'Croatia': ('croatia',),
    'Haiti': ('haiti',),
    'Hungary': ('hungary',),
    'Indonesia': ('indonesia',),
    'Ireland': ('ireland',),
    'Israel': ('israel',),
    'Isle of Man': ('isle of man',),
    'India': ('india', 'indian nationality'),
    'Iraq': ('iraq',),
    'Iran': ('iran',),
    'Iceland': ('iceland',),
    'Italy': ('italy',),
    'Jamaica': ('jamaica',),
    'Japan': ('japan',),
    'Kenya': ('kenya',),
    'Kyrgyzstan': ('kyrgyzstan',),
    'Cambodia': ('cambodia',),
    'Kiribati': ('kiribati',),
    'Comoros': ('comoros',),
    'Saint Kitts and Nevis': ('saint kitts and nevis',),
    'North Korea': ('north korea',),
    'South Korea': ('south korea', 'korea'),
    'Kuwait': ('kuwait',),
    'Cayman Islands': ('cayman islands',),
    'Kazakhstan': ('kazakhstan',),
    'Lao': ('lao',),
    'Lebanon': ('lebanon',),
    'Saint Lucia': ('saint lucia',),
    'Liechtenstein': ('liechtenstein',),
    'Sri Lanka': ('sri lanka',),
    'Liberia': ('liberia',),
    'Lesotho': ('lesotho',),
    'Lithuania': ('lithuania',),
    'Luxembourg': ('luxembourg',),
    'Latvia': ('latvia',),
    'Libyan Arab Jamahiriya': ('libyan arab jamahiriya',),
    'Morocco': ('morocco',),
    'Monaco': ('monaco',),
    'Moldova': ('moldova',),
    'Montenegro': ('montenegro',),
    'Saint Martin (French part)': ('saint martin (french part)',),
    'Madagascar': ('madagascar',),
    'Marshall Islands': ('marshall islands',),
    'Macedonia': ('macedonia',),
    'Mali': ('mali',),
    'Myanmar': ('myanmar',),
    'Mongolia': ('mongolia',),
    'Macao': ('macao',),
    'Northern Mariana Islands': ('northern mariana islands',),
    'Mauritania': ('mauritania',),
    'Montserrat': ('montserrat',),
    'Malta': ('malta',),
    'Mauritius': ('mauritius',),
    'Maldives': ('maldives',),
    'Malawi': ('malawi',),
    'Mexico': ('mexico',),
    'Malaysia': ('malaysia',),
    'Mozambique': ('mozambique',),
    'Namibia': ('namibia',),
    'New Caledonia': ('new caledonia',),
    'Niger': ('niger',),
    'Norfolk Island': ('norfolk island',),
    'Nigeria': ('nigeria',),
    'Nicaragua': ('nicaragua',),
    'Netherlands': ('netherlands', 'holland'),
    'Norway': ('norway',),
    'Nepal': ('nepal',),
    'Nauru': ('nauru',),
    'Niue': ('niue',),
    'New Zealand': ('new zealand',),
    'Oman': ('oman',),
    'Panama': ('panama',),
    'Peru': ('peru',),
    'French Polynesia': ('french polynesia',),
    'Papua New Guinea': ('papua new guinea',),
    'Philippines': ('philippines',),
    'Pakistan': ('pakistan',),
    'Poland': ('poland',),
    'Saint Pierre and Miquelon': ('saint pierre and miquelon',),
    'Pitcairn': ('pitcairn',),
    'Puerto Rico': ('puerto rico',),
    'Portugal': ('portugal',),
    'Palau': ('palau',),
    'Paraguay': ('paraguay',),
    'Qatar': ('qatar',),
    'Romania': ('romania',),
    'Russian Federation': ('russian federation',),
    'Rwanda': ('rwanda',),
    'Saudi Arabia': ('saudi arabia',),
    'Solomon Islands': ('solomon islands',),
    'Seychelles': ('seychelles',),
    'Sudan': ('sudan',),
    'Sweden': ('sweden',),
    'Singapore': ('singapore', 'singapore 417942', 'thailand & singapore'),
    'Saint Helena': ('saint helena',),
    'Slovenia': ('slovenia',),
    'Slovakia': ('slovakia',),
    'Sierra Leone': ('sierra leone',),
    'San Marino': ('san marino',),
    'Senegal': ('senegal',),
    'Somalia': ('somalia',),
    'Suriname': ('suriname',),
    'Sao Tome and Principe': ('sao tome and principe',),
    'El Salvador': ('el salvador',),
    'Syrian Arab Republic': ('syrian arab republic',),
    'Swaziland': ('swaziland',),
    'Turks and Caicos Islands': ('turks and caicos islands',),
    'Chad': ('chad',),
    'Togo': ('togo',),
    'Thailand': ('rayong,thailand', 'bangkok', 'thailand', 'thailand.', 'thaniland', ', thailand', 'thailland', 'th'),
    'Tajikistan': ('tajikistan',),
    'Tokelau': ('tokelau',),
    'Timor-Leste': ('timor-leste',),
    'Turkmenistan': ('turkmenistan',),
    'Tunisia': ('tunisia',),
    'Tonga': ('tonga',),
    'Turkey': ('turkey',),
    'Trinidad and Tobago': ('trinidad and tobago',),
    'Tuvalu': ('tuvalu',),
    'Taiwan': ('taiwan',),
    'Tanzania': ('tanzania',),
    'Ukraine': ('ukraine',),
    'Uganda': ('uganda',),
    'United States': ('united states', 'usa'),
    'Uruguay': ('uruguay',),
    'Uzbekistan': ('uzbekistan',),
    'Saint Vincent and the Grenadines': ('saint vincent and the grenadines',),
    'Venezuela': ('venezuela',),
    'Virgin Islands, British': ('virgin islands, british',),
    'Viet Nam': ('viet nam',),
    'Vanuatu': ('vanuatu',),
    'Wallis and Futuna': ('wallis and futuna',),
    'Samoa': ('samoa',),
    'Yemen': ('yemen',),
    'Mayotte': ('mayotte',),
    'South Africa': ('south africa',),
    'Zambia': ('zambia',),
    'Zimbabwe': ('zimbabwe',),
}


currency_codes = {
    'aed': ('aed', 'uae dirham (aed)',),
    'ars': ('ars', 'argentinian peso (ars$)',),
    'aud': ('aud', 'australian dollar (aud$)',),
    'brl': ('brl', 'brazilian real (brl$)',),
    'cad': ('cad', 'canadian dollar (cad$)',),
    'chf': ('chf', 'swiss franc (chf)',),
    'euro': ('eur', 'euro (eur€)',),
    'hkd': ('hkd', 'hk dollar (hkd$)',),
    'idr': ('idr', 'indonesian rupiah (idrrp)',),
    'mxn': ('mxn', 'mexican peso (mxn$)',),
    'myr': ('myr', 'malaysian ringgit (myrrm)',),
    'nzd': ('nzd', ' new zealand dollar (nzd$)',),
    'php': ('php', 'philippine peso (php₱)',),
    # 'gbp': ('gbp', 'gbp pound sterling (gbp£)', '£', 'pound'),
    'pound': ('gbp', 'gbp pound sterling (gbp£)', '£', 'pound', 'sterling'),
    'pn': ('pn', 'polish zloty (plnzł)',),
    'rub': ('rub', 'russian ruble (rubруб)',),
    'pkr': ('pkr', 'pakistani rupee (pkr$)',),
    'inr': ('inr', 'indian rupee (inr₨)',),
    'sar': ('sar', 'saudi riyal (sar$)',),
    'sgd': ('sgd', 'sing dollar (sgd$)',),
    'thb': ('thb', 'thai baht (thb฿)',),
    'twd': ('twd', 'taiwan dollar (twdnt$)',),
    'usd': ('usd', 'us dollar (usd$)',),
    'vnd': ('vnd', 'vietnam dong (vnd₫)',),
    'krw': ('krw', 'korean won (krw₩)',),
    'jpy': ('jpy', 'japanese yen (jpy¥)',),
    'cny': ('cny', 'chinese yuan (cny¥)',),
    'zar': ('zar', 'south african rand (zarr)',),
}

regex_pattern_email = r"\w\S*@[a-zA-Z0-9.-]*"
regex_pattern_email = r"\w\S*@[a-zA-Z0-9.-]*\w{1,}"


def get_country_code(country_name):
    """https://www.nationsonline.org/oneworld/country_code_list.htm"""
    return_code = ''
    for k, v in country_codes.items():
        # print("code {0}, name {1}".format(k, v))
        if str(country_name).lower().strip() in v:  # check an item exits in the tuple v
            return_code = k
            break
    return return_code


def map_country_name(country_name):
    """https://www.nationsonline.org/oneworld/country_code_list.htm"""
    return_name = ''
    for k, v in country_names.items():
        # print("code {0}, name {1}".format(k, v))
        if country_name.lower().strip() in v:  # check an item exits in the tuple v
            return_name = k
            break
    return return_name




def write_file(data, filename):
    # The wb indicates that the file is opened for writing in binary mode
    with open(filename, 'wb') as f:
        f.write(data)


def get_geolocator1(df, result_filename, colname, alternative_colname=None, logger=None):
    geolocator = Nominatim(user_agent="my-application", timeout=None)
    for index, row in df.iterrows():
        finding_for = row[colname]
        finding_for = '' if finding_for is None else finding_for
        finding_for = '' if isinstance(finding_for, float) and math.isnan(finding_for) else finding_for  # check for nan
        try:
            finding_for = finding_for.replace('\n', ' ')
            finding_for = finding_for.replace('\r', ' ')
            finding_for = re.sub(r"\s{2,}", ' ', finding_for)
            finding_for = finding_for.strip()
            if row['location_address'] is None or row['location_address'] == ' ':
                if logger:
                    logger.info(finding_for)
                location = geolocator.geocode(finding_for)
                if location is None and alternative_colname is not None:  # the first time found nothing, find by the alternative col
                    finding_for = row[alternative_colname].replace('\n', ' ')
                    finding_for = finding_for.replace('\r', ' ')
                    finding_for = re.sub(r"\s{2,}", ' ', finding_for)
                    if logger:
                        logger.info(finding_for)
                    location = geolocator.geocode(finding_for)
                if location is not None:
                    #                     df_comp_files.set_value(index, 'location_address', location.address)
                    #                     df_comp_files.set_value(index, 'location_latitude', location.latitude)
                    #                     df_comp_files.set_value(index, 'location_longitude', location.longitude)
                    df.loc[index, 'location_address'] = location.address
                    df.loc[index, 'location_latitude'] = location.latitude
                    df.loc[index, 'location_longitude'] = location.longitude
                time.sleep(1)
        except AttributeError as e:
            if logger:
                logger.error('finding for: %s' % finding_for)
                logger.error(e)
        except GeocoderTimedOut:
            time.sleep(60)
        except:
            time.sleep(60)
        finally:
            if (index % 100) == 0:
                df.to_csv(result_filename, index=False, header=True, sep=",")
                if logger:
                    logger.info('write data to csv file: data_file/company_geolocator.csv')
    return df


"""
Write "to_sql" in chunks so that the database doesn't timeout
https://github.com/pandas-dev/pandas/issues/7347

engine = sqlalchemy.create_engine('mysql://...') #database details omited
write_to_db(engine, frame, 'retail_pendingcustomers', 20000) 
"""


def write_to_db(engine, frame, table_name, chunk_size, dtype, log):
    """
    if the table_name existed, it will be replaced
    :param engine:
    :param frame:
    :param table_name:
    :param chunk_size:
    :param dtype:
    :param log:
    :return:
    """
    start_index = 0
    end_index = chunk_size if chunk_size < len(frame) else len(frame)
    frame = frame.where(pd.notnull(frame), None)  # replace all NaN by None
    if_exists_param = 'replace'
    while start_index != end_index:
        log.info("Writing to %s rows %s through %s" % (table_name, start_index, end_index))
        frame.iloc[start_index:end_index, :].to_sql(con=engine, name=table_name, if_exists=if_exists_param, chunksize=chunk_size, dtype=dtype, index=False)
        if_exists_param = 'append'
        start_index = min(start_index + chunk_size, len(frame))
        end_index = min(end_index + chunk_size, len(frame))


def append_to_db(engine, frame, table_name, chunk_size, dtype, log):
    start_index = 0
    end_index = chunk_size if chunk_size < len(frame) else len(frame)
    frame = frame.where(pd.notnull(frame), None)
    if_exists_param = 'append'
    while start_index != end_index:
        log.info("Writing to %s rows %s through %s" % (table_name, start_index, end_index))
        frame.iloc[start_index:end_index, :].to_sql(con=engine, name=table_name, if_exists=if_exists_param, chunksize=chunk_size, dtype=dtype, index=False)
        if_exists_param = 'append'
        start_index = min(start_index + chunk_size, len(frame))
        end_index = min(end_index + chunk_size, len(frame))


def save_dataframe_to_sql(df, engine, table_name, if_exists_param, chunk_size, dtype, index):
    df.to_sql(con=engine, name=table_name, if_exists=if_exists_param, chunksize=chunk_size, dtype=dtype, index=index)


def sqlalchemy_create_table_from_pandas_dataframe(engine, df, tblname, keys, dtype):
    """
    sqlalchemy creates a new table base on pandas data frame structure
    if the tale existed, it will be deleted first
    :param engine:
    :param df:
    :param tblname:
    :keys: primary key constraint
    :return:
    """
    # schema = pd.io.sql.get_schema(df, tblname, con=engine, keys=df.columns)
    schema = pd.io.sql.get_schema(df, tblname, con=engine, keys=keys, dtype=dtype)
    try:
        engine.execute('DROP TABLE ' + tblname + ';')
    except Exception as ex:
        pass
    engine.execute(schema)


def write_to_db_2(engine, frame, table_name, chunk_size, dtype, log, thr_num=1):
    """
    drop the table if existed, then create a new one
    :param engine:
    :param frame:
    :param table_name:
    :param chunk_size:
    :param dtype:
    :param log:
    :return:
    """
    from common import thread_pool
    pool = thread_pool.ThreadPool(thr_num)
    tbls = []
    dfs = df_split_to_listofdfs(frame, int(len(frame)/50)) if len(frame)>=1000 else [frame] # 50 dfs
    for _, df in enumerate(dfs):
        tbl = 'temp_{0}{1}'.format(table_name, _)
        tbls.append(tbl)
        pool.add_task(write_to_db, engine, df, tbl, chunk_size, dtype, log)
    pool.wait_completion()
    sqlalchemy_create_table_from_pandas_dataframe(engine, frame, table_name, [], dtype)
    for t in tbls:
        engine.execute("insert into {0} select * from {1}".format(table_name, t))
        engine.execute("drop table {0}".format(t))


def append_to_db_2(engine, frame, table_name, chunk_size, dtype, log, thr_num=10):
    from common import thread_pool
    pool = thread_pool.ThreadPool(thr_num)
    tbls = []
    dfs = df_split_to_listofdfs(frame, int(len(frame)/100)) if len(frame)>=1000 else [frame] # 50 dfs
    for _, df in enumerate(dfs):
        tbl = 'tung_temp_{0}{1}'.format(table_name, _)
        tbls.append(tbl)
        pool.add_task(write_to_db, engine, df, tbl, chunk_size, dtype, log)
    pool.wait_completion()
    for t in tbls:
        # engine.execute("insert into {0} ([{2}]) select [{2}] from {1}".format(table_name, t, '], ['.join(frame.columns)))
        try:
            engine.execute(r"insert into {0} select * from {1}".format(table_name, t))
            engine.execute("drop table {0}".format(t))
        except sqlalchemy.exc.ProgrammingError as ex:  # TODO: EXCEPTON
            sqlalchemy_create_table_from_pandas_dataframe(engine, frame, table_name, [], dtype)
            engine.execute(r"insert into {0} select * from {1}".format(table_name, t))
            engine.execute("drop table {0}".format(t))


def sqlcol(dfparam, is_postgre=False):
    dtypedict = {}
    for i,j in zip(dfparam.columns, dfparam.dtypes):
        if "object" in str(j):
            # dtypedict.update({i: sqlalchemy.types.NVARCHAR(length_video=4000)})
            dtypedict.update({i: sqlalchemy.types.VARCHAR if is_postgre else sqlalchemy.types.NVARCHAR})
            # dtypedict.update({i: sqlalchemy.types.TEXT})
        if "datetime" in str(j):
            dtypedict.update({i: sqlalchemy.types.DateTime()})
        if "float" in str(j):
            dtypedict.update({i: sqlalchemy.types.Float(precision=3, asdecimal=True)})
        if "int" in str(j):
            dtypedict.update({i: sqlalchemy.types.INT()})
    return dtypedict


def splitDataFrameList(df, target_column, separator):
    column_names = df.columns
    ''' df_comp_files = dataframe to split,
    target_column = the column containing the values to split
    separator = the symbol used to perform the split
    returns: a dataframe with each entry for the target column separated, with each element moved into a new row. 
    The values in the other columns are duplicated across the newly divided rows.
    '''

    def splitListToRows(row, row_accumulator, target_column, separator):
        split_row = row[target_column].split(separator)
        # print(split_row)
        for s in split_row:
            new_row = row.to_dict()
            new_row[target_column] = s.strip()
            row_accumulator.append(new_row)

    new_rows = []
    df.apply(splitListToRows, axis=1, args=(new_rows, target_column, separator))
    new_df = pd.DataFrame(new_rows, columns=column_names)
    return new_df


# Python 3.5+ supports 'type annotations' that can be
# used with tools like Mypy to write statically typed Python:
def splitDataFrameList_1(df: pd.DataFrame, target_column: str, separator: str) -> list:
    return splitDataFrameList(df, target_column, separator)


def clean_duplicate_dropna_stripwhitespace_join(list_of_dfs, groupby_colname, new_colname, sep=r'\n'):
    '''
    :param list_of_dfs: list of dataframes
    :param groupby_colname: str: groupby column name
    :param new_colname: str: 
    :return: dataframe
    1. drop out duplicates from the current dataframe
    2. drop all na records
    3. strip whitespace
    4. group by groupby_colname, join together by '\n'
    '''
    for idx, e in enumerate(list_of_dfs):
        colnames = e.columns  # get columns names of the current dataframe
        e = e.dropna()  # drop all na records
        e[colnames[1]] = e[colnames[1]].str.strip()  # strip whitespace
        e = e.drop_duplicates()  # drop out duplicates from the current dataframe
        temp_series = e.groupby(groupby_colname)[colnames[1]].apply(lambda x: ', '.join(x))  # group by groupby_colname, join all the contents by ','
        e = pd.DataFrame(temp_series).reset_index()  #
        e.columns = [e.columns[0], new_colname]  # rename the second column name to ...
        list_of_dfs[idx] = e
    list_of_dfs = pd.concat(list_of_dfs)  # concat all dataframes in the list into one dataframe
    temp_series = list_of_dfs.groupby(groupby_colname)[new_colname].apply(lambda x: sep.join(x))  # group by groupby_colname, join all the notes by '\n'
    return pd.DataFrame(temp_series).reset_index()


def clean_duplicate_dropna_stripwhitespace_join_v1(list_of_dfs, prefixs, groupby_colname, new_colname, sep=r'\n'):
    """
    :rtype: object
    :param list_of_dfs: list of dataframes
    :param prefixs: list of string
    :param groupby_colname: str: groupby column name
    :param new_colname: str: 
    :return: dataframe
    1. drop out duplicates from the current dataframe
    2. drop all na records
    3. strip whitespace
    4. group by groupby_colname, join together by '\n'
    ignore warning:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fun....
    """
    for idx, e in enumerate(list_of_dfs):
        colnames = e.columns  # get columns names of the current dataframe
        e = e.dropna()  # drop all na records
        e[colnames[1]] = e[colnames[1]].astype(str).str.strip()  # strip whitespace
        #         e[colnames[1]] = [str(x).strip() for idx, x in e.iterrows()] # strip whitespace
        e = e.drop_duplicates()  # drop out duplicates from the current dataframe
        temp_series = e.groupby(groupby_colname)[colnames[1]].apply(lambda x: ', '.join(x))  # group by groupby_colname, join all the contents by ','
        e = pd.DataFrame(temp_series).reset_index()  #
        e.columns = [e.columns[0], new_colname]  # rename the second column name to ...
        # add prefix to the new col
        if idx <= prefixs.__len__() and prefixs.__len__() >= 1:
            #             e[new_colname] = prefixs[idx] + e[new_colname].astype(str)
            e[new_colname] = ["%s %s" % (prefixs[idx], str(x).replace('\r', '')) if (x is not None and str(x) != '') else np.nan for x in e[new_colname]]
            e = e.dropna()  # drop all na records
        list_of_dfs[idx] = e
    list_of_dfs = pd.concat(list_of_dfs)  # concat all dataframes in the list into one dataframe
    temp_series = list_of_dfs.groupby(groupby_colname)[new_colname].apply(lambda x: sep.join(x))  # group by groupby_colname, join all the notes by '\n'
    return pd.DataFrame(temp_series).reset_index()

def clean_duplicate_dropna_stripwhitespace_join_v2(list_of_dfs, prefixs, groupby_colnames, new_colname, sep='\n'):
    """
    :rtype: object
    :param list_of_dfs: list of dataframes
    :param prefixs: list of string
    :param groupby_colnames: str: groupby column name
    :param new_colname: str:
    :return: dataframe
    1. drop out duplicates from the current dataframe
    2. drop all na records
    3. strip whitespace
    4. group by groupby_colname, join together by '\n'
    ignore warning:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        fun....
    """
    for idx, e in enumerate(list_of_dfs):
        colnames = e.columns  # get columns names of the current dataframe
        e = e.dropna()  # drop all na records
        e[colnames[-1]] = e[colnames[-1]].astype(str).str.strip()  # strip whitespace for the last column

        #         e[colnames[1]] = [str(x).strip() for idx, x in e.iterrows()] # strip whitespace
        e = e.drop_duplicates()  # drop out duplicates from the current dataframe
        temp_series = e.groupby(groupby_colnames)[colnames[-1]].apply(lambda x: ', '.join(x))  # group by groupby_colname, join all the contents by ','
        e = pd.DataFrame(temp_series)  #.reset_index()
        e.rename(columns={colnames[-1]:new_colname}, inplace=True) # rename the second column name to ...
        # add prefix to the new col
        if idx <= prefixs.__len__() and prefixs.__len__() >= 1:
            #             e[new_colname] = prefixs[idx] + e[new_colname].astype(str)
            e[new_colname] = ["%s %s" % (prefixs[idx], str(x).replace('\r', '')) if (x is not None and str(x) != '') else np.nan for x in e[new_colname]]
            e = e.dropna()  # drop all na records
        list_of_dfs[idx] = e
    list_of_dfs = pd.concat(list_of_dfs)  # concat all dataframes in the list into one dataframe
    temp_series = list_of_dfs.groupby(groupby_colnames)[new_colname].apply(lambda x: sep.join(x))  # group by groupby_colname, join all the notes by '\n'
    return pd.DataFrame(temp_series).reset_index()






def df_split_to_listofdfs(df, max_rows=50000):
    dataframes = []
    while len(df) > max_rows:
        top = df[:max_rows]
        dataframes.append(top)
        df = df[max_rows:]
    else:
        dataframes.append(df)
    return dataframes


def read_sql_to_dataframe(sql, db_conn, offset=0, chunk_size=300, logger=None):
    dfs = []
    while True:
        logger.info("Fetching for rows from %d to next %d rows" % (offset, chunk_size))
        temsql = sql % (offset, chunk_size)
        df = pd.read_sql(temsql, db_conn)
        offset += chunk_size
        dfs.append(df)
        if len(df) < chunk_size:
            break
    return pd.concat(dfs)


def read_sql_to_csv(sql, db_conn, csvfile, offset=0, chunk_size=300, logger=None):
    index = 0
    while True:
        logger.info("Fetching for rows from %d to next %d rows" % (offset, chunk_size))
        temsql = sql % (offset, chunk_size)
        df = pd.read_sql(temsql, db_conn)
        offset += chunk_size
        if index == 0:
            df.to_csv(csvfile, mode='w', index=False, header=True, sep=',')
        else:
            df.to_csv(csvfile, mode='a', index=False, header=False, sep=',')
        index += 1
        if len(df) < chunk_size:
            break


def read_sql_to_csv1(sql, db_conn, csvfile, offset=0, chunk_size=300, logger=None):
    index = 0
    while True:
        if (logger is not None):
            logger.info("Fetching for rows from %d to next %d rows" % (offset, chunk_size))
        temsql = sql % (offset, chunk_size)
        try:
            df = pd.read_sql(temsql, db_conn)
        except:
            df = None
        if df is not None:
            offset += chunk_size
            if index == 0:
                df.to_csv(csvfile, mode='w', index=False, header=True, sep=',')
            else:
                df.to_csv(csvfile, mode='a', index=False, header=False, sep=',')
            index += 1
            if len(df) < chunk_size:
                break


def read_sql_to_multi_csv(sql, db_conn, csvfile, offset=0, chunk_size=300, logger=None):
    index = 0
    while True:
        if (logger is not None):
            logger.info("Fetching for rows from %d to next %d rows" % (offset, chunk_size))
        temsql = sql % (offset, chunk_size)
        try:
            df = pd.read_sql(temsql, db_conn)
        except:
            df = None
        if df is not None:
            offset += chunk_size
            df.to_csv(csvfile.format(index, len(df)), mode='w', index=False, header=True, sep=',')
            index += 1
            if len(df) < chunk_size:
                break


def read_sql_to_multi_csv_2(sql, connection_str, csvfile, offset, chunk_size, totalrow, logger, t=0):
    def que(index, offset, chunk_size):
        logger.info("Fetching for rows from %d to next %d rows" % (offset, chunk_size))
        temsql = sql % (offset, chunk_size)
        try:
            sdbconn_engine = sqlalchemy.create_engine(connection_str)
            df = pd.read_sql(temsql, sdbconn_engine)
        except Exception as ex:
            logger.error("!!!Fetching for rows from %d to next %d rows" % (offset, chunk_size))
            logger.error(ex)
            logger.error("This worker wil go into sleep mode for 60 seconds at times %s, the rerun. If error occur after 10 times, it will be not rerun anymore" % t)
            time.sleep(60)
            if t < 10:
                return read_sql_to_multi_csv_2(sql, connection_str, csvfile, offset, chunk_size, totalrow, logger, t=t+1)
            df = None
        if df is not None:
            df.to_csv(csvfile.format(index, len(df)), mode='w', index=False, header=True, sep=',')
    from common import thread_pool
    pool = thread_pool.ThreadPool(100)
    times = (totalrow-offset)//chunk_size
    for i in range(0, times+1):
        # print('{}_{}'.format(offset, chunk_size))
        pool.add_task(que, i, offset, chunk_size)
        offset += chunk_size


def upload_files_to_s3(upload_folder,
                       bucket ='sin-vc-p1-file',
                       key = '/tung.vincere.io/upload/file/1b9c5a18-ec95-4283-92ae-29600bc2d508/',
                       access_key = '',
                       secret_key = '+', log=None):
    conn = boto.connect_s3(access_key, secret_key)
    bucket = conn.get_bucket(bucket)
    uploaded_fail = []
    total_files = sum([len(files) for r, d, files in os.walk(upload_folder)])
    total_uploaded_files = 0
    for root, dirs, files in os.walk(upload_folder):
        # print("root: %s" % root)
        # print("dirs: %s" % dirs)
        # print("files: %s" % files)
        # print("#--------------------------------")
        for idx, file in enumerate(files):
            total_uploaded_files += 1
            try:
                key_is_existed = bucket.get_key(key + file)
                if not key_is_existed:
                    if log:
                        log.info('uploading: %i/%i - %s' % (total_uploaded_files, total_files, os.path.join(root, file)))
                    k = bucket.new_key(key + file)
                    k.set_contents_from_filename(os.path.join(root, file))
                else:
                    if log:
                        log.info('existed: %s' % os.path.join(root, file))

            except Exception as e:
                total_uploaded_files -= 1
                uploaded_fail.append(
                    {'file': file, 'root': root}
                )
                if log:
                    log.error(str(e))
    while len(uploaded_fail):
        e = uploaded_fail.pop(0)
        total_uploaded_files += 1
        file = e.get('file')
        root = e.get('root')
        try:
            key_is_existed = bucket.get_key(key + file)
            if not key_is_existed:
                if log:
                    log.info('re-uploading: %i/%i - %s' % (total_uploaded_files, total_files, os.path.join(root, file)))
                k = bucket.new_key(key + file)
                k.set_contents_from_filename(os.path.join(root, file))
            else:
                if log:
                    log.info('existed: %s' % os.path.join(root, file))

        except Exception as e:
            total_uploaded_files -= 1
            uploaded_fail.append(
                {'file': file, 'root': root}
            )
            if log:
                log.error(str(e))

def map_column_name_company(cols):
    # company
    cols = cols.map(lambda x: 'company-externalId' if re.match(r'.*_mapto_companyexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'company-name' if re.match(r'.*_mapto_companyname', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'company-Owners' if re.match(r'.*_mapto_companyowner', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'company-switchBoard' if re.match(r'.*_mapto_companyswitchboard', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'company-website' if re.match(r'.*_mapto_companywebsite', x, flags=re.IGNORECASE) else x)
    return cols

def map_column_name_contact(cols):
    # contact
    cols = cols.map(lambda x: 'contact-externalId' if re.match(r'.*_mapto_contactexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-firstName' if re.match(r'.*_mapto_contactfirstname', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-lastName' if re.match(r'.*_mapto_contactlastname', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-title' if re.match(r'.*_mapto_contacttitle', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-email' if re.match(r'.*_mapto_contactemail', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-jobTitle' if re.match(r'.*_mapto_contactposition', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-companyId' if re.match(r'.*_mapto_contactcompanyexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-owners' if re.match(r'.*_mapto_contactowner', x, flags=re.IGNORECASE) else x)
    # cols = cols.map(lambda x: 'contact-externalId' if re.match(r'.*_mapto_contactCurrentAddress', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'contact-linkedin' if re.match(r'.*_mapto_contactlinkedin', x, flags=re.IGNORECASE) else x)
    # cols = cols.map(lambda x: 'contact-externalId' if re.match(r'.*_mapto_contactprimaryphone', x, flags=re.IGNORECASE) else x)
    # cols = cols.map(lambda x: 'contact-externalId' if re.match(r'.*_mapto_contactmobilephone', x, flags=re.IGNORECASE) else x)
    return cols

def map_column_name_job(cols):
    # job
    cols = cols.map(lambda x: 'position-externalId' if re.match(r'.*_mapto_jobexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'position-externalId' if re.match(r'.*mapto_positionexternalid', x) else x)
    cols = cols.map(lambda x: 'position-contactId' if re.match(r'.*_mapto_jobcontactexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'position-contactId' if re.match(r'.*mapto_contactexternalid', x) else x)
    cols = cols.map(lambda x: 'position-type' if re.match(r'.*mapto_positiontype', x) else x)
    cols = cols.map(lambda x: 'position-type' if re.match(r'.*_mapto_jobtype', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'position-companyId' if re.match(r'.*_mapto_jobcompanyexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'position-companyId' if re.match(r'.*mapto_companyexternalid', x) else x)
    cols = cols.map(lambda x: 'position-title' if re.match(r'.*_mapto_jobtitle', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'position-title' if re.match(r'.*mapto_positiontitle', x) else x)
    cols = cols.map(lambda x: 'position-owners' if re.match(r'.*_mapto_jobowner', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'position-owners' if re.match(r'.*mapto_positionowner', x) else x)
    cols = cols.map(lambda x: 'position-startDate' if re.match(r'.*mapto_positionstartdate', x) else x)
    cols = cols.map(lambda x: 'position-endDate' if re.match(r'.*mapto_positionenddate', x) else x)
    return cols


def map_column_name_candidate(cols):
    # job
    cols = cols.map(lambda x: 'candidate-externalId' if re.match(r'.*_mapto_candidateexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-firstName' if re.match(r'.*_mapto_candidatefirstname', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-Lastname' if re.match(r'.*_mapto_candidatelastname', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-middleName' if re.match(r'.*_mapto_candidateMiddleName', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-email' if re.match(r'.*_mapto_candidateemail', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-company1' if re.match(r'.*_mapto_candidatecurrentemployer', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-jobTitle1' if re.match(r'.*_mapto_candidateCurrentPosition', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-jobType' if re.match(r'.*_mapto_candidatejobType', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'candidate-owners' if re.match(r'.*_mapto_candidateowner', x, flags=re.IGNORECASE) else x)
    return cols


def map_column_name_application(cols):
    """

    :rtype:
    """
    # job
    cols = cols.map(lambda x: 'application-candidateExternalId' if re.match(r'.*_mapto_applicationcandidateexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'application-positionExternalId' if re.match(r'.*_mapto_applicationjobexternalid', x, flags=re.IGNORECASE) else x)
    cols = cols.map(lambda x: 'application-actionedDate' if re.match(r'.*_mapto_applicationdate', x, flags=re.IGNORECASE) else x)
    return cols


def get_folder_structure(fol):
    folders = []

    fullfilenames = []
    filenames = []
    roots = []
    size = []
    last_modification_dates = []
    for f in os.scandir(fol):
        if f.is_dir():
            folders.append(f)
        if f.is_file():
            fullfilenames.append(f.path)
            filenames.append(f.name)
            roots.append(f.path.replace(f.name,'')[:-1])
            last_modification_dates.append(datetime.datetime.fromtimestamp(os.path.getmtime(f.path)))
        while len(folders):
            for subf in (os.scandir(folders.pop())):
                if subf.is_dir():
                    folders.append(subf)
                if subf.is_file():
                    fullfilenames.append(subf.path)
                    filenames.append(subf.name)
                    # size.append(os.path.getsize(subf.path))
                    roots.append(subf.path.replace(subf.name, '')[:-1])
                    last_modification_dates.append(datetime.datetime.fromtimestamp(os.path.getmtime(subf.path)))


    folder_files = pd.concat([
        pd.Series(filenames, name='file'),
        pd.Series(roots, name='root'),
        pd.Series(fullfilenames, name='file_fullpath'),
        pd.Series(last_modification_dates, name='last_modification_date'),
        # pd.Series(size, name='size'),
    ], axis=1)
    folder_files['extension_file'] = folder_files['file'].map(lambda x:  re.search(r"\.[a-zA-Z0-9_ ]*(?!.*\.[a-zA-Z0-9_ ]*)", x))
    folder_files['alter_file1'] = folder_files['file'].map(lambda x: re.sub(r"%|\$|&| |'|\[|\]|-|\+", '_', x))
    folder_files['alter_file2'] = folder_files['extension_file'].map(lambda x: ('%s%s') % (str(uuid.uuid4()), x.group() if x else ''))
    return folder_files

class Common():
    def __init__(self):
        """
                temporary and temp-to-perm are not be used any more
                """
        tem = np.array([[1, 5, 4, 2, 3], list(map(str.lower, ['permanent', 'project consulting', 'temporary', 'contract', 'temp-to-perm']))])
        self.jobtype = pd.DataFrame(tem.transpose(), columns=['position_type', 'desc'])
        self.jobtype['position_type'] = self.jobtype['position_type'].astype(np.int8)

    def get_country_code(self, country_name):
        """https://www.nationsonline.org/oneworld/country_code_list.htm"""
        from common import country_code
        return_code = ''
        for k, v in country_code.country_codes.items():
            # print("code {0}, name {1}".format(k, v))
            if str(country_name).lower().strip() in [i.lower() for i in v]:  # check an item exits in the tuple v
                return_code = k
                break
        return return_code

    def process_vincere_email(self, df, id_col, email_col):
        # email
        # df_comp_files[email_col].fillna('', inplace=True) # fill '' for missing emails
        df[email_col] = df[email_col].fillna('')
        df[email_col] = df.apply(
            lambda x: ','.join(set(re.findall(regex_pattern_email, x[email_col]))  # set does not allow duplicates
                               ), axis=1)  # extract valid email, remove dup email of the same contact, join them by ','
        df[email_col] = df.apply(
            lambda x: ('No_email_' + str(x[id_col]) + '@email.com') if len(x[email_col]) == 0 else x[email_col],
            axis=1)  # set fake email for missing values

        df_email = splitDataFrameList(df[[id_col, email_col]], email_col, ',')
        df_email[email_col] = df_email[email_col].astype(str)
        #     print(df_email.dtypes)
        #     for i, e in df_email.iterrows():
        #         if type(e) == 'float':
        #             print('%i_%s' % (i, e))
        df_email['email_cumcount'] = df_email.groupby(
            df_email[email_col].str.lower()).cumcount() + 1  # group by email case insensitive

        df_email[email_col] = df_email.apply(
            lambda x: x[email_col] if x['email_cumcount'] == 1 else "%s_%s" % (x['email_cumcount'], x[email_col]),
            axis=1)  # add prefix for dup contact email

        # EDIT 20190621: multi email are not allowed any more
        df_email['rn'] = df_email.groupby(id_col).cumcount()
        df_email = df_email.query("rn==0")

        # COMMENT EDIT 20190621: multi email are not allowed any more
        # temp_series = df_email.groupby(id_col)[email_col].apply(lambda x: ', '.join(x))  # group by groupby_colname, join all the contents by ','
        # df_email = pd.DataFrame(temp_series).reset_index()  #

        df.drop(email_col, axis=1, inplace=True)
        return pd.merge(
            left=df,
            right=df_email,
            left_on=id_col,
            right_on=id_col,
            how='left'
        )

    def set_position_employment_type(self, x):
        if re.sub(r"\s{1,}", '', x).lower() in ('p', 'permanent', 'fulltime', 'full_time'):
            return 'FULL_TIME'
        elif re.sub(r"\s{1,}", '', x).lower() in ('c', 'contract', 'casual'):
            return 'CASUAL'
        elif re.sub(r"\s{1,}", '', x).lower() in ('t', 'parttime', 'part_time', 'temporary'):
            return 'PART_TIME'
        else:
            return 'FULL_TIME'

    def map_currency_code(self, currency, default=None):
        return_name = None
        for k, v in currency_codes.items():
            # print("code {0}, name {1}".format(k, v))
            if str(currency).lower().strip() in v:  # check an item exits in the tuple v
                return_name = k
                break
        return return_name if return_name is not None else default

    def set_position_type(self, x):
        try:
            if x.lower() in ('p', 'permanent', 'fulltime'):
                return 'PERMANENT'
            elif x.lower() in ('c', 'contract'):
                return 'CONTRACT'
            elif x.lower() in ('t', 'parttime', 'temporary', 'temporary_to_permanent'):
                return 'TEMPORARY_TO_PERMANENT'
            else:
                return 'PERMANENT'
        except Exception as e:
            print('input value %s' % x)
            raise TypeError('%s' % e)

    def process_title(self, df, title_col):
        title = {
            'MR': ('mr', 'm r'),
            'MRS': ('mrs',),
            'MS': ('ms', 'm s'),
            'MISS': ('miss', 'mis'),
            'DR': ('dr',),
        }
        df[title_col] = ['MR' if str(x).strip().lower() in title.get('MR') else x for x in df[title_col]]
        df[title_col] = ['MRS' if str(x).strip().lower() in title.get('MRS') else x for x in df[title_col]]
        df[title_col] = ['MS' if str(x).strip().lower() in title.get('MS') else x for x in df[title_col]]
        df[title_col] = ['MISS' if str(x).strip().lower() in title.get('MISS') else x for x in df[title_col]]
        df[title_col] = ['DR' if str(x).strip().lower() in title.get('DR') else x for x in df[title_col]]
        # other are blank
        df[title_col] = [x if x in ('MR', 'MRS', 'MS', 'MISS', 'DR',) else '' for x in df[title_col]]


