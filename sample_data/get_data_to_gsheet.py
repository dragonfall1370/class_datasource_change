# coding: utf-8
import os, sys
sys.path.append(os.path.dirname(os.getcwd()))

import pandas as pd
import psycopg2
import pymysql.cursors
import pyodbc
from sqlalchemy import create_engine
import pygsheets
from pathlib import Path
from datetime import datetime, timedelta
import urllib

def connection(connection_type, db_name): 
    if connection_type == 'postgres_fra':
        connection = psycopg2.connect(database=db_name,
                                    user='postgres',
                                    host='dmpfra.vinceredev.com',
                                    port='5432',
                                    password='123456')
    elif connection_type == 'postgres_us':
        connection = psycopg2.connect(database=db_name,
                                    user='postgres',
                                    host='dmpus.vinceredev.com',
                                    port='5432',
                                    password='123456')
    elif connection_type == 'postgres_sing':
        connection = psycopg2.connect(database=db_name,
                                    user='postgres',
                                    host='dmp.vinceredev.com',
                                    port='5432',
                                    password='123456')
    elif connection_type == 'mysql':
        connection = pymysql.connect(host='dmpfra.vinceredev.com', 
                                    port=3306,
                                    user='root',
                                    password='123qwe',
                                    db=db_name)
    elif connection_type == 'sql_server_fra':
        connection = pyodbc.connect('driver={%s};server=%s;database=%s;UID=%s;PWD=%s;' \
                                % ('ODBC Driver 17 for SQL Server', 'dmpfra.vinceredev.com', db_name, 'sa', '123$%^qwe'))
    elif connection_type == 'sql_server_us':
        connection = pyodbc.connect('driver={%s};server=%s;database=%s;UID=%s;PWD=%s;' \
                                % ('ODBC Driver 17 for SQL Server', 'dmpus.vinceredev.com', db_name, 'sa', '123$%^qwe'))
    elif connection_type == 'sql_server_sing':
        connection = pyodbc.connect('driver={%s};server=%s;database=%s;UID=%s;PWD=%s;' \
                                % ('ODBC Driver 17 for SQL Server', 'dmp.vinceredev.com', db_name, 'sa', '123$%^qwe'))                         
    elif connection_type == 'postgres_engine_fra':
        engine = create_engine('postgresql+psycopg2://postgres:123456@dmpfra.vinceredev.com/' + db_name)
        connection = engine.connect()
    elif connection_type == 'postgres_engine_us':
        engine = create_engine('postgresql+psycopg2://postgres:123456@dmpus.vinceredev.com/' + db_name)
        connection = engine.connect()
    elif connection_type == 'postgres_engine_sing':
        engine = create_engine('postgresql+psycopg2://postgres:123456@dmp.vinceredev.com/' + db_name)
        connection = engine.connect()
    elif connection_type == 'sql_server_engine_fra':
        params = urllib.parse.quote_plus('DRIVER=ODBC Driver 17 for SQL Server;Server=dmpfra.vinceredev.com;Database=' + db_name + ';UID=sa;PWD=123$%^qwe;Port=1433;')
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))
        connection = engine.connect()
    elif connection_type == 'sql_server_engine_us':
        params = urllib.parse.quote_plus('DRIVER=ODBC Driver 17 for SQL Server;Server=dmpus.vinceredev.com;Database=' + db_name + ';UID=sa;PWD=123$%^qwe;Port=1433;')
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))
        connection = engine.connect()
    elif connection_type == 'sql_server_engine_sing':
        params = urllib.parse.quote_plus('DRIVER=ODBC Driver 17 for SQL Server;Server=dmp.vinceredev.com;Database=' + db_name + ';UID=sa;PWD=123$%^qwe;Port=1433;')
        engine = create_engine('mssql+pyodbc:///?odbc_connect={}'.format(params))
        connection = engine.connect()
    else:
        print('DB name is not supported')
        connection = None
    return connection


def get_data(region, db_type, db_name, table_name, limit):
    df = pd.DataFrame()

    if db_type == 'postgres':
        # create engine
        if region == 'fra':
            engine = connection('postgres_engine_fra', db_name)
        elif region == 'us':
            engine = connection('postgres_engine_us', db_name)
        elif region == 'sing':
            engine = connection('postgres_engine_sing', db_name)
        else:
            print('Unrecognized region!!!')

        # create cursor
        if region == 'fra':
            _connection = connection('postgres_fra', db_name)
            cursor = _connection.cursor()
        elif region == 'us':
            _connection = connection('postgres_us', db_name)
            cursor = _connection.cursor()
        elif region == 'sing':
            _connection = connection('postgres_sing', db_name)
            cursor = _connection.cursor()
        else:
            print('Unrecognized region!!!')

        #check if table exists
        cursor.execute("SELECT CASE WHEN EXISTS (SELECT * FROM information_schema.tables WHERE table_name=%s) THEN 1 ELSE 0 END", (table_name,))

    elif db_type == 'sql_server':
        # create engine
        if region == 'fra':
            engine = connection('sql_server_engine_fra', db_name)
        elif region == 'us':
            engine = connection('sql_server_engine_us', db_name)
        elif region == 'sing':
            engine = connection('sql_server_engine_sing', db_name)
        else:
            print('Unrecognized region!!!')

        # create cursor
        if region == 'fra':
            _connection = connection('sql_server_fra', db_name)
            cursor = _connection.cursor()
        elif region == 'us':
            _connection = connection('sql_server_us', db_name)
            cursor = _connection.cursor()
        elif region == 'sing':
            _connection = connection('sql_server_sing', db_name)
            cursor = _connection.cursor()
        else:
            print('Unrecognized region!!!')

        #check if table exists
        cursor.execute("SELECT CASE WHEN EXISTS (SELECT * FROM information_schema.tables WHERE table_name=?) THEN 1 ELSE 0 END", table_name)
    
    if cursor.fetchone()[0] == 1:
        list_columns = []
        cursor.execute("SELECT column_name FROM information_schema.columns WHERE table_name=%s", (table_name,))
        for row in cursor:
            list_columns.append(row[0])
        columns_with_quote = ['"' + x + '"' if '-' in x else x for x in list_columns]

        if db_type == 'postgres':    
            convert_columns_to_text = [x + '::text' for x in columns_with_quote]
        elif db_type == 'sql_server':
            convert_columns_to_text = ['CAST ' + x + 'AS VARCHAR' for x in columns_with_quote]
        join_columns = ','.join(convert_columns_to_text)
        query = 'SELECT * FROM {0} ORDER BY CASE WHEN COALESCE({1}) IS NOT NULL THEN 1 ELSE 2 END LIMIT {2}'.format(table_name, join_columns, limit)
        df = pd.read_sql_query(query, engine)
        df = df.fillna('')
    else:
        print('Table {} not found\n'.format(table_name))
    
    return df

# ----------------------------------------------------Process start here------------------------------------------------
print('Starting at {}\n'.format(datetime.now() + timedelta(hours=6)))
if __name__ == "__main__":
    url = str(sys.argv[1])
    region = str(sys.argv[2])
    db_type = str(sys.argv[3])
    db_name = str(sys.argv[4])
    limit = int(sys.argv[5])

    credential_file = Path(os.getcwd() +  '\\google-sheets-4f0fcdc59c49.json')
    client = pygsheets.authorize(service_account_file=credential_file)

    sheet = client.open_by_url(url)

    if not sheet.worksheet('title', 'Data'):
        sheet.add_worksheet(title='Data', rows=limit*4+5)
    working_sheet = sheet.worksheet_by_title('Data')

    table_names = ['_01_company_sample', '_02_contact_sample', '_03_job_sample', '_04_candidate_sample']
    working_sheet.clear()
    start_row = 1
    for table in table_names:
        print('Executing {}...\n'.format(table))
        df = get_data(region, db_type, db_name, table, limit)
        working_sheet.set_dataframe(df, (start_row,1))
        start_row += 502
    
    print('Finished at: {}'.format(datetime.now() + timedelta(hours=6)))