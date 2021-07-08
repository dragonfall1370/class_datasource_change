from numpy.lib.function_base import append
import pyodbc
import pandas as pd
from sqlalchemy import create_engine
import os
import glob

class create_database_for_new_source ():
    def __init__(self, database_name, schema_name, path, method):
        self.database_name = database_name;
        self.schema_name = schema_name;
        self.path = path;
        self.method = method;

        self.DRIVER = "ODBC Driver 17 for SQL Server";
        self.SERVERNAME = "LAPTOP-9P3BKBLO";
        self.DB=database_name;


        # create new database locally based on the requests

    def left(s, amount):
        return s[:amount]

    def right(s, amount):
        return s[-amount:]

    def mid(s, offset, amount):
        return s[offset:offset+amount]


    def run_csv(self):   
            try:
                local_connection_string = r'Driver={ODBC Driver 17 for SQL Server};Server=LAPTOP-9P3BKBLO;Trusted_Connection=yes;';
                                    
                local_conn_create_database = pyodbc.connect(local_connection_string, autocommit=True);
                cur = local_conn_create_database.cursor();

                check_existing_database = r"DROP DATABASE IF EXISTS " + self.database_name;
                cur.execute(check_existing_database);

                create_new_database = r"Create database " + self.database_name;
                cur.execute(create_new_database);


                cur.close();
                local_conn_create_database.close();

                # read all csv tables inside the 

                extension = 'csv';
                os.chdir(self.path);
                all_csv_file = glob.glob('*.{}'.format(extension))

                # create new table in new database locally 

                local_connection_string_table = r'Driver={ODBC Driver 17 for SQL Server};Server=LAPTOP-9P3BKBLO;Trusted_Connection=yes;Database='+self.database_name;
                                    
                local_conn_create_table = pyodbc.connect(local_connection_string_table, autocommit=True);
                cur_table = local_conn_create_table.cursor();

                #delete existed schema in the database

                check_existing_schema = r"DROP SCHEMA IF EXISTS " + self.schema_name;
                cur_table.execute(check_existing_schema);


                create_new_schema =  "create schema " + self.schema_name;
                cur_table.execute(create_new_schema);

                # create the new engine to the local sql server


                DRIVER = self.DRIVER;
                SERVERNAME = self.SERVERNAME;
                DB=self.DB;


                engine = create_engine(f"mssql+pyodbc://{SERVERNAME}/{DB}?driver={DRIVER}", fast_executemany=True)



                print(engine)

                for table_name in all_csv_file:
                    
                    table_name_final = table_name[:-4]


                    check_existing_table = r"""IF OBJECT_ID(N'""" + table_name_final + r"""' , 'U') IS NOT NULL DROP TABLE """ + table_name_final
                
                    cur_table.execute(check_existing_table);   

                    df = pd.read_csv(os.path.join(self.path, table_name));
                
                    df.to_sql(table_name_final, con=engine, schema=self.schema_name, if_exists='append', method='multi', index=False, chunksize=100)

                cur_table.close();
                local_conn_create_table.close();
            except:
                pass

    









