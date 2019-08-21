
# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import numpy as np
import pymssql
import os
import common.vincere_common as vincere_common
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
fr = cf[cf['default'].get('src_db')]
mylog = log.get_info_logger(log_file)
#
# create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)

if __name__ == '__main__':
    sdbconn = pymssql.connect(server=fr.get('server'), user=fr.get('user'), password=fr.get('password'), database=fr.get('database'), as_dict=True)

    sql = """
       select  
       LGpers as candidate_external_id, 
       0 as contact_external_id,
       LGvac as position_external_id, 
       0 as company_external_id,
       LGentered as insert_timestamp,
       concat(LGtype, ': ', LGdet) as content,
       LGuser as userlogin,
       'comment' as category
       from CandLog
       order by LGentered OFFSET %d ROWS FETCH NEXT %d ROWS ONLY
       """
    df = vincere_common.read_sql_to_csv1(sql, sdbconn, os.path.join(data_input, 'activities1.csv'), offset=0, chunk_size=10000)

    sql = """
       select 
       0 as candidate_external_id,
       CTLGpers as contact_external_id, 
       CTLGvac as position_external_id, 
       CTLGcomp as company_external_id, 
       CTLGentered as insert_timestamp,
       concat(CTLGtype, ': ', CTLGdet) as content,
       CTLGuser as userlogin,
       'comment' as category
       from ClientLog
       order by CTLGentered OFFSET %d ROWS FETCH NEXT %d ROWS ONLY
       """
    df = vincere_common.read_sql_to_csv1(sql, sdbconn, os.path.join(data_input, 'activities2.csv'), offset=0, chunk_size=10000)

if False:
#     df_company = pd.read_sql(r'select * from comps where COactive=1', sdbconn)
    df_company = pd.read_sql(r'select * from comps', sdbconn)
    df_company.to_csv(os.path.join(data_input, 'company.csv'), index=False, header=True, sep=',')
    
    df_candidate = pd.read_sql(r'select * from cand', sdbconn)
    df_candidate.to_csv(os.path.join(data_input, 'candidate.csv'), index=False, header=True, sep=',')
 
#     df_contact = pd.read_sql(r'select * from clients  where CTactive=1', sdbconn)
    df_contact = pd.read_sql(r'select * from clients', sdbconn)
    df_contact.to_csv(os.path.join(data_input, 'contact.csv'), index=False, header=True, sep=',')

    df_job = pd.read_sql(r'select * from vacancy', sdbconn)
    df_job.to_csv(os.path.join(data_input, 'job.csv'), index=False, header=True, sep=',')

    df_user_options = pd.read_sql(r'select * from UserOptions', sdbconn)
    df_user_options.to_csv(os.path.join(data_input, 'user_options.csv'), index=False, header=True, sep=',')

    df = pd.read_sql(r"""
    select Vacancy, Candidate, Status, RegDate from IntControl 
    where Vacancy in (select VAnum from Vacancy)
    and Candidate in (select CAnum from Cand)
    """, sdbconn)
    df.to_csv(os.path.join(data_input, 'job_app.csv'), index=False, header=True, sep=',')
    
    df = pd.read_sql(r"""
    select  
    LGpers as candidate_external_id, 
    0 as contact_external_id,
    LGvac as position_external_id, 
    0 as company_external_id,
    LGentered as insert_timestamp,
    concat(LGtype, ': ', LGdet) as content,
    LGuser as userlogin,
    'comment' as category
    from CandLog
    """, sdbconn)
    df.to_csv(os.path.join(data_input, 'activities1.csv'), index=False, header=True, sep=',')
    
    df = pd.read_sql(r"""
    select 
    0 as candidate_external_id,
    CTLGpers as contact_external_id, 
    CTLGvac as position_external_id, 
    CTLGcomp as company_external_id, 
    CTLGentered as insert_timestamp,
    concat(CTLGtype, ': ', CTLGdet) as content,
    CTLGuser as userlogin,
    'comment' as category
    from ClientLog
    """, sdbconn)
    df.to_csv(os.path.join(data_input, 'activities2.csv'), index=False, header=True, sep=',')

    sql = """
    select  
    LGpers as candidate_external_id, 
    0 as contact_external_id,
    LGvac as position_external_id, 
    0 as company_external_id,
    LGentered as insert_timestamp,
    concat(LGtype, ': ', LGdet) as content,
    LGuser as userlogin,
    'comment' as category
    from CandLog
    order by LGentered OFFSET %d ROWS FETCH NEXT %d ROWS ONLY
    """
    df = vincere_common.read_sql_to_csv1(sql, sdbconn, os.path.join(data_input, 'activities1.csv'), offset=0, chunk_size=10000)

    sql = """
    select 
    0 as candidate_external_id,
    CTLGpers as contact_external_id, 
    CTLGvac as position_external_id, 
    CTLGcomp as company_external_id, 
    CTLGentered as insert_timestamp,
    concat(CTLGtype, ': ', CTLGdet) as content,
    CTLGuser as userlogin,
    'comment' as category
    from ClientLog
    order by CTLGentered OFFSET %d ROWS FETCH NEXT %d ROWS ONLY
    """
    df = vincere_common.read_sql_to_csv1(sql, sdbconn, os.path.join(data_input, 'activities2.csv'), offset = 0, chunk_size = 10000)




