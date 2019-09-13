# -*- coding: UTF-8 -*-
import psycopg2.extras
import numpy as np
import pandas as pd
from common import vincere_common
from os import path
import warnings
from dateutil.relativedelta import relativedelta
import datetime
import pathlib
import sqlalchemy
import re
import time
from deprecated import deprecated
# https://github.com/googlemaps/google-maps-services-python
import googlemaps


def clean_activities(db_conn, logger=None):
    cur = db_conn.cursor()
    # clear activity mapping and activity
    if logger:
        logger.info('delete from activity_job')
    cur.execute("delete from activity_job")
    if logger:
        logger.info('delete from activity where position_id is not NULL and position_id > 0')
    cur.execute("delete from activity where position_id is not NULL and position_id > 0")
    if logger:
        logger.info('delete from activity_candidate')
    cur.execute("delete from activity_candidate")
    if logger:
        logger.info('delete from activity where candidate_id is not NULL and candidate_id > 0')
    cur.execute("delete from activity where candidate_id is not NULL and candidate_id > 0")
    if logger:
        logger.info('delete from activity_company')
    cur.execute("delete from activity_company")
    if logger:
        logger.info('delete from activity where company_id is not NULL and company_id > 0')
    cur.execute("delete from activity where company_id is not NULL and company_id > 0")
    if logger:
        logger.info('delete from activity_contact')
    cur.execute("delete from activity_contact")
    if logger:
        logger.info('delete from activity where contact_id is not NULL and contact_id > 0')
    cur.execute("delete from activity where contact_id is not NULL and contact_id > 0")
    # commit the changes to the database
    db_conn.commit()


def clean_activities_2(ddbconn, logger):
    cur = ddbconn.cursor()
    cur.execute("""
       truncate  activity_job;
       truncate  activity_candidate;
       truncate  activity_company;
       truncate  activity_contact;
       """)
    ddbconn.commit()
    # remove activities
    test = pd.read_sql("select count(*), min(id), max(id) from activity", ddbconn)
    delfr = test.loc[0, 'min']
    if test.loc[0, 'count']:
        for i in range(test.loc[0, 'min'], test.loc[0, 'max'], 5000):
            sql = 'delete from activity where id between %s and %s' % (delfr, i)
            logger.info(sql)
            delfr = i
            cur = ddbconn.cursor()
            cur.execute(sql)
            ddbconn.commit()
        sql = 'delete from activity where id between %s and %s' % (i, test.loc[0, 'max'])
        cur = ddbconn.cursor()
        cur.execute(sql)
        ddbconn.commit()


def clean_reinsert_activity_mapping(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
    truncate  activity_job;
    truncate  activity_candidate;
    truncate  activity_company;
    truncate  activity_contact;
    """)
    db_conn.commit()
    # insert activity mapping
    cur.execute(
        "insert into activity_candidate (activity_id, candidate_id, insert_timestamp) "
        + "SELECT id "
        + ", candidate_id "
        + ", insert_timestamp "
        + "from activity "
        + "where candidate_id is not NULL and candidate_id > 0 and id not in (23, 24, 25) "
    )
    db_conn.commit()
    cur.execute(
        "insert into activity_company (activity_id, company_id, insert_timestamp) "
        + "SELECT id "
        + ", company_id "
        + ", insert_timestamp "
        + "from activity "
        + "where company_id is not NULL and company_id > 0 and id not in (23, 24, 25) "
    )
    db_conn.commit()
    cur.execute(
        "insert into activity_contact (activity_id, contact_id, insert_timestamp) "
        + "SELECT id "
        + ", contact_id "
        + ", insert_timestamp "
        + "from activity "
        + "where contact_id is not NULL and contact_id > 0 and id not in (23, 24, 25) "
    )
    db_conn.commit()
    cur.execute(
        """insert into activity_job (activity_id, job_id, insert_timestamp) 
        SELECT id 
        , position_id 
        , insert_timestamp 
        from activity 
        where position_id is not NULL 
        and position_id > 0 
        and id not in (23, 24, 25) 
        and position_id in (select id from position_description) 
        """
    )
    db_conn.commit()
    cur.close()


def insert_activities(df_activities, db_conn, logger, delete_flag=False):
    """
    :param df_activities:
    :param db_conn:
    :param logger:
    :return:
    """
    cur = db_conn.cursor()
    # clear activity mapping and activity
    if delete_flag:
        cur.execute("delete from activity_job")
        cur.execute("delete from activity where position_id is not NULL and position_id > 0")
        cur.execute("delete from activity_candidate")
        cur.execute("delete from activity where candidate_id is not NULL and candidate_id > 0")
        cur.execute("delete from activity_company")
        cur.execute("delete from activity where company_id is not NULL and company_id > 0")
        cur.execute("delete from activity_contact")
        cur.execute("delete from activity where contact_id is not NULL and contact_id > 0")
        # commit the changes to the database
        db_conn.commit()

    df_activities['candidate_id'] = df_activities['candidate_id'].fillna(0)
    df_activities['candidate_id'] = df_activities['candidate_id'].astype(np.int64)
    df_activities['company_id'] = df_activities['company_id'].fillna(0)
    df_activities['company_id'] = df_activities['company_id'].astype(np.int64)
    df_activities['position_id'] = df_activities['position_id'].fillna(0)
    df_activities['position_id'] = df_activities['position_id'].astype(np.int64)
    df_activities['contact_id'] = df_activities['contact_id'].fillna(0)
    df_activities['contact_id'] = df_activities['contact_id'].astype(np.int64)
    df_activities['user_account_id'] = df_activities['user_account_id'].fillna(0)
    df_activities['user_account_id'] = df_activities['user_account_id'].astype(np.int64)

    logger.info('number of rows will be inserted: %i' % len(df_activities))
    logger.info("activity inserting...")
    page_size = 500  # TRUONG MENTION
    dfs = vincere_common.df_split_to_listofdfs(df_activities, page_size)
    for _idx, _df in enumerate(dfs):
        try:
            logger.info('inserting activity table: row %s to the next %s rows' % (_idx * page_size, len(_df)))
            list_values = []
            for index, row in _df.iterrows():
                a_record = list()
                a_record.append(row['company_id'] if row['company_id'] != 0 else None)  #
                a_record.append(row['contact_id'] if row['contact_id'] != 0 else None)  #
                a_record.append(row['candidate_id'] if row['candidate_id'] != 0 else None)  #
                a_record.append(row['position_id'] if row['position_id'] != 0 else None)  #
                a_record.append(row['user_account_id'] if row['user_account_id'] != 0 else -10)  #
                a_record.append(row['insert_timestamp'])  #
                a_record.append(row['content'])  #
                a_record.append(row['category'])  #
                list_values.append(tuple(a_record))

            sql = "INSERT INTO activity(company_id, contact_id, candidate_id, position_id, user_account_id, insert_timestamp, content, category) VALUES %s "
            psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=page_size)
            db_conn.commit()
        except Exception as ex:
            logger.error(ex)
            logger.info("Error batch data will be saved")
            _df.to_csv('error_activity_insert_batch_{0}.csv'.format(_idx), index=False, header=True, sep=',')
    cur.close()


@deprecated(reason="use vincere_activity.insert_activities_1 instead")
def insert_activities_1(df_act, db_conn, logger, delete_flag=False):
    if len(df_act):
        # add cols if not existed
        df_act['category'] = 'comment'
        if 'company_external_id' not in df_act.columns:
            df_act['company_external_id'] = None
        if 'contact_external_id' not in df_act.columns:
            df_act['contact_external_id'] = None
        if 'candidate_external_id' not in df_act.columns:
            df_act['candidate_external_id'] = None
        if 'position_external_id' not in df_act.columns:
            df_act['position_external_id'] = None
        # remove trailing and leading spaces from external ids
        df_act['company_external_id'] = df_act.apply(lambda x: str(x['company_external_id']).strip() if (x['company_external_id'] != None) and (str(x['company_external_id']) != 'nan') else None, axis=1)
        df_act['contact_external_id'] = df_act.apply(lambda x: str(x['contact_external_id']).strip() if (x['contact_external_id'] != None) and (str(x['contact_external_id']) != 'nan') else None, axis=1)
        df_act['candidate_external_id'] = df_act.apply(lambda x: str(x['candidate_external_id']).strip() if (x['candidate_external_id'] != None) and (str(x['candidate_external_id']) != 'nan') else None, axis=1)
        df_act['position_external_id'] = df_act.apply(lambda x: str(x['position_external_id']).strip() if (x['position_external_id'] != None) and (str(x['position_external_id']) != 'nan') else None, axis=1)

        df_vin_cont = pd.read_sql("select id as contact_id, external_id from contact where deleted_timestamp is null", db_conn)
        df_vin_cand = pd.read_sql("select id as candidate_id, external_id from candidate where deleted_timestamp is null", db_conn)
        df_vin_comp = pd.read_sql("select id as company_id, external_id from company where deleted_timestamp is null", db_conn)
        df_vin_posi = pd.read_sql("select id as position_id, external_id from position_description where deleted_timestamp is null", db_conn)
        df_owner = pd.read_sql("select id as user_account_id, email from user_account", db_conn)

        if len(df_vin_cont):
            df_vin_cont['external_id'] = df_vin_cont.apply(lambda x: str(x['external_id']).strip(), axis=1)
        if len(df_vin_cand):
            df_vin_cand['external_id'] = df_vin_cand.apply(lambda x: str(x['external_id']).strip(), axis=1)
        if len(df_vin_comp):
            df_vin_comp['external_id'] = df_vin_comp.apply(lambda x: str(x['external_id']).strip(), axis=1)
        if len(df_vin_posi):
            df_vin_posi['external_id'] = df_vin_posi.apply(lambda x: str(x['external_id']).strip(), axis=1)

        df_act = df_act.merge(df_vin_cont, left_on='contact_external_id', right_on='external_id', how='left')
        df_act = df_act.merge(df_vin_cand, left_on='candidate_external_id', right_on='external_id', how='left')
        df_act = df_act.merge(df_vin_comp, left_on='company_external_id', right_on='external_id', how='left')
        df_act = df_act.merge(df_vin_posi, left_on='position_external_id', right_on='external_id', how='left')
        df_act = df_act.merge(df_owner, left_on='owner', right_on='email', how='left')
        df_act['user_account_id'] = df_act['user_account_id'].map(lambda x: x if x else -10)
        df_act = df_act[df_act.contact_id.notnull() | df_act.candidate_id.notnull() | df_act.company_id.notnull() | df_act.position_id.notnull()]
        insert_activities(df_act, db_conn, logger, delete_flag)


def prepare_temp_activities(df_act, engine_conn, logger, delete_flag=False):
    """
    TODO: ham nay chay chua dung, can test lai truoc khi dung
    :param df_act:
    :param engine_conn:
    :param logger:
    :param delete_flag:
    :return:
    """
    if len(df_act):
        # add cols if not existed
        df_act['category'] = 'comment'
        if 'company_external_id' not in df_act.columns:
            df_act['company_external_id'] = None
        if 'contact_external_id' not in df_act.columns:
            df_act['contact_external_id'] = None
        if 'candidate_external_id' not in df_act.columns:
            df_act['candidate_external_id'] = None
        if 'position_external_id' not in df_act.columns:
            df_act['position_external_id'] = None
        # remove trailing and leading spaces from external ids
        # df_act['company_external_id'] = df_act.apply(lambda x: str(x['company_external_id']).strip() if (x['company_external_id'] != None) and (str(x['company_external_id']) != 'nan') else None, axis=1)
        # df_act['contact_external_id'] = df_act.apply(lambda x: str(x['contact_external_id']).strip() if (x['contact_external_id'] != None) and (str(x['contact_external_id']) != 'nan') else None, axis=1)
        # df_act['candidate_external_id'] = df_act.apply(lambda x: str(x['candidate_external_id']).strip() if (x['candidate_external_id'] != None) and (str(x['candidate_external_id']) != 'nan') else None, axis=1)
        # df_act['position_external_id'] = df_act.apply(lambda x: str(x['position_external_id']).strip() if (x['position_external_id'] != None) and (str(x['position_external_id']) != 'nan') else None, axis=1)

        df_vin_cont = pd.read_sql("select id as contact_id, external_id as contact_external_id from contact where deleted_timestamp is null", engine_conn)
        df_vin_cand = pd.read_sql("select id as candidate_id, external_id as candidate_external_id from candidate where deleted_timestamp is null", engine_conn)
        df_vin_comp = pd.read_sql("select id as company_id, external_id as company_external_id from company where deleted_timestamp is null", engine_conn)
        df_vin_posi = pd.read_sql("select id as position_id, external_id as position_external_id from position_description where deleted_timestamp is null", engine_conn)
        df_owner = pd.read_sql("select id as user_account_id, email from user_account", engine_conn)

        # if len(df_vin_cont):
        #     df_vin_cont['external_id'] = df_vin_cont.apply(lambda x: str(x['external_id']).strip(), axis=1)
        # if len(df_vin_cand):
        #     df_vin_cand['external_id'] = df_vin_cand.apply(lambda x: str(x['external_id']).strip(), axis=1)
        # if len(df_vin_comp):
        #     df_vin_comp['external_id'] = df_vin_comp.apply(lambda x: str(x['external_id']).strip(), axis=1)
        # if len(df_vin_posi):
        #     df_vin_posi['external_id'] = df_vin_posi.apply(lambda x: str(x['external_id']).strip(), axis=1)

        df_act = df_act.merge(df_vin_cont, on='contact_external_id', how='left')
        df_act = df_act.merge(df_vin_cand, on='candidate_external_id', how='left')
        df_act = df_act.merge(df_vin_comp, on='company_external_id', how='left')
        df_act = df_act.merge(df_vin_posi, on='position_external_id', how='left')
        df_act = df_act.merge(df_owner, left_on='owner', right_on='email', how='left')
        df_act['user_account_id'] = df_act['user_account_id'].map(lambda x: x if x else -10)
        df_act = df_act[df_act.contact_id.notnull() | df_act.candidate_id.notnull() | df_act.company_id.notnull() | df_act.position_id.notnull()]

        # df_act['candidate_id'] = df_act['candidate_id'].astype(np.int64)
        # df_act['company_id'] = df_act['company_id'].astype(np.int64)
        # df_act['position_id'] = df_act['position_id'].astype(np.int64)
        # df_act['contact_id'] = df_act['contact_id'].astype(np.int64)
        # df_act['user_account_id'] = df_act['user_account_id'].astype(np.int64)
        tblname = 'tung_temp_activity'
        vincere_common.append_to_db_2(engine_conn, df_act, tblname, 10000, vincere_common.sqlcol(df_act, is_postgre=True), logger, thr_num=100)
        return tblname


def clean_company(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from company_industry;
        delete from company_legal_document;
        delete from company;
        -- delete from company where note !='Sample data';
    """)
    db_conn.commit()
    cur.close()


def clean_contact(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from contact_comment;
        delete from contact_industry;
        delete from contact;
        --  delete from contact where id not in (select contact_id from contact_industry);
    """)
    db_conn.commit()
    cur.close()


def clean_candidate(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from candidate_work_history;
        delete from candidate;
    """)
    db_conn.commit()
    cur.close()


def clean_job(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from compensation;
        delete from position_agency_consultant; -- contains job owner info
        delete from position_description;
    """)
    db_conn.commit()
    cur.close()


def clean_job_application(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from position_candidate_feedback;
        delete from position_candidate;
        -- vacuum analyze verbose position_candidate;
        """)
    db_conn.commit()
    cur.close()


def clean_recent_record(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from recent_record;
        """)
    db_conn.commit()
    cur.close()


def clean_bulk_upload(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from bulk_upload;
        delete from bulk_upload_detail;
        delete from bulk_upload_document_mapping;
        """)
    db_conn.commit()
    cur.close()


def clean_candidate_gdpr_compliance(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from candidate_gdpr_compliance;
        """)
    db_conn.commit()
    cur.close()


def clean_unsupper_users(db_conn):
    cur = db_conn.cursor()
    cur.execute("""
        delete from user_account_location where user_account_id in (select id from user_account where super_user!=1);
        delete from user_account where super_user!=1;
        """)
    db_conn.commit()
    cur.close()


def clean_industry(db_conn):
    """
    check again

    :param db_conn:
    :return:
    """
    cur = db_conn.cursor()
    cur.execute("""
        update position_description set vertical_id=null;
        delete from company_industry;
        delete from contact_industry;
        delete from candidate_industry;
        delete from vertical_detail_language;
        delete from user_account_vertical;
        delete from vertical;
        """)
    db_conn.commit()
    cur.close()


def clean_functional_and_subfunctional_expertise(db_conn):
    """
    check again

    :param db_conn:
    :return:
    """
    cur = db_conn.cursor()
    cur.execute("""
        delete from candidate_functional_expertise;
        delete from contact_functional_expertise;
        delete from position_description_functional_expertise;
        delete from sub_functional_expertise;
        delete from functional_expertise;
        delete from team_group_functional_expertise;
        """)
    db_conn.commit()
    cur.close()


def insert_bulk_upload_document_mapping_4_position_description(df, db_conn):
    df_posdes = pd.read_sql("""select id as entity_id, external_id from position_description""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df = df.merge(df_posdes, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df.iterrows():
        a_record = list()
        a_record.append(row['file_name'])  # file_name
        a_record.append('POSITION')  # entity_type
        a_record.append('job_description')  # document_type
        a_record.append(row['entity_id'])  # entity_id
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """insert into bulk_upload_document_mapping (file_name, entity_type, document_type, entity_id) values %s """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()
    return df


def insert_bulk_upload_document_mapping_4_candidate(df, db_conn):
    df_candidate = pd.read_sql("""select id as entity_id, external_id from candidate""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df = df.merge(df_candidate, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df.iterrows():
        a_record = list()
        a_record.append(row['file_name'])  # file_name
        a_record.append('CANDIDATE')  # entity_type
        a_record.append('resume')  # document_type
        a_record.append(row['entity_id'])  # entity_id
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """insert into bulk_upload_document_mapping (file_name, entity_type, document_type, entity_id) values %s """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()
    return df

def insert_bulk_upload_document_mapping_4_candidate_photo(df, db_conn):
    df_candidate = pd.read_sql("""select id as entity_id, external_id from candidate""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df = df.merge(df_candidate, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df.iterrows():
        a_record = list()
        a_record.append(row['file_name'])  # file_name
        a_record.append('CANDIDATE')  # entity_type
        a_record.append('candidate_photo')  # document_type
        a_record.append(row['entity_id'])  # entity_id
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """insert into bulk_upload_document_mapping (file_name, entity_type, document_type, entity_id) values %s """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()
    return df


def insert_bulk_upload_document_mapping_4_candidate_other_docs(df, db_conn):
    df_candidate = pd.read_sql("""select id as entity_id, external_id from candidate""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df = df.merge(df_candidate, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df.iterrows():
        a_record = list()
        a_record.append(row['file_name'])  # file_name
        a_record.append('CANDIDATE')  # entity_type
        a_record.append('other_docs')  # document_type
        a_record.append(row['entity_id'])  # entity_id
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """insert into bulk_upload_document_mapping (file_name, entity_type, document_type, entity_id) values %s """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()


def insert_bulk_upload_document_mapping_4_company(df, db_conn):
    """

    :param df: have to contain atleast: file_name and external_id columns
    :param db_conn:
    :return:
    """
    df_company = pd.read_sql("""select id as entity_id, external_id from company""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df_temp = df.merge(df_company, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df_temp.iterrows():
        a_record = list()
        a_record.append(row['file_name'])  # file_name
        a_record.append('COMPANY')  # entity_type
        a_record.append('legal_document')  # document_type
        a_record.append(row['entity_id'])  # entity_id
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """insert into bulk_upload_document_mapping (file_name, entity_type, document_type, entity_id) values %s """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()
    return df_temp


def document_mapping_directly_for_company(df, conn_param, mylog):
    """
    this function inject data to company_legal_document and candidate_document table: this help to
    create mappings for physical files on S3 server.
    These physical files can be uploaded later to the production folder.
    :param df:
    :param conn_param:
    :param mylog:
    :return:
    """
    conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(conn_param.get('user'), conn_param.get('password'), conn_param.get('server'), conn_param.get('port'), conn_param.get('database'))
    sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
    ddbconn = sdbconn_engine.raw_connection()

    candidate_document = df[['file', 'alter_file2', 'insert_timestamp', 'entity_id', 'root']]
    candidate_document['uploaded_filename'] = candidate_document['file']
    candidate_document['saved_filename'] = candidate_document['alter_file2']
    candidate_document['insert_timestamp'] = candidate_document['insert_timestamp']
    candidate_document['company_id'] = candidate_document['entity_id']

    company_legal_document = candidate_document[['company_id']].dropna().drop_duplicates()
    company_legal_document['type'] = 1
    company_legal_document['title'] = 'Default'
    company_legal_document['insert_timestamp'] = datetime.datetime.now()
    new_legaldoc = company_legal_document.merge(pd.read_sql("select id as legal_doc_id, company_id from company_legal_document", ddbconn), on='company_id', how='left').query("legal_doc_id.isnull()")  # get legal doc id
    if len(new_legaldoc):  # insert new doc mapping for company
        load_data_to_vincere(new_legaldoc, conn_param, 'insert', 'company_legal_document', ['company_id', 'type', 'title', 'insert_timestamp'], [], mylog)
    company_legal_document = company_legal_document.merge(pd.read_sql("select id as legal_doc_id, company_id from company_legal_document", ddbconn), on='company_id', how='left')  # reload legal doc id

    tem2 = candidate_document[['uploaded_filename', 'saved_filename', 'insert_timestamp', 'company_id']]
    tem2 = tem2.merge(company_legal_document[['company_id', 'legal_doc_id']], on='company_id')
    tem2['document_type'] = 'legal_document'
    tem2['created'] = tem2['insert_timestamp']
    tem2['trigger_index_update_timestamp'] = tem2['insert_timestamp']
    tem2['version_no'] = 1
    tem2['successful_parsing_percent'] = 0
    tem2['primary_document'] = 0
    tem2['google_viewer'] = -1
    tem2['temporary'] = 0
    tem2['customer_portal'] = 0
    tem2['visible'] = 1
    if len(tem2):
        load_data_to_vincere(tem2, conn_param, 'insert', 'candidate_document',
                                                      ['legal_doc_id', 'uploaded_filename', 'saved_filename', 'insert_timestamp'
                                                          , 'document_type', 'created'
                                                          , 'trigger_index_update_timestamp', 'version_no', 'successful_parsing_percent'
                                                          , 'primary_document', 'google_viewer', 'temporary', 'customer_portal', 'visible'], [], mylog)
    ddbconn.close()
    sdbconn_engine.dispose()


def document_mapping_directly_for_job(df, conn_param, mylog):
    """
    this function inject data to company_legal_document and candidate_document table: this help to
    create mappings for physical files on S3 server.
    These physical files can be uploaded later to the production folder.
    :param df:
    :param conn_param:
    :param mylog:
    :return:
    """
    conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(conn_param.get('user'), conn_param.get('password'), conn_param.get('server'), conn_param.get('port'), conn_param.get('database'))
    sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
    ddbconn = sdbconn_engine.raw_connection()

    candidate_document = df[['file', 'alter_file2', 'insert_timestamp', 'entity_id', 'root']]
    candidate_document['uploaded_filename'] = candidate_document['file']
    candidate_document['saved_filename'] = candidate_document['alter_file2']
    candidate_document['insert_timestamp'] = candidate_document['insert_timestamp']
    candidate_document['position_description_id'] = candidate_document['entity_id']

    tem2 = candidate_document[['uploaded_filename', 'saved_filename', 'insert_timestamp', 'position_description_id']]
    tem2['document_type'] = 'job_description'
    tem2['created'] = tem2['insert_timestamp']
    tem2['trigger_index_update_timestamp'] = tem2['insert_timestamp']
    tem2['version_no'] = 1
    tem2['successful_parsing_percent'] = 0
    tem2['primary_document'] = 0
    tem2['google_viewer'] = -1
    tem2['temporary'] = 0
    tem2['customer_portal'] = 0
    tem2['visible'] = 1
    if len(tem2):
        load_data_to_vincere(tem2, conn_param, 'insert', 'candidate_document',
                                                      ['position_description_id', 'uploaded_filename', 'saved_filename', 'insert_timestamp'
                                                          , 'document_type', 'created'
                                                          , 'trigger_index_update_timestamp', 'version_no', 'successful_parsing_percent'
                                                          , 'primary_document', 'google_viewer', 'temporary', 'customer_portal', 'visible'], [], mylog)
    ddbconn.close()
    sdbconn_engine.dispose()


def document_mapping_directly_for_candidate(df, conn_param, mylog):
    """
    this function inject data to company_legal_document and candidate_document table: this help to
    create mappings for physical files on S3 server.
    These physical files can be uploaded later to the production folder.
    :param df:
    :param conn_param:
    :param mylog:
    :return:
    """
    conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(conn_param.get('user'), conn_param.get('password'), conn_param.get('server'), conn_param.get('port'), conn_param.get('database'))
    sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
    ddbconn = sdbconn_engine.raw_connection()

    candidate_document = df[['file', 'alter_file2', 'insert_timestamp', 'entity_id', 'root']]
    candidate_document['uploaded_filename'] = candidate_document['file']
    candidate_document['saved_filename'] = candidate_document['alter_file2']
    candidate_document['insert_timestamp'] = candidate_document['insert_timestamp']
    candidate_document['candidate_id'] = candidate_document['entity_id']

    tem2 = candidate_document[['uploaded_filename', 'saved_filename', 'insert_timestamp', 'candidate_id']]
    tem2['document_type'] = 'resume'
    tem2['created'] = tem2['insert_timestamp']
    tem2['trigger_index_update_timestamp'] = tem2['insert_timestamp']
    tem2['version_no'] = 1
    tem2['successful_parsing_percent'] = 0
    tem2['primary_document'] = 0
    tem2['google_viewer'] = -1
    tem2['temporary'] = 0
    tem2['customer_portal'] = 0
    tem2['visible'] = 1
    tem2['document_types_id'] = 1  # resume
    if len(tem2):
        load_data_to_vincere(tem2, conn_param, 'insert', 'candidate_document',
                                                      ['candidate_id', 'document_types_id', 'uploaded_filename', 'saved_filename', 'insert_timestamp'
                                                          , 'document_type', 'created'
                                                          , 'trigger_index_update_timestamp', 'version_no', 'successful_parsing_percent'
                                                          , 'primary_document', 'google_viewer', 'temporary', 'customer_portal', 'visible'], [], mylog)

    ddbconn.close()
    sdbconn_engine.dispose()


def document_mapping_directly_for_contact(df, conn_param, mylog):
    """
    this function inject data to company_legal_document and candidate_document table: this help to
    create mappings for physical files on S3 server.
    These physical files can be uploaded later to the production folder.
    :param df:
    :param conn_param:
    :param mylog:
    :return:
    """
    conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(conn_param.get('user'), conn_param.get('password'), conn_param.get('server'), conn_param.get('port'), conn_param.get('database'))
    sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
    ddbconn = sdbconn_engine.raw_connection()

    candidate_document = df[['file', 'alter_file2', 'insert_timestamp', 'entity_id', 'root']]
    candidate_document['uploaded_filename'] = candidate_document['file']
    candidate_document['saved_filename'] = candidate_document['alter_file2']
    candidate_document['insert_timestamp'] = candidate_document['insert_timestamp']
    candidate_document['contact_id'] = candidate_document['entity_id']

    tem2 = candidate_document[['uploaded_filename', 'saved_filename', 'insert_timestamp', 'contact_id']]
    tem2['document_type'] = 'document'
    tem2['created'] = tem2['insert_timestamp']
    tem2['trigger_index_update_timestamp'] = tem2['insert_timestamp']
    tem2['version_no'] = 1
    tem2['successful_parsing_percent'] = 0
    tem2['primary_document'] = 0
    tem2['google_viewer'] = -1
    tem2['temporary'] = 0
    tem2['customer_portal'] = 0
    tem2['visible'] = 1
    tem2['document_types_id'] = 3  # other docs
    if len(tem2):
        load_data_to_vincere(tem2, conn_param, 'insert', 'candidate_document',
                                                      ['contact_id', 'document_types_id', 'uploaded_filename', 'saved_filename', 'insert_timestamp'
                                                          , 'document_type', 'created'
                                                          , 'trigger_index_update_timestamp', 'version_no', 'successful_parsing_percent'
                                                          , 'primary_document', 'google_viewer', 'temporary', 'customer_portal', 'visible'], [], mylog)
    ddbconn.close()
    sdbconn_engine.dispose()


def insert_bulk_upload_document_mapping_4_contact(df, db_conn):
    df_contact = pd.read_sql("""select id as entity_id, external_id from contact""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df = df.merge(df_contact, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df.iterrows():
        a_record = list()
        a_record.append(row['file_name'])  # file_name
        a_record.append('CONTACT')  # entity_type
        a_record.append('documents')  # document_type
        a_record.append(row['entity_id'])  # entity_id
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """insert into bulk_upload_document_mapping (file_name, entity_type, document_type, entity_id) values %s """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()
    return df

def insert_bulk_upload_document_mapping_4_contact_photo(df, db_conn):
    df_contact = pd.read_sql("""select id as entity_id, external_id from contact""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df = df.merge(df_contact, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df.iterrows():
        a_record = list()
        a_record.append(row['file_name'])  # file_name
        a_record.append('CONTACT')  # entity_type
        a_record.append('contact_photo')  # document_type
        a_record.append(row['entity_id'])  # entity_id
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """insert into bulk_upload_document_mapping (file_name, entity_type, document_type, entity_id) values %s """
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()
    return df


def update_contact(df, db_conn):
    df_contact = pd.read_sql("""select id as contact_id, external_id from contact""", db_conn)
    df['external_id'] = df.apply(lambda x: str(x['external_id']).strip(), axis=1)
    df = df.merge(df_contact, left_on='external_id', right_on='external_id')
    list_values = []
    for index, row in df.iterrows():
        a_record = list()
        a_record.append(row['contact_id'])
        a_record.append(row['mobile_phone'])
        a_record.append(row['phone'])
        list_values.append(tuple(a_record))
    cur = db_conn.cursor()
    sql = """UPDATE contact SET mobile_phone = data.v1, phone=data.v2 FROM (VALUES %s) AS data (id, v1, v2)
         WHERE contact.id = data.id"""
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()


def update_contact_1(df, db_conn, up_cols, wh_cols):
    """

    :param df: source dataframe
    :param db_conn: destination database connection
    :param up_cols: updated columns name
    :param wh_cols: where columns name
    :return:
    """
    df_contact = pd.read_sql("""select id, external_id from contact""", db_conn)
    df = df.merge(df_contact, left_on='external_id', right_on='external_id')
    psycopg2_bulk_update(df, db_conn, up_cols, wh_cols, 'contact')


def psycopg2_bulk_update(df, db_conn, up_cols, wh_cols, tblname):
    """

     :param df: source dataframe
    :param db_conn: destination database connection
    :param up_cols: updated columns name
    :param wh_cols: where columns name
    :param tblname: table name
    :return:
    """
    list_values = []
    for index, row in df.iterrows():
        a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in (up_cols + wh_cols)]
        list_values.append(tuple(a_record))

    _1 = ', '.join(['{0}=data.{0}'.format(c) for c in up_cols])
    _2 = ' and '.join(['{1}.{0}=data.{0}'.format(c, tblname) for c in wh_cols])
    _3 = ', '.join(['{0}'.format(c) for c in (up_cols + wh_cols)])

    sql = """UPDATE {0} SET {1} FROM (VALUES %s) AS data ({3})
         WHERE {2} """.format(tblname, _1, _2, _3)
    cur = db_conn.cursor()
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()

import numpy
from psycopg2.extensions import register_adapter, AsIs
def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
register_adapter(numpy.float64, addapt_numpy_float64)
register_adapter(numpy.int64, addapt_numpy_int64)

def psycopg2_bulk_update_tracking(df, db_conn, up_cols, wh_cols, tblname, logger):
    """

     :param df: source dataframe
    :param db_conn: destination database connection
    :param up_cols: updated columns name
    :param wh_cols: where columns name
    :param tblname: table name
    :return:
    """
    offset = 0

    # edit 2019 05 31: replace nan, nat by None
    df = df.where(df.notnull(), None)
    # edit 2019 05 31: replace nan, nat by None

    dfs = vincere_common.df_split_to_listofdfs(df, 1000)
    for _idx, _df in enumerate(dfs):
        _inseted_to = (_idx + 1) * 1000 - (1000 - len(_df))
        logger.info("updating into {0}, row {1} to {2}".format(tblname, offset, _inseted_to))
        offset = _inseted_to

        list_values = []
        for index, row in _df.iterrows():
            a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in (up_cols + wh_cols)]
            list_values.append(tuple(a_record))

        _1 = ', '.join(['{0}=data.{0}'.format(c) for c in up_cols])
        _2 = ' and '.join(['{1}.{0}=data.{0}'.format(c, tblname) for c in wh_cols])
        _3 = ', '.join(['{0}'.format(c) for c in (up_cols + wh_cols)])

        sql = r"""UPDATE {0} SET {1} FROM (VALUES %s) AS data ({3})
             WHERE {2} """.format(tblname, _1, _2, _3)
        logger.info(sql)
        logger.info(list_values)
        with db_conn.cursor() as cur:
            psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
            db_conn.commit()


def writedb(df, dtype, table_name, connection_str, logger):
    """
    this function can auto retry to connect to server after 60 second sleep if the sever is not connected
    :param df:
    :param dtype:
    :param table_name:
    :param connection_str:
    :param logger:
    :return:
    """
    for attempt in range(10):
        try:
            logger.info("Writing to %s rows %s" % (table_name, len(df)))
            eng = sqlalchemy.create_engine(connection_str, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
            df.to_sql(con=eng, name=table_name, if_exists='replace', chunksize=len(df), dtype=dtype, index=False)
        except Exception as oe:
            print(oe)
            logger.warn("Sleep 60s before attempt to reinsert: {}".format(attempt))
            time.sleep(60)
        else:
            break
    else:  # fail after 10 times attemt
        raise Exception("Fail after 10 times attempting to write to database")
    eng.dispose()


def load_data_to_vincere(df, conn_param, type, tblname, up_cols, wh_cols, logger):
    """

    :param type: insert/update
    :return:
    """
    dfs = vincere_common.df_split_to_listofdfs(df, 1000)
    # load to vincere
    from common import thread_pool
    pool = thread_pool.ThreadPool(50)
    tbls = []
    error_tbls = []

    conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(conn_param.get('user'), conn_param.get('password'), conn_param.get('server'), conn_param.get('port'), conn_param.get('database'))

    sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
    current_activity_id = pd.read_sql("select max(id) as current_activity_id from activity", sdbconn_engine)
    for _, df in enumerate(dfs):
        df = df.where(df.notnull(), None)
        tbls.append('ztung_tem_{}_{}'.format(tblname, _))
        pool.add_task(writedb, df, vincere_common.sqlcol(df, is_postgre=True), 'ztung_tem_{}_{}'.format(tblname, _), conn_str_ddb, logger)
    pool.wait_completion()

    if type.lower() == 'update':
        conditions = ' and '.join(['{1}.{0}=data.{0}'.format(c, tblname) for c in wh_cols])
        _3 = ', '.join(['{0}'.format(c) for c in (up_cols + wh_cols)])
        _1 = ', '.join(['{0}=data.{0}'.format(c) for c in up_cols])

        sql = """
        UPDATE {tblname}
        SET {set_pairs}
        FROM (
             select {data_cols} from {temp_tblname}
           ) AS data ({data_cols})
        WHERE {conditions}
        ;
        """.format(tblname=tblname, conditions=conditions, data_cols=_3, set_pairs=_1, temp_tblname = '{}')
        logger.info(sql)

    if type.lower() == 'insert':
        # sql = """
        # INSERT INTO activity (company_id, contact_id, candidate_id, position_id, user_account_id, insert_timestamp, content, category)
        # select cast(company_id as int), cast(contact_id as int), cast(candidate_id as int), cast(position_id as int), cast(user_account_id as int), TO_TIMESTAMP(insert_timestamp, 'YYYY-MM-DD hh24:mi:ss'), content, category from {}
        #     """

        _1 = ', '.join(['{0}'.format(c) for c in up_cols])
        sql = "INSERT INTO {tblname} ({data_cols}) select {data_cols} from {temp_tblname} ".format(data_cols = _1, tblname = tblname, temp_tblname = '{}')

    for t in tbls:
        for attemp in range(0, 10):
            try:
                logger.info(t)
                sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
                connection = sdbconn_engine.raw_connection()
                execute_sql_update(sql.format(t), connection)
                # print("==================")
                # print(sql.format(t))
                connection.close()
                sdbconn_engine.dispose()
            except Exception as ex:
                logger.warn(ex)
                logger.warn("Will retry after 30 seconds")
                time.sleep(30)
                # sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
                # connection = sdbconn_engine.raw_connection()
                pass
            else:
                break
        else:  # fail after 10 times attempt
            error_tbls.append(t)
    assert 0 == len(error_tbls)
    # assert False

    # remove tem tables
    # for t in tbls:
    #     for attemp in range(0, 10):
    #         try:
    #             logger.info("Dropping table %s" % t)
    #             sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
    #             connection = sdbconn_engine.raw_connection()
    #             execute_sql_update('drop table {}'.format(t), connection)
    #         except Exception as ex:
    #             logger.warn(ex)
    #             logger.warn("Will retry after 30 seconds")
    #             time.sleep(30)
    #             pass
    #         else:
    #             break
    #     else:
    #         logger.warn("================= Cannot remove temporary table: %s =================" % t)

    remove_table_command = '; '.join(map(lambda t: 'drop table {}'.format(t), tbls))
    for attemp in range(0, 10):
        try:
            logger.info("Dropping table %s" % remove_table_command)
            sdbconn_engine = sqlalchemy.create_engine(conn_str_ddb, pool_size=200, max_overflow=300, client_encoding='utf8', use_batch_mode=True)
            connection = sdbconn_engine.raw_connection()
            execute_sql_update(remove_table_command, connection)
        except Exception as ex:
            logger.warn(ex)
            logger.warn("Will retry after 30 seconds")
            time.sleep(30)
            pass
        else:
            break
    else:
        logger.warn("================= Cannot remove temporary table: %s =================" % remove_table_command)


def psycopg2_bulk_delete(df, db_conn, wh_col, tblname, chunk_size=1000):
    start_index = 0
    end_index = chunk_size if chunk_size < len(df) else len(df)
    cur = db_conn.cursor()
    while start_index != end_index:
        tem = df.iloc[start_index:end_index, :]
        _2 = ','.join(["'%s'" % e for e in tem[wh_col]])
        sql = """delete FROM {0} WHERE {1} in ({2}) """.format(tblname, wh_col, _2, )
        print(sql)
        cur.execute(sql)
        db_conn.commit()
        # shift start and end index
        start_index = min(start_index + chunk_size, len(df))
        end_index = min(end_index + chunk_size, len(df))
    cur.close()


def psycopg2_bulk_insert(df, db_conn, up_cols, tblname):
    """

     :param df: source dataframe
    :param db_conn: destination database connection
    :param up_cols: updated columns name
    :param tblname: table name
    :return:
    """
    list_values = []
    for index, row in df.iterrows():
        a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in up_cols]
        list_values.append(tuple(a_record))
    _1 = ', '.join(['{0}'.format(c) for c in up_cols])
    sql = "INSERT INTO {1} ({0}) VALUES %s ".format(_1, tblname)
    cur = db_conn.cursor()
    psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
    db_conn.commit()
    cur.close()


def psycopg2_bulk_insert_tracking(df, db_conn, up_cols, tblname, logger=None):
    """

     :rtype:
     :param df: source dataframe
    :param db_conn: destination database connection
    :param up_cols: updated columns name
    :param tblname: table name
    :return:
    """
    offset = 0
    dfs = vincere_common.df_split_to_listofdfs(df, 1000)
    for _idx, _df in enumerate(dfs):
        if logger:
            _inseted_to = (_idx + 1) * 1000 - (1000 - len(_df))
            logger.info("inserting into {0}, row {1} to {2}".format(tblname, offset, _inseted_to))
            offset = _inseted_to
        list_values = []
        for index, row in _df.iterrows():
            a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in up_cols]
            list_values.append(tuple(a_record))
        _1 = ', '.join(['{0}'.format(c) for c in up_cols])
        sql = "INSERT INTO {1} ({0}) VALUES %s ".format(_1, tblname)
        cur = db_conn.cursor()
        psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
        db_conn.commit()
        cur.close()


def turbodbc_bulk_insert_mssql(df, db_conn, cols, tblname):
    list_values = []
    for index, row in df.iterrows():
        a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in cols]
        list_values.append(tuple(a_record))
    _1 = '], ['.join(['{0}'.format(c) for c in cols])
    _val_holder = ', '.join(['?' for c in cols])
    sql = "INSERT INTO {1} VALUES ({2}) ".format(_1, tblname, _val_holder)
    cur = db_conn.cursor()
    cur.executemany(sql, list_values)
    db_conn.commit()
    cur.close()


def pymssql_bulk_insert(df, db_conn, cols, tblname):
    list_values = []
    for index, row in df.iterrows():
        a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in cols]
        list_values.append(tuple(a_record))
    _1 = '], ['.join(['{0}'.format(c) for c in cols])
    _val_holder = ', '.join(['%s' for c in cols])
    sql = "INSERT INTO {1} VALUES ({2}) ".format(_1, tblname, _val_holder)
    cur = db_conn.cursor()
    cur.executemany(sql, list_values)
    db_conn.commit()
    cur.close()


def pymssql_bulk_insert2(df, db_conn, cols, tblname, log):
    dfs = vincere_common.df_split_to_listofdfs(df, 1000)
    for _i, df in enumerate(dfs):
        log.info("Writing to %s batch %s through %s" % (tblname, _i, len(df)))
        list_values = []
        for index, row in df.iterrows():
            a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in cols]
            # list_values.append("('{}')".format("','".join(str(x) for x in a_record)))
            list_values.append("({})".format(",".join("N'%s'" % str(x).strip().replace('\r\n', '\n').replace("'", "''") if x else 'NULL' for x in a_record)))

        _1 = '], ['.join(['{0}'.format(c) for c in cols])
        _val_holder = ', '.join(x for x in list_values)
        sql = "INSERT INTO {1} ([{0}]) VALUES {2} ".format(_1, tblname, _val_holder)
        # print(sql)
        cur = db_conn.cursor()
        cur.execute(sql)
        db_conn.commit()


def pymssql_bulk_insert3(df, engine, cols, tblname, log):
    dfs = vincere_common.df_split_to_listofdfs(df, 1000)
    for _i, df in enumerate(dfs):
        log.info("Writing to %s batch %s through %s" % (tblname, _i, len(df)))
        list_values = []
        for index, row in df.iterrows():
            a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in cols]
            # list_values.append("('{}')".format("','".join(str(x) for x in a_record)))
            list_values.append("({})".format(",".join("N'%s'" % str(x).strip().replace('\r\n', '\n').replace("'", "''") if x else 'NULL' for x in a_record)))

        _1 = '], ['.join(['{0}'.format(c) for c in cols])
        _val_holder = ', '.join(x for x in list_values)
        sql = "INSERT INTO {1} ([{0}]) VALUES {2} ".format(_1, tblname, _val_holder)
        # print(sql)
        engine.connect().execute(sql)


def sqlite_bulk_insert3(df, engine, cols, tblname, log):
    dfs = vincere_common.df_split_to_listofdfs(df, 1000)
    for _i, df in enumerate(dfs):
        log.info("Writing to %s batch %s through %s" % (tblname, _i, len(df)))
        list_values = []
        for index, row in df.iterrows():
            a_record = [row[c] if (str(row[c]) != 'nan') and (str(row[c]) != 'NaT') else None for c in cols]
            # list_values.append("('{}')".format("','".join(str(x) for x in a_record)))
            list_values.append("({})".format(",".join("'%s'" % str(x).strip().replace('\r\n', '\n').replace("'", "''") if x else 'NULL' for x in a_record)))

        _1 = '], ['.join(['{0}'.format(c) for c in cols])
        _val_holder = ', '.join(x for x in list_values)
        sql = "INSERT INTO {1} ([{0}]) VALUES {2} ".format(_1, tblname, _val_holder)
        # print(sql)
        engine.connect().execute(sql)


def update_candidate_country_specific(df, db_conn):
    df_can = pd.read_sql("""select id, external_id from candidate""", db_conn)
    df = df.merge(df_can, left_on='external_id', right_on='external_id')
    psycopg2_bulk_update(df, db_conn, ['country_specific', ], ['id', ], 'candidate')


def insert_candidate_gdpr_compliance(df, db_conn):
    df_can = pd.read_sql("""select id as candidate_id, external_id from candidate""", db_conn)
    df = df.merge(df_can, left_on='external_id', right_on='external_id')
    cols = ['candidate_id',
            'exercise_right',  # 3: Other [Person informed how to exercise their rights]
            'request_through',  # 6: Other
            'request_through_date',
            'obtained_through',  # 6: Other
            'obtained_through_date',
            'expire',  # 0: No
            'obtained_by',  # user_account.id
            'consent_level',  # 0 or null: Please select / 1: Full registration / 2: Job only / 3: Marketing [Consent level]
            'portal_status',  # 1:Consent given / 2:Pending [Consent to keep]
            'notes',  # [Notes | Journal]
            'insert_timestamp']
    psycopg2_bulk_insert(df, db_conn, cols, 'candidate_gdpr_compliance')


def remove_candidate_gdpr_compliance(ddbconn):
    with ddbconn.cursor() as cur:
        cur.execute("delete from candidate_gdpr_compliance;")
        ddbconn.commit()


def insert_contact_activities(df, ddbconn, cols):
    """(company_id, contact_id, candidate_id, position_id, user_account_id, insert_timestamp, content, category) """
    cur = ddbconn.cursor()
    df['type'] = 'contact'
    df['category'] = 'comment'
    df['content'] = ["ITNNT_CONTACT:%s" % x for x in df['content']]
    psycopg2_bulk_insert(df, ddbconn, cols, 'activity')
    cur.execute(
        """insert into activity_contact (activity_id, contact_id, insert_timestamp) 
        SELECT id 
        , contact_id 
        , insert_timestamp 
        from activity 
        where content like 'ITNNT_CONTACT:%';
        update activity set content = replace(content, 'ITNNT_CONTACT:', '') where content like 'ITNNT_CONTACT:%';
        """
    )
    ddbconn.commit()
    cur.close()


def insert_company_activities(df, ddbconn, cols):
    """(company_id, contact_id, candidate_id, position_id, user_account_id, insert_timestamp, content, category) """
    cur = ddbconn.cursor()
    df['type'] = 'company'
    df['category'] = 'comment'
    df['content'] = ["ITNNT_COMPANY:%s" % x for x in df['content']]
    psycopg2_bulk_insert(df, ddbconn, cols, 'activity')
    cur.execute(
        """insert into activity_company (activity_id, company_id, insert_timestamp) 
        SELECT id 
        , company_id 
        , insert_timestamp 
        from activity 
        where content like 'ITNNT_COMPANY:%';
        update activity set content = replace(content, 'ITNNT_COMPANY:', '') where content like 'ITNNT_COMPANY:%';
        """
    )
    ddbconn.commit()
    cur.close()


# def insert_user_account(df_comp_files):


def mapping_industries_to_team_brand(ddbconn, from_industry_id=0, team_group_id=None):
    """
    mapping all industries items for all teams (if team_group_id is None)
    :param ddbconn:
    :return:
    """
    cur = ddbconn.cursor()
    cur.execute("""
    insert into team_group_industry (team_group_id, industry_id, insert_timestamp)
    select *, now() as insert_timestamp 
    from (select id as team_group_id from team_group where 1=1 {0}) a,
         (select id as industry_id from vertical where id>{1}) b
    """.format(('and id=%s' % team_group_id) if team_group_id is not None else '', from_industry_id))
    ddbconn.commit()
    cur.close()


def mapping_jobtypes_to_team_brand(ddbconn, team_group_id=None):
    """
    mapping all industries items for all teams
    :param ddbconn:
    :return:
    """
    job_type = ["select '%s' as job_type" % e for e in ['FULL_TIME',
                                                        'PART_TIME',
                                                        'GRADUATE',
                                                        'CONTRACT',
                                                        'TEMP_TO_PERM', ]]

    cur = ddbconn.cursor()
    cur.execute("""
    insert into team_group_job_type (team_group_id, job_type, insert_timestamp)
    select *, now() as insert_timestamp 
    from (select id as team_group_id from team_group where 1=1 {}) a,
         ({}) b
    """.format(('and id=%s' % team_group_id) if team_group_id is not None else '', ' union '.join(job_type)))
    ddbconn.commit()
    cur.close()


def mapping_user_to_team_brand(ddbconn, team_group_id=None, user_emails=None):
    """
    :param ddbconn:
    :return:
    """
    cur = ddbconn.cursor()
    cur.execute("""
    insert into team_group_user (team_group_id, user_id, insert_timestamp)
    select *, now() as insert_timestamp 
    from (select id as team_group_id from team_group where 1=1 {}) a,
         (select id from user_account where email in ('sysadmin@vincere.io' {})) b
    """.format(
        ('and id=%s' % team_group_id) if team_group_id is not None else '',
        (", %s" % (", ".join("'%s'" % e for e in user_emails))) if user_emails is not None else ''
    )
    )
    ddbconn.commit()
    cur.close()


def mapping_functional_expertise_to_team_brand(ddbconn, team_group_id=None):
    cur = ddbconn.cursor()
    cur.execute("""
    insert into team_group_functional_expertise (team_group_id, functional_expertise_id, insert_timestamp)
    select *, now() as insert_timestamp 
    from (select id as team_group_id from team_group where 1=1 {}) a,
         (select id as functional_expertise_id from functional_expertise) b
    """.format(('and id=%s' % team_group_id) if team_group_id is not None else ''))
    ddbconn.commit()
    cur.close()


def load_location_by_postcost(df, col_id, col_add, col_postcode, result_filename, logger):
    """ generate location geo: due to the missing of company's city"""
    if True:
        if not path.exists(result_filename):  # file chua ton tai
            df_company_geolocator = df[[col_id, col_add, col_postcode]]

            df_company_geolocator = df_company_geolocator.assign(location_address=' ')
            df_company_geolocator = df_company_geolocator.assign(location_latitude=' ')
            df_company_geolocator = df_company_geolocator.assign(location_longitude=' ')

        else:
            df_company_geolocator = pd.read_csv(result_filename)
            if col_add in df_company_geolocator.columns:
                df_company_geolocator.drop(col_add, axis=1, inplace=True)  # drop / remove / delete column
            if col_postcode in df_company_geolocator.columns:
                df_company_geolocator.drop(col_postcode, axis=1, inplace=True)  # drop / remove / delete column
            df_company_geolocator = pd.merge(
                left=df_company_geolocator
                , right=df[[col_id, col_add, col_postcode]]
                , left_on=col_id
                , right_on=col_id
                , how='right'
            )
        df_company_geolocator = vincere_common.get_geolocator1(df_company_geolocator, result_filename, col_add, col_postcode, logger)


def load_location_by_postcost_googlemap(df, col_id, col_add, col_postcode, result_filename, logger):
    """ generate location geo: due to the missing of company's city"""
    if True:
        if not path.exists(result_filename):  # file chua ton tai
            df_company_geolocator = df[[col_id, col_add, col_postcode]]

            df_company_geolocator = df_company_geolocator.assign(location_address=' ')
            df_company_geolocator = df_company_geolocator.assign(location_latitude=' ')
            df_company_geolocator = df_company_geolocator.assign(location_longitude=' ')

        else:
            df_company_geolocator = pd.read_csv(result_filename)
            if col_add in df_company_geolocator.columns:
                df_company_geolocator.drop(col_add, axis=1, inplace=True)  # drop / remove / delete column
            if col_postcode in df_company_geolocator.columns:
                df_company_geolocator.drop(col_postcode, axis=1, inplace=True)  # drop / remove / delete column
            df_company_geolocator = pd.merge(
                left=df_company_geolocator
                , right=df[[col_id, col_add, col_postcode]]
                , left_on=col_id
                , right_on=col_id
                , how='right'
            )
        gmaps = googlemaps.Client(key="")
        # Geocoding an address
        for idx, r in df.iterrows():
            try:
                geocode_result = gmaps.geocode(r[col_add])
                df.loc[idx, 'location_address'] = geocode_result[0]['formatted_address']
                df.loc[idx, 'location_latitude'] = geocode_result[0]['geometry']['location']['lat']
                df.loc[idx, 'location_longitude'] = geocode_result[0]['geometry']['location']['lng']
                df.to_csv(result_filename, index=False, header=True, sep=",")
            except Exception as ex:
                logger.error('can not parse for: %s' % r[col_add])
                logger.error(ex)


def clean_custom_drop_down_list_values_field(field_key, ddbconn):
    remove = """
    delete from additional_form_values 
    where field_id in (select id from configurable_form_field where  field_key ='%s')
    and form_id in (select form_id from configurable_form_field where  field_key ='%s')
    """ % (field_key, field_key)
    cur = ddbconn.cursor()
    cur.execute(remove)
    ddbconn.commit()
    cur.close()
    remove = """
    delete from configurable_form_language 
    where language_code in (
    select title_language_code from configurable_form_field_value 
    where field_id in (select id from configurable_form_field where  field_key ='%s')
    and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
    )""" % (field_key, field_key)
    cur = ddbconn.cursor()
    cur.execute(remove)
    ddbconn.commit()
    cur.close()
    remove = """
    delete from configurable_form_field_value 
    where field_id in (select id from configurable_form_field where  field_key ='%s')
    and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
    """ % (field_key, field_key)
    cur = ddbconn.cursor()
    cur.execute(remove)
    ddbconn.commit()
    cur.close()


def insert_drop_down_list_values(drpvals, field_key, ddbconn):
    """
    :param drpvals:
    :param field_key:
    :param ddbconn:
    :return:
    """
    # this field is already created on gui
    df_vincere_form_field = pd.read_sql("""
    select a.id as field_id, a.form_id , a.field_key, 'en' as language, b.type
    from configurable_form_field a
    join configurable_form b on a.form_id=b.id
    where field_key ='%s'
    """ % field_key, ddbconn)
    df_vincere_form_field = df_vincere_form_field.assign(insert_timestamp=datetime.datetime.now())
    df = pd.DataFrame({'translate': drpvals})
    df['field_key'] = field_key
    df = df.drop_duplicates()
    df['field_value'] = df.reset_index().index + 1  # add row number column
    df = df.merge(df_vincere_form_field, left_on='field_key', right_on='field_key', how='inner')
    # insert this table so that title_language_code is auto generated
    psycopg2_bulk_insert(df, ddbconn, ['field_id', 'form_id', 'insert_timestamp', 'field_value'], 'configurable_form_field_value')

    # get auto generated title_language_code
    df_configurable_form_field_value = pd.read_sql("""select title_language_code as language_code, field_id, form_id, insert_timestamp, field_value from configurable_form_field_value """, ddbconn)
    df_configurable_form_field_value['field_value'] = df_configurable_form_field_value['field_value'].astype(np.int64)
    df = df.merge(df_configurable_form_field_value, left_on=['field_id', 'form_id', 'insert_timestamp', 'field_value'], right_on=['field_id', 'form_id', 'insert_timestamp', 'field_value'])
    psycopg2_bulk_insert(df, ddbconn, ['language_code', 'language', 'translate'], 'configurable_form_language')


def insert_candidate_drop_down_list_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_cand_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.form_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def insert_candidate_muti_selection_checkbox(df, candidate_extid, values_colname, field_key, ddbconn, logger=None):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_cand_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

    temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
    df = pd.DataFrame(temp_series).reset_index()  #

    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert_tracking(df, ddbconn, cols, 'additional_form_values', logger)
    return df


def append_candidate_muti_selection_checkbox(df, candidate_extid, values_colname, field_key, ddbconn, logger=None):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    additional_form_values = pd.read_sql("""
        select * from additional_form_values 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key), ddbconn)

    configurable_form_language = pd.read_sql("""
        select * from configurable_form_language 
        where language_code in (
        select title_language_code from configurable_form_field_value 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
        )""" % (field_key, field_key), ddbconn)

    configurable_form_field_value = pd.read_sql("""
        select * from configurable_form_field_value 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
        """ % (field_key, field_key), ddbconn)

    df = df[df[values_colname].notnull()]

    test = df[[values_colname]].drop_duplicates()
    test = test.merge(configurable_form_language, left_on=values_colname, right_on='translate', how='outer', indicator=True)
    test = test.query("_merge == 'left_only'")

    drpd_vals = test[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)

    df = df.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_cand_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

    temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
    df = pd.DataFrame(temp_series).reset_index()  #

    df['constraint_id'] = df['constraint_id'].astype(str)
    test2 = df.merge(additional_form_values, on=['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'], how='left', suffixes=('', '_y'))
    test2 = test2.loc[test2['insert_timestamp'].isnull()]

    test2['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert_tracking(test2, ddbconn, cols, 'additional_form_values', logger)


def insert_job_muti_selection_checkbox(df, candidate_extid, values_colname, field_key, ddbconn, logger=None):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from position_description", ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_job_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

    temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
    df = pd.DataFrame(temp_series).reset_index()  #

    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert_tracking(df, ddbconn, cols, 'additional_form_values', logger)


# def insert_candidate_muti_selection_checkbox2(df, candidate_extid, values_colname, field_key, ddbconn):
#     """
#     :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
#     :param candidate_extid: column name of the candidate external id field
#     :param values_colname: column name of the drop down list values field
#     :param field_key: field key values on the GUI
#     :param ddbconn: destination database connection
#     :return:
#     """
#     clean_custom_drop_down_list_values_field(field_key, ddbconn)
#     df = df[df[values_colname].notnull()]
#     drpd_vals = df[values_colname].unique()
#     drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
#     drpd_vals.sort()
#     insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
#     df = df.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn), left_on=candidate_extid, right_on='external_id')
#     sql = """
#         select a.translate, 'add_cand_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
#         from configurable_form_language a
#         join configurable_form_field_value b on a.language_code=b.title_language_code
#         where field_id in (select id from configurable_form_field where  field_key ='%s')
#         and form_id in (select form_id from configurable_form_field where  field_key ='%s')
#         """ % (field_key, field_key)
#     df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
#     # insert to: additional_form_values
#     cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']
#
#     temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
#     df = pd.DataFrame(temp_series).reset_index()  #
#
#     df['insert_timestamp'] = datetime.datetime.now()
#     # text buffer
#     df = df[cols]
#     s_buf = io.StringIO()
#     # saving a data frame to a buffer (same as with a regular file):
#     df.to_csv(s_buf, index=False, sep='\t', header=False)
#     # In order to read from the buffer afterwards, its position should be set to the beginning:
#     s_buf.seek(0)
#     curs = ddbconn.cursor()
#     curs.copy_from(s_buf, 'additional_form_values', sep='\t', columns=cols)


def insert_contact_drop_down_list_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from contact", ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_con_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.form_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def insert_contact_muti_selection_checkbox(df, cont_extid, values_colname, field_key, ddbconn, logger=None):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param cont_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from contact", ddbconn), left_on=cont_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_con_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

    temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
    df = pd.DataFrame(temp_series).reset_index()  #

    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert_tracking(df, ddbconn, cols, 'additional_form_values', logger)
    return df


@deprecated(version='1', reason="You should use vincere_multi_selection_checkbox.append_muti_selection_checkbox")
def append_muti_selection_checkbox(df, candidate_extid, values_colname, field_key, ddbconn, entity_type, logger=None):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    if entity_type not in ('company', 'contact', 'candidate', 'job'):
        raise Exception("entity_type not in ('company', 'contact', 'candidate', 'job')")
    else:
        if entity_type == 'company':
            additional_type = 'add_com_info'
            entbl = 'company'
        if entity_type == 'contact':
            additional_type = 'add_con_info'
            entbl = 'contact'
        if entity_type == 'candidate':
            additional_type = 'add_cand_info'
            entbl = 'candidate'
        if entity_type == 'job':
            additional_type = 'add_job_info'
            entbl = 'position_description'

    additional_form_values = pd.read_sql("""
        select * from additional_form_values 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key), ddbconn)

    configurable_form_language = pd.read_sql("""
        select * from configurable_form_language 
        where language_code in (
        select title_language_code from configurable_form_field_value 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
        )""" % (field_key, field_key), ddbconn)

    configurable_form_field_value = pd.read_sql("""
        select * from configurable_form_field_value 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
        """ % (field_key, field_key), ddbconn)

    df = df[df[values_colname].notnull()]

    test = df[[values_colname]].drop_duplicates()
    test = test.merge(configurable_form_language, left_on=values_colname, right_on='translate', how='outer', indicator=True)
    test = test.query("_merge == 'left_only'")

    drpd_vals = test[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)

    df = df.merge(pd.read_sql("select id as additional_id, external_id from {}".format(entbl), ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, '%s' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (additional_type, field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

    temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
    df = pd.DataFrame(temp_series).reset_index()  #

    if len(df):
        df['constraint_id'] = df['constraint_id'].astype(str)
        test2 = df.merge(additional_form_values, on=['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'], how='left', suffixes=('', '_y'))
        test2 = test2.loc[test2['insert_timestamp'].isnull()]

        test2['insert_timestamp'] = datetime.datetime.now()
        psycopg2_bulk_insert_tracking(test2, ddbconn, cols, 'additional_form_values', logger)
        return test2


def insert_company_drop_down_list_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from company", ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_com_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.form_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def insert_company_muti_selection_checkbox(df, cont_extid, values_colname, field_key, ddbconn, logger=None):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param cont_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from company", ddbconn), left_on=cont_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_com_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

    temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
    df = pd.DataFrame(temp_series).reset_index()  #

    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert_tracking(df, ddbconn, cols, 'additional_form_values', logger)


def insert_job_drop_down_list_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    drpd_vals = df[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    insert_drop_down_list_values(drpd_vals, field_key, ddbconn)
    df = df.merge(pd.read_sql("select id as additional_id, external_id from position_description", ddbconn), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, 'add_job_info' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.form_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def insert_contact_text_field_values(df, contact_extid, values_colname, field_key, ddbconn):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param contact_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from contact", ddbconn), left_on=contact_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select 'add_con_info' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % field_key
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['insert_timestamp'] = datetime.datetime.now()
    df['field_value'] = df[values_colname]
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def insert_candidate_text_field_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn), left_on=candidate_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select 'add_cand_info' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % field_key
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['insert_timestamp'] = datetime.datetime.now()
    df['field_value'] = df[values_colname]
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def append_text_field_values(df, candidate_extid, values_colname, field_key, ddbconn, entity_type, logger):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    if entity_type not in ('company', 'contact', 'candidate', 'job'):
        raise Exception("entity_type not in ('company', 'contact', 'candidate', 'job')")
    else:
        if entity_type == 'company':
            additional_type = 'add_com_info'
            entbl = 'company'
        if entity_type == 'contact':
            additional_type = 'add_con_info'
            entbl = 'contact'
        if entity_type == 'candidate':
            additional_type = 'add_cand_info'
            entbl = 'candidate'
        if entity_type == 'job':
            additional_type = 'add_job_info'
            entbl = 'position_description'

    additional_form_values = pd.read_sql("""
        select * from additional_form_values 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key), ddbconn)

    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from {}".format(entbl), ddbconn), left_on=candidate_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select '%s' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % (additional_type, field_key)
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['field_value'] = df[values_colname]

    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']

    df['constraint_id'] = df['constraint_id'].astype(str)
    test2 = df.merge(additional_form_values, on=['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'], how='left', suffixes=('', '_y'))
    test2 = test2.loc[test2['insert_timestamp'].isnull()]
    test2['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert_tracking(test2, ddbconn, cols, 'additional_form_values', logger)
    return test2


def insert_job_text_field_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    append_job_text_field_values(df, candidate_extid, values_colname, field_key, ddbconn)

def append_job_text_field_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from position_description", ddbconn), left_on=candidate_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select 'add_job_info' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % field_key
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['insert_timestamp'] = vincere_common.my_insert_timestamp
    df['field_value'] = df[values_colname]
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def insert_candidate_date_field_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn), left_on=candidate_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select 'add_cand_info' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % field_key
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['insert_timestamp'] = datetime.datetime.now()
    df['field_date_value'] = df[values_colname]
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_date_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def append_date_field_values(df, candidate_extid, values_colname, field_key, ddbconn, entity_type, logger):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    if entity_type not in ('company', 'contact', 'candidate', 'job'):
        raise Exception("entity_type not in ('company', 'contact', 'candidate', 'job')")
    else:
        if entity_type == 'company':
            additional_type = 'add_com_info'
            entbl = 'company'
        if entity_type == 'contact':
            additional_type = 'add_con_info'
            entbl = 'contact'
        if entity_type == 'candidate':
            additional_type = 'add_cand_info'
            entbl = 'candidate'
        if entity_type == 'job':
            additional_type = 'add_job_info'
            entbl = 'position_description'

    additional_form_values = pd.read_sql("""
        select * from additional_form_values 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key), ddbconn)

    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from {}".format(entbl), ddbconn), left_on=candidate_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select 'add_cand_info' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % field_key
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['field_date_value'] = df[values_colname]

    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_date_value', 'insert_timestamp']

    df['constraint_id'] = df['constraint_id'].astype(str)
    test2 = df.merge(additional_form_values, on=['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'], how='left', suffixes=('', '_y'))
    test2 = test2.loc[test2['insert_timestamp'].isnull()]
    test2['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert_tracking(test2, ddbconn, cols, 'additional_form_values', logger)
    return test2


def insert_job_date_field_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from position_description", ddbconn), left_on=candidate_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select 'add_job_info' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % field_key
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['insert_timestamp'] = datetime.datetime.now()
    df['field_date_value'] = df[values_colname]
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_date_value', 'insert_timestamp']
    df['insert_timestamp'] = datetime.datetime.now()
    psycopg2_bulk_insert(df, ddbconn, cols, 'additional_form_values')


def generate_candidate_text_field_values(df, candidate_extid, values_colname, field_key, ddbconn):
    """
    insert value for custom field: text box, multi value text box
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :return:
    """
    df = df[df[values_colname].notnull()]
    df = df.merge(pd.read_sql("select id as additional_id, external_id from candidate", ddbconn), left_on=candidate_extid, right_on='external_id')
    df['field_key'] = field_key
    sql = """
        select 'add_cand_info' as additional_type, form_id, id as field_id, constraint_id, field_key
        from configurable_form_field where field_key ='%s'
        """ % field_key
    df = df.merge(pd.read_sql(sql, ddbconn), left_on='field_key', right_on='field_key')
    df['insert_timestamp'] = datetime.datetime.now()
    df['field_value'] = df[values_colname]
    # insert to: additional_form_values
    df['insert_timestamp'] = datetime.datetime.now()
    df = df[['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp']]
    # clean_custom_drop_down_list_values_field(field_key, ddbconn)
    df.to_csv('__additional_form_values.csv', index=False, header=True, sep=',')


def insert_candidate_source(src_names, ddbconn):
    """
    candidate source: PLEASE NOT THAT [Auto Parsed] CAN NOT BE REMOVED
    :param src_names:
    :param ddbconn:
    :return:
    """
    if len(src_names) == 0:
        return
    df = pd.DataFrame({'name': src_names})
    locid = pd.read_sql("select id from location where name='All'", ddbconn).loc[0, 'id']

    # ignore warning:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df['source_type'] = 1
        df['insert_timestamp'] = datetime.datetime.now()
        df['contract_margin_style'] = 0
        df['permanent_percentage'] = 0
        df['contract_percentage'] = 0
        df['internal'] = 0
        df['payment_style'] = 0
        df['periodic_payment_start_date'] = datetime.datetime.now()
        df['periodic_payment_end_date'] = datetime.datetime.now() + relativedelta(years=+50)
        df['included_job_count'] = 1
        df['percentage_option'] = 0
        df['percentage_option_plus'] = 0
        df['contract_percentage_option'] = 0
        df['candidate_method'] = 0
        df['show_job'] = 1
        df['show_careersite'] = 1
        df['location_id'] = locid
    cols = [
        'name',
        'source_type',
        'insert_timestamp',
        'contract_margin_style',
        'permanent_percentage',
        'contract_percentage',
        'internal',
        'payment_style',
        'periodic_payment_start_date',
        'periodic_payment_end_date',
        'included_job_count',
        'percentage_option',
        'percentage_option_plus',
        'contract_percentage_option',
        'candidate_method',
        'show_job',
        'show_careersite',
        'location_id',
    ]
    psycopg2_bulk_insert(df, ddbconn, cols, 'candidate_source')


def get_files_from_dbms_save_to_physical_files(sql, dbconn, destfolder, chunk_size=100):
    """
    read files stored in database and save it to physical file
    :param sql: string of sql command, have to return atleast 2 columns: file_name and file_data
    :param dbconn: connection to the database
    :param chunk_size: int, default 100
    :return:
    """
    pathlib.Path(destfolder).mkdir(parents=True, exist_ok=True)  # create folder if not exist
    offset = 0
    while True:
        temp_sql = sql.format(offset, chunk_size)
        #
        # extract documents to physical files
        df_company_docs = pd.read_sql(temp_sql, dbconn)
        #
        # write to physical files
        for index, row in df_company_docs.iterrows():
            vincere_common.write_file(row['file_data'], '%s/%s' % (destfolder, row['file_name']))
        offset += chunk_size
        if len(df_company_docs) < chunk_size:
            break


def run_after_standard_upload(default_currency, destination_db_connection):
    """
    this method should be run after each standard migration
    :param default_currency:
    :param destination_db_connection:
    :return:
    """
    execute_sql_update("update contact set insert_timestamp='1989-03-12 00:00:00' where first_name like '%DEFAULT%'", destination_db_connection)

    #
    # load offers
    offer = pd.read_sql("""
    select
    currency_type
, position_type
, employment_type
, working_hour_per_day, working_day_per_week, working_hour_per_week, working_day_per_month, working_week_per_month
, position_candidate_id, id
     from offer;
    """, destination_db_connection)

    #
    # load position_candidates have offers
    position_candidate = pd.read_sql("""
    select
    currency_type
    , status
    , candidate_id
    , offer_date
    , placed_date
    , hire_date
    , work_start_date
    , position_description_id, id
    from position_candidate where id in (select position_candidate_id from offer);
    """, destination_db_connection)

    position_candidate['work_start_date'] = position_candidate.apply(lambda x: x['offer_date'] + relativedelta(months=+1), axis=1)

    psycopg2_bulk_update(position_candidate, destination_db_connection, ['work_start_date', ], ['id', ], 'position_candidate')

    #
    # load position_descriptions have offers
    position_description = pd.read_sql("""
    select 
        currency_type, id
        , company_id, contact_id 
    from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
    """, destination_db_connection)

    #
    # load candidates have offers
    candidate = pd.read_sql("""
    select 
    male, phone, current_location_id, gender_title, email as cand_email, id 
    from candidate where id in (select candidate_id from position_candidate where id in (select position_candidate_id from offer))
    """, destination_db_connection)

    #
    # load companies have offers
    company = pd.read_sql("""
    select 
    id, name as comp_name, user_account_id
    from company where id in (
    select company_id from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
    )
    """, destination_db_connection)

    #
    # load companies' locations have offers
    company_location = pd.read_sql("""
    select 
    min(id) as client_billing_location_id, company_id 
    from company_location 
    where company_id in (
        select company_id from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
    ) group by company_id
    """, destination_db_connection)

    #
    # load contacts have offers
    contact = pd.read_sql("""
        select 
        id, first_name, middle_name, last_name, email as cont_email, phone as cont_phone
        from contact where id in (
        select contact_id from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
        )
        """, destination_db_connection)
    contact.fillna('', inplace=True)
    contact['cont_name'] = contact.apply(lambda x: ' '.join([x.first_name, x.middle_name, x.last_name]).replace('  ', ' '), axis=1)

    #
    # set default currency for job
    updf = position_description[position_description['currency_type'].isnull()]
    updf['currency_type'].fillna(default_currency, inplace=True)

    psycopg2_bulk_update(updf, destination_db_connection, ['currency_type', ], ['id', ], 'position_description')
    position_description['currency_type'].fillna(default_currency, inplace=True)

    #
    # set default currency for offer based on job's currency
    position_candidate.rename(columns={'id': 'position_candidate_id'}, inplace=True)
    offer = offer.merge(position_candidate, on='position_candidate_id')
    position_description.rename(columns={'id': 'position_description_id'}, inplace=True)
    offer = offer.merge(position_description, on='position_description_id')
    candidate.rename(columns={'id': 'candidate_id'}, inplace=True)
    offer = offer.merge(candidate, on='candidate_id')
    company.rename(columns={'id': 'company_id'}, inplace=True)
    offer = offer.merge(company, on='company_id')
    offer = offer.merge(company_location, on='company_id', how='left')
    contact.rename(columns={'id': 'contact_id'}, inplace=True)
    offer = offer.merge(contact, on='contact_id')

    # id_x: offer id - needed for bulk update
    # offer['id'] = offer['id_x']
    # set default working_hour_per_day

    offer['working_hour_per_day'].fillna(8, inplace=True)
    # set default working_day_per_week
    offer['working_day_per_week'].fillna(5, inplace=True)
    # set default working_hour_per_week
    offer['working_hour_per_week'].fillna(8 * 5, inplace=True)
    # set default working_day_per_month
    offer['working_day_per_month'].fillna(22, inplace=True)
    # set default working_week_per_month
    offer['working_week_per_month'].fillna(4, inplace=True)
    psycopg2_bulk_update(offer, destination_db_connection, ['currency_type', 'working_hour_per_day', 'working_day_per_week', 'working_hour_per_week', 'working_day_per_month', 'working_week_per_month'], ['id', ], 'offer')

    #
    # generate default offer_personal_info for each offer
    offer_personal_info = pd.DataFrame()
    offer_personal_info['offer_id'] = offer['id']
    offer_personal_info['gender_title'] = offer['gender_title']
    offer_personal_info['last_name'] = offer['last_name'] # contact last_name: WRONG SHOULD BE COME FROM CANDIDATE
    offer_personal_info['first_name'] = offer['first_name'] # contact first_name: WRONG SHOULD BE COME FROM CANDIDATE
    offer_personal_info['middle_name'] = offer['middle_name'] # contact middle_name: WRONG SHOULD BE COME FROM CANDIDATE
    offer_personal_info['male'] = offer['male']
    offer_personal_info['phone'] = offer['phone']
    offer_personal_info['email'] = offer['cand_email']
    offer_personal_info['offer_date'] = offer['offer_date']
    offer_personal_info['placed_date'] = offer['offer_date']
    # offer_personal_info['placed_date'] = offer['placed_date']
    offer_personal_info['start_date'] = offer['work_start_date']
    offer_personal_info['current_location_id'] = offer['current_location_id']
    offer_personal_info['client_company_id'] = offer['company_id']
    offer_personal_info['client_company_name'] = offer['comp_name']
    offer_personal_info['client_contact_id'] = offer['contact_id']
    offer_personal_info['client_contact_name'] = offer['cont_name']
    offer_personal_info['client_contact_email'] = offer['cont_email']
    offer_personal_info['client_contact_phone'] = offer['cont_phone']
    offer_personal_info['client_tax_exempt'] = 0
    offer_personal_info['terms'] = 0
    offer_personal_info['tax_rate'] = 0
    offer_personal_info['net_total'] = 0
    offer_personal_info['other_invoice_items_total'] = 0
    offer_personal_info['invoice_total'] = 0
    offer_personal_info['use_profit'] = 1
    offer_personal_info['offer_letter_signatory_user_id'] = offer['user_account_id']
    offer_personal_info['export_data_to'] = 'other'
    offer_personal_info['client_billing_location_id'] = offer['client_billing_location_id']

    # check existed offer_personal_info
    existed_offper_info = pd.read_sql("select offer_id from offer_personal_info", destination_db_connection)
    offer_personal_info = offer_personal_info.loc[~offer_personal_info['offer_id'].isin(existed_offper_info['offer_id'])]

    psycopg2_bulk_insert(offer_personal_info, destination_db_connection, offer_personal_info.columns, 'offer_personal_info')

    cur = destination_db_connection.cursor()
    cur.execute("update company_location set location_name=address where location_name is null and address is not null;")
    destination_db_connection.commit()
    cur.execute("update common_location set location_name=address where location_name is null and address is not null;")
    destination_db_connection.commit()
    cur.execute("update user_account set timezone=(select timezone from user_account where id=-10) where timezone is null;")
    destination_db_connection.commit()
    cur.execute(r"update company set note=replace(note, '\n', chr(10)) where note is not null;")
    destination_db_connection.commit()

    # update offer.position_type by position_description.position_type
    cur.execute(r"""
    update offer set position_type = data.val
    from (select o.id, pd.position_type from offer o
    join position_candidate pc on o.position_candidate_id = pc.id
    join position_description pd on pc.position_description_id = pd.id
    ) as data (id, val)
    where offer.id = data.id;
    """)
    destination_db_connection.commit()
    cur.close()

    # reupdate candidate info
    opi = pd.read_sql("""
    select
        opi.id,
        c.gender_title,
        c.first_name,
        c.last_name,
        c.middle_name,
        c.male,
        c.phone,
        c.home_phone,
        c.email,
        c.address1,
        c.city,
        c.zipcode,
        c.country,
        c.state,
        c.date_of_birth,
        c.nickname as preferred_name,
        c.current_location_id
    from offer_personal_info opi
    join offer o on opi.offer_id = o.id
    join position_candidate pc on o.position_candidate_id = pc.id
    join candidate c on pc.candidate_id = c.id
    --where opi.first_name like '%DEFAULT%'
    """, destination_db_connection)
    # vincere_custom_migration.execute_sql_update("select * into offer_personal_info_bk20190529 from offer_personal_info", ddbconn)
    psycopg2_bulk_update(opi, destination_db_connection, ['gender_title', 'first_name', 'last_name', 'middle_name', 'male', 'phone', 'home_phone', 'email', 'address1', 'city', 'zipcode', 'country', 'state', 'preferred_name', 'current_location_id'], ['id'], 'offer_personal_info')
    tem2 = opi[['id', 'date_of_birth']].dropna()
    tem2['date_of_birth'] = pd.to_datetime(tem2['date_of_birth'])
    psycopg2_bulk_update(tem2, destination_db_connection, ['date_of_birth'], ['id'], 'offer_personal_info')


def execute_sql_update(sql, destination_db_connection):
    """
    :rtype: object
    """
    cur = destination_db_connection.cursor()
    cur.execute(sql)
    destination_db_connection.commit()
    cur.close()


def execute_table_statistics(destination_db_connection):
    """
    """
    tablenames = pd.read_sql("""
            SELECT relname
          FROM
        (SELECT
        --N.nspname,
        C.relname,
        /*pg_stat_get_tuples_inserted(C.oid) AS n_tup_ins,
        pg_stat_get_tuples_updated(C.oid) AS n_tup_upd,
        pg_stat_get_tuples_deleted(C.oid) AS n_tup_del,
        pg_stat_get_live_tuples(C.oid) AS n_live_tup,*/
        pg_stat_get_dead_tuples(C.oid) AS n_dead_tup,
        --C.reltuples AS reltuples,
        round(current_setting('autovacuum_vacuum_threshold')::integer
        + current_setting('autovacuum_vacuum_scale_factor')::numeric *
        C.reltuples)
        AS av_threshold
        /*date_trunc('minute',greatest(pg_stat_get_last_vacuum_time(C.oid),
        pg_stat_get_last_autovacuum_time(C.oid))) AS last_vacuum, date_trunc('minute',greatest(pg_stat_get_last_analyze_time(C.oid),
        pg_stat_get_last_analyze_time(C.oid))) AS last_analyze*/
        FROM pg_class C
        LEFT JOIN pg_namespace N ON (N.oid = C.relnamespace)
        WHERE C.relkind IN ('r', 't')
        AND N.nspname NOT IN ('pg_catalog', 'information_schema') AND
        N.nspname !~ '^pg_toast'
           ) AS av
          where n_dead_tup > av_threshold;
            """, destination_db_connection)

    def inf(tblname):
        cur = destination_db_connection.cursor()
        print("Analyzing {}".format(tblname))
        cur.execute("analyze {};".format(tblname))
        destination_db_connection.commit()
        cur.close()

    if len(tablenames):
        tablenames['tablenames'].apply(lambda x: inf(x))


def move_jobapp_from_offer_to_place(df, destination_db_connection):
    """

    :param df: dataframe contains position_candidate(job_app) are needed to be change to placements
    :param destination_db_connection:
    :return:
    """

    #
    # update position_candidate: status=301|302|303
    # PLACEMENT_PERMANENT(301, “Placement Permanent”, “PLACEMENT_PERMANENT”),
    # PLACEMENT_CONTRACT(302, “Placement Contract”, “PLACEMENT_CONTRACT”),
    # PLACEMENT_TEMP(303, “Placement Temp”, “PLACEMENT_TEMP”),

    #
    # load position_candidates have offers needed to be changed to placements
    position_candidate = pd.read_sql("""
        select
    currency_type
    , status
    , offer_date
    , placed_date
    , hire_date
    , work_start_date
    , candidate_id
    , position_description_id, id
    from position_candidate where id in (select position_candidate_id from offer);
        """, destination_db_connection)

    position_candidate_placement = df.merge(position_candidate, on=['candidate_id', 'position_description_id', ])

    #
    # load position_descriptions have offers
    position_description = pd.read_sql("""
            select 
                currency_type, id, position_type
                , company_id, contact_id 
            from position_description where id in (select position_description_id from position_candidate where id in (select position_candidate_id from offer))
            """, destination_db_connection)

    position_description.rename(columns={'id': 'position_description_id'}, inplace=True)
    position_candidate_placement = position_candidate_placement.merge(position_description, on='position_description_id')
    position_candidate_placement['status'] = position_candidate_placement.apply(lambda x: int('30%s' % x.position_type), axis=1)

    # update position_candidate status
    psycopg2_bulk_update(position_candidate_placement, destination_db_connection, ['status', ], ['id', ], 'position_candidate')

    #
    # update offers -> placements
    offer = pd.read_sql("""
            select
            draft_offer, position_candidate_id, id
             from offer;
            """, destination_db_connection)

    position_candidate_placement.rename(columns={'id': 'position_candidate_id'}, inplace=True)
    df_place = position_candidate_placement.merge(offer, on='position_candidate_id')
    #
    # update offer.draft_offer=3
    df_place['draft_offer'] = 3
    psycopg2_bulk_update(df_place, destination_db_connection, ['draft_offer', ], ['id', ], 'offer')

    invoice = pd.DataFrame()
    invoice['position_candidate_id'] = df_place['position_candidate_id']
    invoice['offer_id'] = df_place['id']
    invoice['insert_timestamp'] = datetime.datetime.now()
    invoice['status'] = 2
    invoice['valid'] = 1
    invoice['renewal_index'] = 1
    invoice['renewal_flow_status'] = 1

    # check exist invoice because offer id must be unique
    existed_invoice = pd.read_sql("select * from invoice", destination_db_connection)
    invoice = invoice.loc[~invoice['offer_id'].isin(existed_invoice['offer_id'])]

    psycopg2_bulk_insert(invoice, destination_db_connection, invoice.columns, 'invoice')
    # place starting
    # execute_sql_update("update invoice set renewal_flow_status=1", destination_db_connection)


def insert_source_for_contact_candidate(sfilename, source_colname, entity_external_id, table, ddbconn):
    """
    insert contact source or candidate source (they are using the same candidate source)
    :param sfilename: filename contains new source
    :param source_colname: column name contains source value
    :param entity_external_id: candidate external id or contact external id
    :param table: candidate or contact
    :return:
    """
    # sfilename = os.path.join(data_folder, 'cont_source_CGO-04.01.csv')
    # source_colname = 'contact-source'
    # entity_external_id = 'contact-externalId'
    # table = 'contact'

    #
    # prepare input
    df_cont_source = pd.read_csv(sfilename)
    df_cont_source.drop_duplicates(inplace=True)
    #
    # merge input data to the available source from the database
    df_cont_source['lname'] = df_cont_source[source_colname].str.lower()
    df_cont_source = df_cont_source.merge(pd.read_sql("select lower(name) as lname, * from candidate_source", ddbconn), left_on='lname', right_on='lname', how='left')
    #
    # filter out the new source names to be injected
    src_names = df_cont_source[df_cont_source['id'].isnull()][source_colname].unique()
    #
    # new source name will be injected to the database, if no value will be insert, the below method will do nothing
    insert_candidate_source(src_names, ddbconn)
    #
    # remerge sources to entity
    df_cont_source = pd.read_csv(sfilename)
    df_cont_source.drop_duplicates(inplace=True)
    df_cont_source['lname'] = df_cont_source[source_colname].str.lower()
    df_cont_source = df_cont_source.merge(pd.read_sql("select lower(name) as lname, * from candidate_source", ddbconn), left_on='lname', right_on='lname', how='left')
    df_cont_source['candidate_source_id'] = df_cont_source['id']
    df_cont_source['external_id'] = df_cont_source[entity_external_id]
    df_cont_source['external_id'] = df_cont_source['external_id'].astype(str)
    psycopg2_bulk_update(df_cont_source, ddbconn, ['candidate_source_id', ], ['external_id', ], table)


@deprecated(reason="migrate to class vincere candidate")
def update_candidate_current_employer_title(df, colname_extid, colname_empname, colname_titname, ddbconn, logger):
    df_vincere_candidate = pd.read_sql("""select id, experience_details_json, external_id from candidate""", ddbconn)
    df = df.merge(df_vincere_candidate, left_on=colname_extid, right_on='external_id')
    df = df[df[colname_empname].notnull() & (df[colname_empname].str.strip() != '') & df[colname_titname].notnull()]
    # replace job title
    df['experience_details_json'] = df.apply(lambda x: re.sub(r',\"jobTitle\":\".*?\",|,\"jobTitle\":null,', (',"jobTitle":"%s",' % x[colname_titname]), x['experience_details_json']), axis=1)
    df['experience_details_json'] = df.apply(lambda x: re.sub(r',\"currentEmployer\":null,|,\"currentEmployer\":\".*?\",', (',"currentEmployer":"%s",' % x[colname_empname]), x['experience_details_json']), axis=1)

    df['candidate_id'] = df['id']
    df['current_employer'] = df[colname_empname]
    df['current_job_title'] = df[colname_titname]
    df['job_title'] = df[colname_titname]
    df['experience_details_json'] = df['experience_details_json']

    psycopg2_bulk_update_tracking(df, ddbconn, ['current_employer', 'current_job_title', ], ['candidate_id', ], 'candidate_extension', logger)
    psycopg2_bulk_update_tracking(df, ddbconn, ['current_employer', 'job_title', ], ['candidate_id', ], 'candidate_work_history', logger)
    psycopg2_bulk_update_tracking(df, ddbconn, ['experience_details_json', ], ['id', ], 'candidate', logger)


def inject_candidate_source(df, entidy_extid, colname_source, ddbconn):
    """ this function can be run many times without changing result"""
    df.drop_duplicates(inplace=True)
    df['lname'] = df[colname_source].map(lambda x: str(x).strip().lower() if (str(x) != 'nan') and (x != None) else x)
    df[colname_source] = df[colname_source].map(lambda x: str(x).strip() if (str(x) != 'nan') and (x != None) else x)
    # df[colname_source] = df[colname_source].map(lambda x: str(x).strip() if (str(x) != 'nan') and (x != None) else 'Data Import')
    # df['lname'] = df[colname_source].map(lambda x: str(x).strip().lower())  # set default source by [Data Import]
    # df_cand_source = df.merge(pd.read_sql("select lower(name) as lname, * from candidate_source  where location_id in (select id from location where name='All');", ddbconn), left_on='lname', right_on='lname', how='left')
    df_cand_source = df.merge(pd.read_sql("select lower(name) as lname, * from candidate_source;", ddbconn), left_on='lname', right_on='lname', how='left')
    #
    # only new source names will be inserted
    src_names = df_cand_source[df_cand_source['id'].isnull() & df_cand_source[colname_source].notnull()][colname_source].unique()
    insert_candidate_source(src_names, ddbconn)
    #
    # remerge candidate source to get vincere ids
    # df_cand_source = df_cand_source.merge(pd.read_sql("select lower(name) as lname, id as candidate_source_id, * from candidate_source where location_id in (select id from location where name='All');", ddbconn), left_on='lname', right_on='lname', how='left')
    df_cand_source = df_cand_source.merge(pd.read_sql("select lower(name) as lname, id as candidate_source_id, * from candidate_source", ddbconn), left_on='lname', right_on='lname', how='left')
    if 'external_id' not in df.columns:
        df_cand_source['external_id'] = df_cand_source[entidy_extid]
    df_cand_source['external_id'] = df_cand_source['external_id'].astype(str)

    # %% modify at 2019-02-15
    df_cand_source = df_cand_source.merge(pd.read_sql("select id, external_id from candidate", ddbconn), on='external_id')
    # psycopg2_bulk_update(df_cand_source, ddbconn, ['candidate_source_id', ], ['external_id', ], 'candidate')
    psycopg2_bulk_update(df_cand_source, ddbconn, ['candidate_source_id', ], ['id', ], 'candidate')
    # to here

    with ddbconn.cursor() as cur:
        cur.execute("update candidate set candidate_source_id = (select id from candidate_source where name='Data Import') where candidate_source_id is null;")
        ddbconn.commit()


def inject_functional_expertise_subfunctional_expertise(fe, colname_func_exp, sfe, colname_sfunc_exp, ddbconn):
    """
    df_sub_functional_expertise.functional_expertise = df_functional_expertise.name
    :rtype:
    :param df_functional_expertise:
    :param df_sub_functional_expertise:
    :param colname_func_exp:
    :param ddbconn:
    :return:
    """
    df_functional_expertise = fe[[colname_func_exp]].drop_duplicates()
    df_sub_functional_expertise = None
    if sfe is not None:
        df_sub_functional_expertise = sfe[[colname_func_exp, colname_sfunc_exp]].drop_duplicates()
    df_functional_expertise['business_units'] = df_functional_expertise[colname_func_exp]
    df_functional_expertise['department'] = df_functional_expertise[colname_func_exp]
    df_functional_expertise['expertise'] = df_functional_expertise[colname_func_exp]
    df_functional_expertise['name'] = df_functional_expertise[colname_func_exp]

    # this command raise error if functional_expertise_id_seq is not existed
    functional_expertise_id = pd.read_sql("select nextval('functional_expertise_id_seq'::regclass)", ddbconn)
    """
    ---CREATE SEQUENCE FUNCTIONAL EXPERTISE
    CREATE SEQUENCE IF NOT EXISTS functional_expertise_id_seq OWNED BY functional_expertise.id;
    ALTER TABLE functional_expertise ALTER COLUMN id SET DEFAULT nextval('functional_expertise_id_seq'::regclass);
    SELECT setval('functional_expertise_id_seq', (SELECT MAX(id) FROM functional_expertise));
    """
    # generate functional expertise id
    # df_functional_expertise['id'] = [functional_expertise_id.iloc[0,0]+i for i in range(0, len(df_functional_expertise))]

    # clean functional_expertise and sub_functional_expertise
    clean_functional_and_subfunctional_expertise(ddbconn)  # IF CLEAN INDUSTRIES, GO TO SETTING => GROUPS, TAGS & LOCATIONS => GROUPS TAB => YOUR BRAND => INDUSTRIES => ADD ALL

    cols = ['business_units', 'department', 'expertise', 'name']
    psycopg2_bulk_insert(df_functional_expertise, ddbconn, cols, 'functional_expertise')

    if df_sub_functional_expertise is not None:
        # df_sub_functional_expertise = df_sub_functional_expertise.merge(df_functional_expertise[['id', 'name']].rename(columns={'name':'functional_expertise'}), on='functional_expertise')
        df_sub_functional_expertise = df_sub_functional_expertise.merge(pd.read_sql("select id, name as functional_expertise from functional_expertise", ddbconn), left_on=colname_func_exp, right_on='functional_expertise')
        df_sub_functional_expertise.rename(columns={'id': 'functional_expertise_id'}, inplace=True)
        if 'insert_timestamp' not in df_sub_functional_expertise.columns:
            df_sub_functional_expertise['insert_timestamp'] = datetime.datetime.now()
        if 'name' not in df_sub_functional_expertise.columns:
            df_sub_functional_expertise['name'] = df_sub_functional_expertise[colname_sfunc_exp]
        psycopg2_bulk_insert(df_sub_functional_expertise, ddbconn, ['name', 'insert_timestamp', 'functional_expertise_id'], 'sub_functional_expertise')
    # mapping function expertise to team brand
    mapping_functional_expertise_to_team_brand(ddbconn)
    execute_sql_update("delete from sub_functional_expertise where name is null;", ddbconn)


def append_functional_expertise_subfunctional_expertise(df_functional_expertise, colname_func_exp, df_sub_functional_expertise, colname_sfunc_exp, ddbconn):
    """
    df_sub_functional_expertise.functional_expertise = df_functional_expertise.name
    :param df_functional_expertise:
    :param df_sub_functional_expertise:
    :param colname_func_exp:
    :param ddbconn:
    :return:
    """
    df_functional_expertise['business_units'] = df_functional_expertise[colname_func_exp]
    df_functional_expertise['department'] = df_functional_expertise[colname_func_exp]
    df_functional_expertise['expertise'] = df_functional_expertise[colname_func_exp]
    df_functional_expertise['name'] = df_functional_expertise[colname_func_exp]

    # this command raise error if functional_expertise_id_seq is not existed
    functional_expertise_id = pd.read_sql("select nextval('functional_expertise_id_seq'::regclass)", ddbconn)
    """
    ---CREATE SEQUENCE FUNCTIONAL EXPERTISE
    CREATE SEQUENCE IF NOT EXISTS functional_expertise_id_seq OWNED BY functional_expertise.id;
    ALTER TABLE functional_expertise ALTER COLUMN id SET DEFAULT nextval('functional_expertise_id_seq'::regclass);
    SELECT setval('functional_expertise_id_seq', (SELECT MAX(id) FROM functional_expertise));
    """

    cols = ['business_units', 'department', 'expertise', 'name']

    existed_fe = pd.read_sql("select * from functional_expertise", ddbconn)
    test = df_functional_expertise.query("name not in @existed_fe.name")

    psycopg2_bulk_insert(test, ddbconn, cols, 'functional_expertise')

    # df_sub_functional_expertise = df_sub_functional_expertise.merge(df_functional_expertise[['id', 'name']].rename(columns={'name':'functional_expertise'}), on='functional_expertise')
    df_sub_functional_expertise = df_sub_functional_expertise.merge(pd.read_sql("select id, name as functional_expertise from functional_expertise", ddbconn), left_on=colname_func_exp, right_on='functional_expertise')
    df_sub_functional_expertise.rename(columns={'id': 'functional_expertise_id'}, inplace=True)

    if 'insert_timestamp' not in df_sub_functional_expertise.columns:
        df_sub_functional_expertise['insert_timestamp'] = datetime.datetime.now()
    if 'name' not in df_sub_functional_expertise.columns:
        df_sub_functional_expertise['name'] = df_sub_functional_expertise[colname_sfunc_exp]

    existed_sfe = pd.read_sql("select concat(fe.name, sfe.name) as fe_sfe from sub_functional_expertise sfe join functional_expertise fe on sfe.functional_expertise_id = fe.id", ddbconn)
    df_sub_functional_expertise['fe_sfe'] = df_sub_functional_expertise.apply(lambda x: '%s%s' % (x[colname_func_exp], x[colname_sfunc_exp]), axis=1)
    test2 = df_sub_functional_expertise.query("fe_sfe not in @existed_sfe.fe_sfe")

    psycopg2_bulk_insert(test2, ddbconn, ['name', 'insert_timestamp', 'functional_expertise_id'], 'sub_functional_expertise')
    # mapping function expertise to team brand

    allmapping = pd.read_sql("""
        select *, now() as insert_timestamp 
        from (select id as team_group_id from team_group where 1=1 {}) a,
             (select id as functional_expertise_id from functional_expertise) b
        """.format(''), ddbconn)

    existed_mapping = pd.read_sql("""select functional_expertise_id, team_group_id, 'map' as note from team_group_functional_expertise""", ddbconn)

    allmapping = allmapping.merge(existed_mapping, on=['functional_expertise_id', 'team_group_id'], how='left')
    new_mapping = allmapping.query("note.isnull()")
    psycopg2_bulk_insert(new_mapping, ddbconn, ['team_group_id', 'functional_expertise_id', 'insert_timestamp'], 'team_group_functional_expertise')


def inject_cand_func_exper(df_cand, cand_exid, cand_fe_colname, cand_sfe_colname, ddbconn):
    #
    # inserting candidate functional expertise
    df_cand = df_cand.merge(pd.read_sql("select id as candidate_id, external_id from candidate", ddbconn), left_on=cand_exid, right_on='external_id')
    df_cand = df_cand.merge(pd.read_sql("select id as functional_expertise_id, name from functional_expertise", ddbconn), left_on=cand_fe_colname, right_on='name')
    df_cand = df_cand.merge(pd.read_sql("select id as sub_functional_expertise_id, functional_expertise_id, name from sub_functional_expertise", ddbconn), left_on=['functional_expertise_id', cand_sfe_colname], right_on=['functional_expertise_id', 'name'])
    df_cand['insert_timestamp'] = datetime.datetime.now()
    cols = ['functional_expertise_id', 'candidate_id', 'insert_timestamp', 'sub_functional_expertise_id']
    psycopg2_bulk_insert(df_cand, ddbconn, cols, 'candidate_functional_expertise')


def inject_cont_func_exper(df_cont, cont_exid, cont_fe_colname, cont_sfe_colname, ddbconn):
    # inserting contact functional expertise
    df_cont = df_cont.merge(pd.read_sql("select id as contact_id, external_id from contact", ddbconn), left_on=cont_exid, right_on='external_id')
    df_cont = df_cont.merge(pd.read_sql("select id as functional_expertise_id, name from functional_expertise", ddbconn), left_on=cont_fe_colname, right_on='name')
    df_cont = df_cont.merge(pd.read_sql("select id as sub_functional_expertise_id, functional_expertise_id, name from sub_functional_expertise", ddbconn), left_on=['functional_expertise_id', cont_sfe_colname], right_on=['functional_expertise_id', 'name'])
    df_cont['insert_timestamp'] = datetime.datetime.now()
    cols = ['functional_expertise_id', 'contact_id', 'insert_timestamp', 'sub_functional_expertise_id']
    psycopg2_bulk_insert(df_cont, ddbconn, cols, 'contact_functional_expertise')
