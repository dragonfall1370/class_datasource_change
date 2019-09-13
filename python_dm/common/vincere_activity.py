# -*- coding: UTF-8 -*-
"""
activity process
1 - select max(id) as current_activity_id from activity
2 - run insert_activities_1 to insert activity for company, contact, candidate, job
3 - run map_activities_to_entities to map new inserted activity to corresponding entities
"""
import pandas as pd
import psycopg2
from common import vincere_common
import numpy as np
import sqlalchemy


def map_activities_to_entities(connection_str, from_activity_id):
    # remap activity
    sdbconn_engine = sqlalchemy.create_engine(connection_str)
    connection = sdbconn_engine.raw_connection()
    cur = connection.cursor()
    # insert activity mapping
    cur.execute(
        """delete from activity_candidate where activity_id > {0}
        ;
        insert into activity_candidate (activity_id, candidate_id, insert_timestamp) 
        SELECT id 
        , candidate_id 
        , insert_timestamp 
        from activity 
        where candidate_id is not NULL 
        and candidate_id > 0
        and candidate_id in (select id from candidate) 
        and id not in (23, 24, 25)
        and id > {0} 
        ;
        """.format(from_activity_id)
    )
    connection.commit()
    cur.execute(
        """delete from activity_company where activity_id > {0}
        ;
        insert into activity_company (activity_id, company_id, insert_timestamp) 
        SELECT id 
        , company_id 
        , insert_timestamp 
        from activity 
        where company_id is not NULL 
        and company_id > 0 
        and company_id in (select id from company)
        and id not in (23, 24, 25) 
        and id > {0}
        ;
        """.format(from_activity_id)
    )
    connection.commit()
    cur.execute(
        """delete from activity_contact where activity_id > {0}
        ;
        insert into activity_contact (activity_id, contact_id, insert_timestamp) 
        SELECT id 
        , contact_id 
        , insert_timestamp 
        from activity 
        where contact_id is not NULL 
        and contact_id > 0
        and contact_id in (select id from contact)  
        and id not in (23, 24, 25) 
        and id > {0}
        ;
        """.format(from_activity_id)
    )
    connection.commit()
    cur.execute(
        """delete from activity_job where activity_id > {0}
        ;
        insert into activity_job (activity_id, job_id, insert_timestamp) 
        SELECT id 
        , position_id 
        , insert_timestamp 
        from activity 
        where position_id is not NULL 
        and position_id > 0 
        and position_id in (select id from position_description) 
        and id not in (23, 24, 25) 
        and id > {0}
        ;
        """.format(from_activity_id)
    )
    connection.commit()
    cur.close()
    connection.close()


def insert_activities(df_activities, connection_str, logger):
    """
    :param df_activities:
    :param db_conn:
    :param logger:
    :return:
    """
    sdbconn_engine = sqlalchemy.create_engine(connection_str)
    connection = sdbconn_engine.raw_connection()
    cur = connection.cursor()
    current_activity_id = pd.read_sql("select max(id) as current_activity_id from activity", connection)

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
    page_size = 100  # TRUONG MENTION
    dfs = vincere_common.df_split_to_listofdfs(df_activities, page_size)
    fail_insert_batch = []
    for _idx, _df in enumerate(dfs):
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
        for attempt in range(10):
            try:
                psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=page_size)
                connection.commit()
            except Exception as ex:
                logger.error(ex)
                logger.info("Reconnect to database and then retry the insert command")
                # reconnect to db
                sdbconn_engine = sqlalchemy.create_engine(connection_str)
                connection = sdbconn_engine.raw_connection()
                cur = connection.cursor()
            else:  # insert successfully at the first attempt
                break
        else:  # fail all the attempt time
            logger.info("Error batch data will be saved")
            _df.to_csv('error_activity_insert_batch_{0}.csv'.format(_idx), index=False, header=True, sep=',')
            fail_insert_batch.append('error_activity_insert_batch_{0}.csv'.format(_idx))

    cur.close()
    connection.close()
    map_activities_to_entities(connection_str, current_activity_id.loc[0, 'current_activity_id'])
    for e in fail_insert_batch:
        logger.error("Error batch data: %" % e)


def insert_activities_1(df_act, connection_str, logger):
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

        sdbconn_engine = sqlalchemy.create_engine(connection_str)
        connection = sdbconn_engine.raw_connection()

        df_vin_cont = pd.read_sql("select id as contact_id, external_id from contact where deleted_timestamp is null", connection)
        df_vin_cand = pd.read_sql("select id as candidate_id, external_id from candidate where deleted_timestamp is null", connection)
        df_vin_comp = pd.read_sql("select id as company_id, external_id from company where deleted_timestamp is null", connection)
        df_vin_posi = pd.read_sql("select id as position_id, external_id from position_description where deleted_timestamp is null", connection)
        df_owner = pd.read_sql("select id as user_account_id, email from user_account", connection)

        # make sure external_id values are in str type
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
        connection.close()
        insert_activities(df_act, connection_str, logger)


def transform_activities_temp(df_act, connection_str, logger, extra_cols=[]):
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

        # sdbconn_engine = sqlalchemy.create_engine(connection_str)
        sdbconn_engine = sqlalchemy.create_engine(connection_str, pool_size=100, max_overflow=200, client_encoding='utf8', use_batch_mode=True)

        connection = sdbconn_engine.raw_connection()

        df_vin_cont = pd.read_sql("select id as contact_id, external_id from contact where deleted_timestamp is null", connection)
        df_vin_cand = pd.read_sql("select id as candidate_id, external_id from candidate where deleted_timestamp is null", connection)
        df_vin_comp = pd.read_sql("select id as company_id, external_id from company where deleted_timestamp is null", connection)
        df_vin_posi = pd.read_sql("select id as position_id, external_id from position_description where deleted_timestamp is null", connection)
        df_owner = pd.read_sql("select id as user_account_id, email from user_account", connection)

        # make sure external_id values are in str type
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
        connection.close()
        test = df_act[[
            'company_id',
            'contact_id',
            'candidate_id',
            'position_id',
            'user_account_id',
            'insert_timestamp',
            'content',
            'category',
        ]+extra_cols]
        return test
        # vincere_common.write_to_db_2(sdbconn_engine, test, 'temp_tung_activity', 1000, vincere_common.sqlcol(test, is_postgre=True), logger, thr_num=20)

# def set_label_for_activities(df, label_name):

