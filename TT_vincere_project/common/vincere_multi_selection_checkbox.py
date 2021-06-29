# -*- coding: UTF-8 -*-
import pandas as pd
import datetime
from common import vincere_custom_migration as vcm
import sqlalchemy


def append_muti_selection_checkbox(df, candidate_extid, values_colname, field_key, entity_type, connection_str, df_dropdownlist_values=None, logger=None):
    """
    :param df: data frame contain atleast 2 columns: candidate external id col and drop down list values col
    :param candidate_extid: column name of the candidate external id field
    :param values_colname: column name of the drop down list values field
    :param field_key: field key values on the GUI
    :param ddbconn: destination database connection
    :param df_dropdownlist_values: a data frame contains values of the drop down list
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

    sdbconn_engine = sqlalchemy.create_engine(connection_str)
    connection = sdbconn_engine.raw_connection()

    additional_form_values = pd.read_sql("""
        select * from additional_form_values 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (field_key, field_key), connection)

    configurable_form_language = pd.read_sql("""
        select * from configurable_form_language 
        where language_code in (
        select title_language_code from configurable_form_field_value 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
        )""" % (field_key, field_key), connection)

    configurable_form_field_value = pd.read_sql("""
        select * from configurable_form_field_value 
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s') 
        """ % (field_key, field_key), connection)

    df = df[df[values_colname].notnull()]

    test = df[[values_colname]].drop_duplicates() if df_dropdownlist_values is None else df_dropdownlist_values.drop_duplicates()
    test = test.merge(configurable_form_language, left_on=values_colname, right_on='translate', how='outer', indicator=True)
    test = test.query("_merge == 'left_only'")

    drpd_vals = test[values_colname].unique()
    drpd_vals = [x for x in drpd_vals if str(x) != 'nan']
    drpd_vals.sort()
    vcm.insert_drop_down_list_values(drpd_vals, field_key, connection)

    df = df.merge(pd.read_sql("select id as additional_id, external_id from {}".format(entbl), connection), left_on=candidate_extid, right_on='external_id')
    sql = """
        select a.translate, '%s' as additional_type, b.form_id, b.field_id, b.field_value, b.insert_timestamp, b.field_id as constraint_id
        from configurable_form_language a
        join configurable_form_field_value b on a.language_code=b.title_language_code
        where field_id in (select id from configurable_form_field where  field_key ='%s')
        and form_id in (select form_id from configurable_form_field where  field_key ='%s')
        """ % (additional_type, field_key, field_key)
    df = df.merge(pd.read_sql(sql, connection), left_on=values_colname, right_on='translate')
    # insert to: additional_form_values
    cols = ['additional_type', 'additional_id', 'form_id', 'field_id', 'field_value', 'insert_timestamp', 'constraint_id']

    temp_series = df.groupby(['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'])['field_value'].apply(lambda x: ','.join(x))  # group by groupby_colname, join all the contents by ','
    df = pd.DataFrame(temp_series).reset_index()  #

    if len(df):
        df['constraint_id'] = df['constraint_id'].astype(str)
        test2 = df.merge(additional_form_values, on=['additional_type', 'additional_id', 'form_id', 'field_id', 'constraint_id'], how='left', suffixes=('', '_y'))
        test2 = test2.loc[test2['insert_timestamp'].isnull()]

        test2['insert_timestamp'] = datetime.datetime.now()
        vcm.psycopg2_bulk_insert_tracking(test2, connection, cols, 'additional_form_values', logger)
