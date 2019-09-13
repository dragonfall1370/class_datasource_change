# -*- coding: UTF-8 -*-
import warnings
import pandas as pd
import datetime
import psycopg2
from dateutil.relativedelta import relativedelta

company = {
    'Suburb': 'company_location.district',
    'State': 'company_location.state',
    'Postcode': 'company_location.post_code',
    'Country': 'company_location.country.country_code',
    'Phone': 'company.phone',
    'Fax': 'company.fax',
    'URL': 'company.website',
    'Brief': 'company.note',
    'Company Owners': 'company.company_owners',
}

contact = {
    'Contact Owners': 'contact.contact_owners',
    'Brief': 'contact.note',
    'Salutation': 'contact.gender_title',
    'LinkedIn': 'contact.linkedin',
    'Primary Phone': 'contact.phone',
    'Primary Email': 'contact.email',
    'Mobile Phone': 'contact.mobile_phone',
    'Home Email': 'contact.personal_email',
}

job = {
    'Pay - Range From': 'compensation.contract_rate_from.annual_salary_from',
    'Pay - Range To': 'compensation.contract_rate_to.annual_salary_to',
    'Pay Interval': 'compensation.contract_rate_type', # 1/2/3/4: hour/day/week/month
    'Contract Length Type': 'compensation.contract_length_type', # 1/2/3/4/5: hour/day/week/month/year
    'Contract Length': 'compensation.contract_length',
    'Public Job Description': 'position_description.public_description',
    'Headcount': 'position_description.head_count',
    'Skills / Keywords': 'position_description.key_words',
    'Job Owners': 'position_agency_consultant.position_id.user_id.insert_timestamp',
    'Submission Date': 'position_description.submission_date',
    'Time': 'position_description.submission_time',
    'Internal Job Description': 'position_description.full_description',
    '': '',
}

candiate = {
    'Facebook': 'candidate.facebook',
    'LinkedIn': 'candidate.linked_in_profile',
    'Twitter': 'candidate.twitter',
    'Primary Phone': 'candidate.phone',
    'Mobile Phone': 'candidate.phone2',
    'Home Phone': 'candidate.home_phone',
    'Title': 'candidate.gender_title',
    'Salutation': 'candidate.gender_title',
    'Work Email': 'candidate.work_email',
    'Salary Type': 'candidate.salary_type',  # 0/1/2: notspecified/annual/month
    'Salary': 'candidate.current_salary',
    'Salary Amount': 'candidate.current_salary',
    'Salary Currency': 'candidate.currency_type',
    'Brief': 'candidate.note',
}

placement_detail = {
    'Placement Note': 'offer_personal_info.note',
    'Internal note': 'offer_personal_info.note',
    'Offer Date': 'offer_personal_info.offer_date',
    'Placement date': 'offer_personal_info.placed_date',
    'Start Date': 'offer_personal_info.start_date',
    'End date': 'offer_personal_info.end_date',
}


class Util():
    @staticmethod
    def df_split_to_listofdfs(df, max_rows=50000):
        dataframes = []
        while len(df) > max_rows:
            top = df[:max_rows]
            dataframes.append(top)
            df = df[max_rows:]
        else:
            dataframes.append(df)
        return dataframes

    @staticmethod
    def psycopg2_bulk_update(self, df, db_conn, up_cols, wh_cols, tblname):
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

    @staticmethod
    def psycopg2_bulk_update_tracking(self, df, db_conn, up_cols, wh_cols, tblname, logger):
        """
         :param df: source dataframe
        :param db_conn: destination database connection
        :param up_cols: updated columns name
        :param wh_cols: where columns name
        :param tblname: table name
        :return:
        """
        offset = 0
        dfs = self.df_split_to_listofdfs(df, 1000)
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

            sql = """UPDATE {0} SET {1} FROM (VALUES %s) AS data ({3})
                 WHERE {2} """.format(tblname, _1, _2, _3)
            with db_conn.cursor() as cur:
                psycopg2.extras.execute_values(cur, sql, list_values, template=None, page_size=1000)
                db_conn.commit()


class Candidate(Util):
    def insert_candidate_source(self, src_names, ddbconn):
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
        self.psycopg2_bulk_insert(df, ddbconn, cols, 'candidate_source')

    def inject_candidate_source(self, df, entidy_extid, colname_source, ddbconn):
        """ this function can be run many times without changing result"""
        df.drop_duplicates(inplace=True)
        df['lname'] = df[colname_source].map(lambda x: str(x).strip().lower() if (str(x) != 'nan') and (x != None) else x)
        df[colname_source] = df[colname_source].map(lambda x: str(x).strip() if (str(x) != 'nan') and (x != None) else x)
        # df[colname_source] = df[colname_source].map(lambda x: str(x).strip() if (str(x) != 'nan') and (x != None) else 'Data Import')
        # df['lname'] = df[colname_source].map(lambda x: str(x).strip().lower())  # set default source by [Data Import]
        df_cand_source = df.merge(pd.read_sql("select lower(name) as lname, * from candidate_source  where location_id in (select id from location where name='All');", ddbconn), left_on='lname', right_on='lname', how='left')
        #
        # only new source names will be inserted
        src_names = df_cand_source[df_cand_source['id'].isnull() & df_cand_source[colname_source].notnull()][colname_source].unique()
        self.insert_candidate_source(src_names, ddbconn)
        #
        # remerge candidate source to get vincere ids
        df_cand_source = df_cand_source.merge(pd.read_sql("select lower(name) as lname, id as candidate_source_id, * from candidate_source where location_id in (select id from location where name='All');", ddbconn), left_on='lname', right_on='lname', how='left')
        if 'external_id' not in df.columns:
            df_cand_source['external_id'] = df_cand_source[entidy_extid]
        df_cand_source['external_id'] = df_cand_source['external_id'].astype(str)
        # %% modify at 2019-02-15
        df_cand_source = df_cand_source.merge(pd.read_sql("select id, external_id from candidate", ddbconn), on='external_id')
        # psycopg2_bulk_update(df_cand_source, ddbconn, ['candidate_source_id', ], ['external_id', ], 'candidate')
        self.psycopg2_bulk_update(df_cand_source, ddbconn, ['candidate_source_id', ], ['id', ], 'candidate')
        # to here

        with ddbconn.cursor() as cur:
            cur.execute("update candidate set candidate_source_id = (select id from candidate_source where name='Data Import') where candidate_source_id is null;")
            ddbconn.commit()