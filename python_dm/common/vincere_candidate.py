# -*- coding: UTF-8 -*-
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common
import numpy as np
import pandas as pd
import datetime
import re


class Candidate(vincere_common.Common):
    def __init__(self, ddbconn):
        vincere_common.Common.__init__(self)
        if ddbconn:
            self.ddbconn = ddbconn
            self.candidate = pd.read_sql("select id, external_id as candidate_externalid, current_location_id, experience_details_json, edu_details_json from candidate", ddbconn)

    def get_candidate_location(self):
        tem2 = pd.read_sql("""
        			select
            c.external_id as candidate_externalid
            , c.id as candidate_id
            , cl.id as candidate_location_id
            , c.first_name
            , c.last_name
            , cl.address
            , cl.district
            , cl.city
            , cl.state
            , cl.country
            , cl.latitude
            , cl.longitude
            , cl.post_code
            from candidate c
            join common_location cl on c.current_location_id = cl.id;
        """, self.ddbconn)
        return tem2

    def update_preferred_name(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['candidate_externalid', 'preferred_name']]
        tem2['nickname'] = tem2['preferred_name']

        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['nickname', ], ['id', ], 'candidate', logger)
        return tem2

    def set_all_current_candidate_address_as_mailling_address(self):
        vincere_custom_migration.execute_sql_update("""
        update common_location 
        set location_name=address
            , location_type='MAILING_ADDRESS' 
        where location_name is null and address is not null
        and id in (select current_location_id from candidate);
        """, self.ddbconn)

    def update_keyword(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'keyword']]
        tem2 = tem2.groupby('candidate_externalid').apply(lambda sufdf: ','.join(set(map(str, sufdf['keyword'])))).reset_index()
        tem2.columns = ['candidate_externalid', 'keyword']
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['keyword', ], ['id', ], 'candidate', logger)
        return tem2

    def update_desire_salary(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'desire_salary']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['desire_salary', ], ['id', ], 'candidate', logger)
        return tem2

    def update_notice_period(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'notice_period']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['notice_period', ], ['id', ], 'candidate', logger)
        return tem2

    def update_desired_job_type(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'desired_job_type']]
        assert set(tem2['desired_job_type'].value_counts().keys()) \
            .issubset(set(self.jobtype['desc'].values)), \
            "There are some invalid job types values"
        tem2 = tem2.merge(self.jobtype, left_on='desired_job_type', right_on='desc')

        tem2['desired_job_type_json'] = tem2['position_type'].apply(lambda x: '{"desiredJobTypeId":"%s"}' % x)
        tem2.groupby('desired_job_type')['desired_job_type_json'].apply(','.join)
        tem3 = tem2.groupby('candidate_externalid')['desired_job_type_json'].apply(lambda x: '[%s]' % ','.join(set(x))).reset_index()

        tem3 = tem3.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem3, self.ddbconn, ['desired_job_type_json', ], ['id', ], 'candidate', logger)
        return tem3

    def update_desired_job_type_2(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'desired_job_type']]
        tem2.desired_job_type.fillna('permanent', inplace=True)
        tem2.desired_job_type.replace({'Contract': 'contract', 'Permanent': 'permanent'}, inplace=True)
        assert set(tem2['desired_job_type'].value_counts().keys()) \
            .issubset(set(self.jobtype['desc'].values)), \
            "There are some invalid job types values"
        tem2 = tem2.merge(self.jobtype, left_on='desired_job_type', right_on='desc')

        tem2['desired_job_type_json'] = tem2['position_type'].apply(lambda x: '{"desiredJobTypeId":"%s"}' % x)
        tem2.groupby('desired_job_type')['desired_job_type_json'].apply(','.join)
        tem3 = tem2.groupby('candidate_externalid')['desired_job_type_json'].apply(lambda x: '[%s]' % ','.join(set(x))).reset_index()

        tem3 = tem3.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem3, self.ddbconn, ['desired_job_type_json', ], ['id', ], 'candidate', logger)
        return tem3

    def update_contract_rate(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'contract_rate']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate', ], ['id', ], 'candidate', logger)
        return tem2

    def update_contract_interval(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'contract_interval']]
        tem2.loc[tem2.contract_interval.str.lower().isin(['hour', 'hourly']), 'contract_interval'] = 'hour'
        tem2.loc[tem2.contract_interval.str.lower().isin(['day', 'daily']), 'contract_interval'] = 'day'
        tem2.loc[tem2.contract_interval.str.lower().isin(['week', 'weekly']), 'contract_interval'] = 'week'
        tem2.loc[tem2.contract_interval.str.lower().isin(['month', 'monthly']), 'contract_interval'] = 'month'
        tem2.loc[tem2.contract_interval.str.lower().isin(['quarter', 'quarterly']), 'contract_interval'] = 'quarter'
        tem2.loc[tem2.contract_interval.str.lower().isin(['year', 'yearly', 'annually']), 'contract_interval'] = 'year'
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_interval', ], ['id', ], 'candidate', logger)
        return tem2

    def update_owner(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'email']]
        tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid, candidate_owner_json from candidate", self.ddbconn), on=['candidate_externalid'])
        tem2 = tem2.merge(pd.read_sql("select id as user_account_id, email from user_account", self.ddbconn), on='email')
        tem2.candidate_owner_json.fillna('[{"ownerId":"-1"}]', inplace=True)
        tem2 = tem2.candidate_owner_json \
            .map(lambda x: eval(x)) \
            .apply(pd.Series) \
            .merge(tem2, left_index=True, right_index=True) \
            .drop('candidate_owner_json', axis=1) \
            .drop('email', axis=1) \
            .drop('candidate_externalid', axis=1) \
            .melt(id_vars=['id', 'user_account_id'], value_name='ownerId') \
            .drop('variable', axis=1)
        tem2 = tem2.where(tem2.notnull(), None)
        tem2.ownerId = tem2.ownerId.map(lambda x: x.get('ownerId') if x else x)
        tem2.loc[tem2.ownerId.notnull()]
        tem2 = tem2.melt(id_vars='id').dropna()
        tem2 = tem2.loc[tem2.value != '-1']
        tem2['candidate_owner_json'] = tem2.value.map(lambda x: '{"ownerId":"%s"}' % x)
        tem2 = tem2.groupby('id').apply(lambda subdf: list(set(subdf.candidate_owner_json))).reset_index().rename(columns={0: 'candidate_owner_json'})
        tem2.candidate_owner_json = tem2.candidate_owner_json.map(lambda x: '[%s]' % ', '.join(x))
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['candidate_owner_json', ], ['id', ], 'candidate', logger)
        return tem2

    def insert_source(self, df):
        tem2 = df[['candidate_externalid', 'source']].dropna()
        vincere_custom_migration.inject_candidate_source(tem2, 'candidate_externalid', 'source', self.ddbconn)

    def update_skills(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'skills']].drop_duplicates().groupby('candidate_externalid')['skills'].apply('\n'.join).reset_index()
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['skills', ], ['id', ], 'candidate', logger)
        return tem2

    def update_exprerience_work_history(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'experience']].drop_duplicates().groupby('candidate_externalid')['experience'].apply('\n\n'.join).reset_index()
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2.experience = tem2.experience.map(lambda x: x.replace('\n', '<br/>'))
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['experience', ], ['id', ], 'candidate', logger)
        # vincere_custom_migration.execute_sql_update(r"update candidate set experience=replace(experience, '\n', chr(10)) where experience is not null;", self.ddbconn)
        return tem2

    def update_currency_type(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'currency']]
        tem2['currency'] = tem2['currency'].str.strip()

        tem2.loc[tem2['currency'].isin(['US Dol', '$', 'US$/A', 'USD', 'US$']), 'currency_type'] = 'usd'  # The United State Dollar
        tem2.loc[tem2['currency'].isin(['SGD', 'Singap', '(SGD)', 'SG$', 'SGD$', 'SGD Mo', '$SGD M', 'S$', '$SGD']), 'currency_type'] = 'singd'  # Sinapore Dollar
        tem2.loc[tem2['currency'].isin(['Hong K', 'HKD', 'HK$', 'HKD$']), 'currency_type'] = 'hkd'  # Hong Kong dollar
        tem2.loc[tem2['currency'].isin(['AU$', 'AUD']), 'currency_type'] = 'aud'  # Australia
        tem2.loc[tem2['currency'].isin(['£', 'GBP']), 'currency_type'] = 'pound'  # Pounds, United Kingdom
        tem2.loc[tem2['currency'].isin(['RMB', 'Renmin', '300Ren']), 'currency_type'] = 'yuan'  # Chinese yuan/renmin
        tem2.loc[tem2['currency'].isin(['MYR']), 'currency_type'] = 'myr'  # Malaysian Ringgit (MYRRM)
        tem2.loc[tem2['currency'].isin(['AED']), 'currency_type'] = 'aed'  # UAE Dirham (AED)
        tem2.loc[tem2['currency'].isin(['NT$']), 'currency_type'] = 'twd'  # Taiwan Dollar (TWDNT$)

        tem2['currency_type'] = tem2['currency'].map(self.map_currency_code)

        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['currency_type', ], ['id', ], 'candidate', logger)
        return tem2

    def update_current_salary(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'current_salary']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_salary', ], ['id', ], 'candidate', logger)
        return tem2

    def insert_gdpr(self, df, logger):
        """
        """
        vincere_custom_migration.remove_candidate_gdpr_compliance(self.ddbconn)
        df = df.merge(self.candidate, on=['candidate_externalid'])
        df['candidate_id'] = df.id
        if 'owner' in df.columns:
            df = df.merge(pd.read_sql('select id as obtained_by, lower(email) as email from user_account where deleted_timestamp is null', self.ddbconn),
                      left_on='owner', right_on='email',
                      how='left')
            df['obtained_by'].fillna(-10, inplace=True)
        df['obtained_by'] = -10
        df['obtained_by'] = df['obtained_by'].astype(np.int64)
        cols = [
            'request_through',
            'request_through_date',
            'obtained_through',
            'obtained_through_date',
            'expire',
            'consent_level',
            'portal_status',
            'obtained_by',
            'insert_timestamp',
            'candidate_id',
            'notes',
            'expire_date',
            'explicit_consent',
            'exercise_right',
        ]
        df['exercise_right'].fillna(-1111, inplace=True)
        df['request_through'].fillna(-1111, inplace=True)
        df['obtained_through'].fillna(-1111, inplace=True)
        df['expire'].fillna(-1111, inplace=True)
        df['consent_level'].fillna(-1111, inplace=True)
        df['portal_status'].fillna(-1111, inplace=True)
        df['explicit_consent'].fillna(-1111, inplace=True)

        for c, t in zip(df.columns, df.dtypes):
            if str(t).startswith('float'):
                df[c] = df[c].astype(np.int64)
                df[c] = df[c].astype(str)
                df[c] = df[c].replace('-1111', np.nan)

        vincere_custom_migration.psycopg2_bulk_insert_tracking(df, self.ddbconn, cols, 'candidate_gdpr_compliance', logger)
        return df

    def update_currency_of_salary(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'currency_of_salary']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['currency_type'] = tem2['currency_of_salary']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['currency_type', ], ['id', ], 'candidate', logger)
        return tem2

    def update_salary_type(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'SalaryType']]
        vincere_salary_type = {'NotSpecified': 0, 'Monthly': 2, 'Annual': 1}
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])

        tem2.loc[tem2['SalaryType'].str.contains('NotSpecified', False), 'salary_type'] = vincere_salary_type.get('NotSpecified')
        tem2.loc[tem2['SalaryType'].str.contains('perhour', False), 'salary_type'] = vincere_salary_type.get('Monthly')
        tem2.loc[tem2['SalaryType'].str.contains('perday', False), 'salary_type'] = vincere_salary_type.get('Monthly')
        tem2.loc[tem2['SalaryType'].str.contains('perweek', False), 'salary_type'] = vincere_salary_type.get('Monthly')
        tem2.loc[tem2['SalaryType'].str.contains('permonth', False), 'salary_type'] = vincere_salary_type.get('Monthly')
        tem2.loc[tem2['SalaryType'].str.contains('peryear', False), 'salary_type'] = vincere_salary_type.get('Annual')

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['salary_type', ], ['id', ], 'candidate', logger)
        return tem2

    def update_desired_contract_rate(self, df, logger):
        """
        """
        tem2 = df[['candidate_externalid', 'desired_contract_rate']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['desired_contract_rate', ], ['id', ], 'candidate', logger)
        return tem2

    def insert_fe_sfe(self, df, logger):
        tem2 = df[['functional_expertise_id', 'candidate_externalid', 'sub_functional_expertise_id']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['candidate_id'] = tem2['id']
        tem2['insert_timestamp'] = datetime.datetime.now()
        vincere_custom_migration.psycopg2_bulk_insert(tem2, self.ddbconn, ['functional_expertise_id', 'candidate_id', 'insert_timestamp', 'sub_functional_expertise_id'], 'candidate_functional_expertise')
        return tem2

    def insert_fe_sfe2(self, df, logger):
        tem2 = df[['candidate_externalid', 'fe', 'sfe']]

        tem2 = tem2.merge(pd.read_sql('select id as functional_expertise_id, name as fe from functional_expertise', self.ddbconn), on='fe', how='left')
        tem2 = tem2.merge(pd.read_sql('select functional_expertise_id, id as sub_functional_expertise_id, name as sfe from sub_functional_expertise', self.ddbconn), on=['functional_expertise_id', 'sfe'], how='left')
        tem2 = tem2.where(tem2.notnull(), None)
        tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'] = tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'].astype(int)

        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['candidate_id'] = tem2['id']
        tem2['insert_timestamp'] = datetime.datetime.now()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['functional_expertise_id', 'candidate_id', 'insert_timestamp', 'sub_functional_expertise_id'], 'candidate_functional_expertise', logger)
        return tem2

    def insert_fe_sfe2_inhouse(self, df, logger):
        tem2 = df[['candidate_id', 'fe', 'sfe']]
        tem2 = tem2.where(tem2.notnull(), None)
        tem2['fe'] = tem2['fe'].apply(lambda x: str.strip(x).lower() if x else x)
        tem2['sfe'] = tem2['sfe'].apply(lambda x: str.strip(x).lower() if x else x)

        tem2 = tem2.merge(pd.read_sql('select id as functional_expertise_id, lower(trim(name)) as fe from functional_expertise', self.ddbconn), on='fe', how='left')
        tem2 = tem2.merge(pd.read_sql('select functional_expertise_id, id as sub_functional_expertise_id, lower(trim(name)) as sfe from sub_functional_expertise', self.ddbconn), on=['functional_expertise_id', 'sfe'], how='left')
        tem2 = tem2.where(tem2.notnull(), None)
        tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'] = tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'].astype(int)
        tem2['insert_timestamp'] = datetime.datetime.now()

        # check existed
        existed_fesfe = pd.read_sql("select id, candidate_id, functional_expertise_id, sub_functional_expertise_id from candidate_functional_expertise;", self.ddbconn)
        existed_fesfe = existed_fesfe.where(existed_fesfe.notnull(), None)
        tem2 = tem2.merge(existed_fesfe, on=['candidate_id', 'functional_expertise_id', 'sub_functional_expertise_id'], how='left')
        tem2 = tem2.query('id.isnull()')

        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['functional_expertise_id', 'candidate_id', 'insert_timestamp', 'sub_functional_expertise_id'], 'candidate_functional_expertise', logger)
        return tem2

    def update_note(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'note']].dropna()
        tem2.note = tem2.note.map(lambda x: x.replace('\n', '<br/>'))
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['note', ], ['id', ], 'candidate', logger)
        # vincere_custom_migration.execute_sql_update(r"update candidate set note=replace(note, '\n', chr(10)) where note is not null;", self.ddbconn)
        # vincere_custom_migration.execute_sql_update(r"update candidate set note=replace(note, '\n', '<br/>') where note is not null;", self.ddbconn)
        return tem2

    def update_note2(self, df, connectparams, logger):
        """
        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'note']].dropna()
        tem2.note = tem2.note.map(lambda x: x.replace('\n', '<br/>'))
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, connectparams, 'update', 'candidate', ['note', ], ['id'], logger)
        return tem2

    def update_reg_date(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['candidate_externalid', 'reg_date']]
        tem2['insert_timestamp'] = tem2['reg_date']
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['insert_timestamp'], ['id'], 'candidate', logger)
        return tem2

    def update_date_of_birth(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['candidate_externalid', 'date_of_birth']]
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['date_of_birth'], ['id'], 'candidate', logger)
        return tem2

    def update_website(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['candidate_externalid', 'website']]
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['website'], ['id'], 'candidate', logger)
        return tem2

    def update_education_summary(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['candidate_externalid', 'education_summary']].drop_duplicates().groupby('candidate_externalid')['education_summary'].apply('\n\n'.join).reset_index()
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['education_summary'], ['id'], 'candidate', logger)
        return tem2

    def update_education_summary_v2(self, df, dbconn_param, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['candidate_externalid', 'education_summary']].drop_duplicates().groupby('candidate_externalid')['education_summary'].apply('\n\n'.join).reset_index()
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, dbconn_param, 'update', 'candidate', ['education_summary'], ['id'], logger)
        return tem2

    def update_qualification(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['candidate_externalid', 'qualification']]
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2.loc[tem2['edu_details_json'].isnull(), 'edu_details_json'] = '[{"qualification":""},{"educationId":"","schoolName":"","schoolAddress":"","institutionName":"","institutionAddress":"","course":"","startDate":"","graduationDate":"","training":"","degreeName":"","qualification":"","department":"","thesis":"","description":"","grade":"","gpa":"","honorific":"","hornors":"","major":"","minor":""}]'
        tem2['edu_details_json'] = tem2.apply(lambda x: re.sub(r'\"qualification\":null,|\"qualification\":\".*?\",', ('"qualification":"%s",' % x['qualification']), x['edu_details_json']), axis=1)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['edu_details_json'], ['id'], 'candidate', logger)
        return tem2

    def update_middle_name(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        tem2 = df[['candidate_externalid', 'middle_name']]
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['middle_name'], ['id'], 'candidate', logger)
        return tem2

    def update_gender_title(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        tem2 = df[['candidate_externalid', 'gender_title']]
        # transform data
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2[['id', 'gender_title']].dropna().drop_duplicates(), self.ddbconn, ['gender_title'], ['id'], 'candidate', logger)
        return tem2

    def update_candidate_current_employer_title(self, df, logger):
        """
        'current_employer', 'current_job_title'
        :param df:
        :param logger:
        :return:
        """
        tem2 = df[['candidate_externalid', 'current_employer', 'current_job_title']].fillna('')
        tem2 = tem2.merge(self.candidate, left_on='candidate_externalid', right_on='candidate_externalid')

        # replace job title
        tem2.loc[tem2['experience_details_json'].isnull(), 'experience_details_json'] = '[{"company":null,"jobTitle":null,"currentEmployer":null,"yearOfExperience":null,"industry":null,"functionalExpertiseId":null,"subFunctionId":null,"cbEmployer":null,"currentEmployerId":null,"dateRangeFrom":null,"dateRangeTo":null}]'
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"jobTitle\":\".*?\",|,\"jobTitle\":null,', (',"jobTitle":"%s",' % x['current_job_title']), x['experience_details_json']), axis=1)
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"currentEmployer\":null,|,\"currentEmployer\":\".*?\",', (',"currentEmployer":"%s",' % x['current_employer']), x['experience_details_json']), axis=1)
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"cbEmployer\":null,|,\"cbEmployer\":\".*?\",', (',"cbEmployer":"%s",' % '1'), x['experience_details_json']), axis=1)

        tem2['candidate_id'] = tem2['id']
        tem2['job_title'] = tem2['current_job_title']

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_employer', 'current_job_title', ], ['candidate_id', ], 'candidate_extension', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_employer', 'job_title', ], ['candidate_id', ], 'candidate_work_history', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['experience_details_json', ], ['id', ], 'candidate', logger)
        return tem2

    def update_candidate_current_employer_title_v2(self, df, conn_param, logger):
        """
        'current_employer', 'current_job_title'
        :param df:
        :param logger:
        :return:
        """
        tem2 = df[['candidate_externalid', 'current_employer', 'current_job_title']].fillna('')
        tem2 = tem2.merge(self.candidate, left_on='candidate_externalid', right_on='candidate_externalid')

        # replace job title
        tem2.loc[tem2['experience_details_json'].isnull(), 'experience_details_json'] = '[{"company":null,"jobTitle":null,"currentEmployer":null,"yearOfExperience":null,"industry":null,"functionalExpertiseId":null,"subFunctionId":null,"cbEmployer":null,"currentEmployerId":null,"dateRangeFrom":null,"dateRangeTo":null}]'
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"jobTitle\":\".*?\",|,\"jobTitle\":null,', (',"jobTitle":"%s",' % x['current_job_title']), x['experience_details_json']), axis=1)
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"currentEmployer\":null,|,\"currentEmployer\":\".*?\",', (',"currentEmployer":"%s",' % x['current_employer']), x['experience_details_json']), axis=1)
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"cbEmployer\":null,|,\"cbEmployer\":\".*?\",', (',"cbEmployer":"%s",' % '1'), x['experience_details_json']), axis=1)

        tem2['candidate_id'] = tem2['id']
        tem2['job_title'] = tem2['current_job_title']

        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_employer', 'current_job_title', ], ['candidate_id', ], 'candidate_extension', logger)
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_employer', 'job_title', ], ['candidate_id', ], 'candidate_work_history', logger)
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['experience_details_json', ], ['id', ], 'candidate', logger)

        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'candidate_extension', ['current_employer', 'current_job_title', ], ['candidate_id', ], logger)
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'candidate_work_history', ['current_employer', 'job_title', ], ['candidate_id', ], logger)
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'candidate', ['experience_details_json', ], ['id', ], logger)
        return tem2

    def update_candidate_current_employer(self, df, logger):
        tem2 = df[['candidate_externalid', 'current_employer']].dropna().drop_duplicates()
        tem2 = tem2.merge(self.candidate, left_on='candidate_externalid', right_on='candidate_externalid')

        # replace job title
        tem2.loc[tem2['experience_details_json'].isnull(), 'experience_details_json'] = '[{"company":null,"jobTitle":null,"currentEmployer":null,"yearOfExperience":null,"industry":null,"functionalExpertiseId":null,"subFunctionId":null,"cbEmployer":null,"currentEmployerId":null,"dateRangeFrom":null,"dateRangeTo":null}]'
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"currentEmployer\":null,|,\"currentEmployer\":\".*?\",', (',"currentEmployer":"%s",' % x['current_employer']), x['experience_details_json']), axis=1)
        tem2['experience_details_json'] = tem2.apply(lambda x: re.sub(r',\"cbEmployer\":null,|,\"cbEmployer\":\".*?\",', (',"cbEmployer":"%s",' % '1'), x['experience_details_json']), axis=1)

        tem2['candidate_id'] = tem2['id']

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_employer', ], ['candidate_id', ], 'candidate_extension', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_employer', ], ['candidate_id', ], 'candidate_work_history', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['experience_details_json', ], ['id', ], 'candidate', logger)
        return tem2

    def update_gender(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'gender']].dropna()
        tem2.loc[tem2['gender'].str.lower() == 'female', 'male'] = 0
        tem2.loc[tem2['gender'].str.lower() == 'male', 'male'] = 1
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['male', ], ['id', ], 'candidate', logger)
        return tem2

    def update_dob(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'date_of_birth']].dropna()
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['date_of_birth', ], ['id', ], 'candidate', logger)
        return tem2

    def update_candidate_company_name(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'company_name']].dropna()
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['company_name', ], ['id', ], 'candidate', logger)
        return tem2

    def update_candidate_company_number(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'company_number']].dropna()
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['company_number', ], ['id', ], 'candidate', logger)
        return tem2

    def update_work_email(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'work_email']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['work_email', ], ['id', ], 'candidate', logger)
        return tem2

    def update_work_email_v2(self, df, dbconn_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'work_email']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, dbconn_param, 'update', 'candidate', ['work_email', ], ['id', ], logger)
        return tem2

    def update_primary_email(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'primary_email']]
        tem2['email'] = tem2['primary_email']
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['email', ], ['id', ], 'candidate', logger)
        return tem2

    # def update_personal_email(self, df, logger):
    #     """
    #       khong co personal_email trong bang candidate
    #     :rtype: object
    #     """
    #     tem2 = df[['candidate_externalid', 'personal_email']].dropna().drop_duplicates()
    #     tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
    #     vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['personal_email', ], ['id', ], 'candidate', logger)
    #     return tem2

    def update_home_phone(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'home_phone']].dropna().drop_duplicates()
        tem2 = tem2.groupby('candidate_externalid').apply(lambda sufdf: ', '.join(sufdf.home_phone)).reset_index().rename(columns={0: 'home_phone'})
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['home_phone', ], ['id', ], 'candidate', logger)
        return tem2

    def update_work_phone(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'work_phone']].dropna().drop_duplicates()
        tem2 = tem2.groupby('candidate_externalid').apply(lambda sufdf: ', '.join(sufdf.work_phone)).reset_index().rename(columns={0: 'work_phone'})
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['work_phone', ], ['id', ], 'candidate', logger)
        return tem2

    def update_mobile_phone(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'mobile_phone']].dropna().drop_duplicates()
        tem2 = tem2.groupby('candidate_externalid').apply(lambda sufdf: ', '.join(sufdf.mobile_phone)).reset_index().rename(columns={0: 'mobile_phone'})
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['phone2'] = tem2['mobile_phone']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['phone2', ], ['id', ], 'candidate', logger)
        return tem2

    def update_mobile_phone_v2(self, df, dbconn_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'mobile_phone']].dropna().drop_duplicates()
        tem2 = tem2.groupby('candidate_externalid').apply(lambda sufdf: ', '.join(sufdf.mobile_phone)).reset_index().rename(columns={0: 'mobile_phone'})
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['phone2'] = tem2['mobile_phone']
        vincere_custom_migration.load_data_to_vincere(tem2, dbconn_param, 'update', 'candidate', ['phone2', ], ['id', ], logger)
        return tem2

    def update_primary_phone(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'primary_phone']].dropna().drop_duplicates()
        tem2 = tem2.groupby('candidate_externalid').apply(lambda sufdf: ', '.join(sufdf.primary_phone)).reset_index().rename(columns={0: 'primary_phone'})
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['phone'] = tem2['primary_phone']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['phone', ], ['id', ], 'candidate', logger)
        return tem2

    def update_primary_phone_v2(self, df, dbconn_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'primary_phone']].dropna().drop_duplicates()
        tem2 = tem2.groupby('candidate_externalid').apply(lambda sufdf: ', '.join(sufdf.primary_phone)).reset_index().rename(columns={0: 'primary_phone'})
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['phone'] = tem2['primary_phone']
        vincere_custom_migration.load_data_to_vincere(tem2, dbconn_param, 'update', 'candidate', ['phone', ], ['id', ], logger)
        return tem2

    def update_linkedin(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'linkedin']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['linked_in_profile'] = tem2['linkedin']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['linked_in_profile', ], ['id', ], 'candidate', logger)
        return tem2

    def update_linkedin_v2(self, df, dbcon_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'linkedin']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['linked_in_profile'] = tem2['linkedin']
        vincere_custom_migration.load_data_to_vincere(tem2, dbcon_param, 'update', 'candidate', ['linked_in_profile', ], ['id', ], logger)
        return tem2

    def update_facebook(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'facebook']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['facebook', ], ['id', ], 'candidate', logger)
        return tem2

    def update_twitter(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'twitter']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['twitter', ], ['id', ], 'candidate', logger)
        return tem2

    def update_skill_languages(self, df, logger):
        tem2 = df[['candidate_externalid', 'language', 'level']]

        tem2.loc[tem2.level.str.lower().isin(['native']), 'level'] = 5  # native
        tem2.loc[tem2.level.str.lower().isin(['excellent', 'fluent']), 'level'] = 4  # fluent
        tem2.loc[tem2.level.str.lower().isin(['advanced', ]), 'level'] = 3  # advanced
        tem2.loc[tem2.level.str.lower().isin(['intermediate', ]), 'level'] = 2  # intermediate
        tem2.loc[tem2.level.str.lower().isin(['beginner', 'good', 'basic']), 'level'] = 1  # intermediate
        tem2.level.unique()
        tem2 = tem2.merge(pd.read_sql("select code, native_name as language from language", self.ddbconn), on='language') \
            .rename(columns={'code': 'languageCode'})
        tem2 = tem2.fillna('')
        tem2.languageCode = tem2.languageCode.map(lambda x: '"languageCode":"%s"' % x)
        tem2.level = tem2.level.map(lambda x: '"level":"%s"' % x)
        tem2['skill_details_json'] = tem2[['languageCode', 'level']].apply(lambda x: '{%s}' % (','.join(x)), axis=1)
        tem2 = tem2.groupby('candidate_externalid')['skill_details_json'].apply(','.join).reset_index()
        tem2.skill_details_json = tem2.skill_details_json.map(lambda x: '[%s]' % x)
        # [{"languageCode":"km","level":""},{"languageCode":"my","level":""}]
        tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid from candidate", self.ddbconn), on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['skill_details_json', ], ['id', ], 'candidate', logger)
        return tem2

    def update_employment_type(self, df, logger):
        """
        employment_type
        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'employment_type']].dropna().drop_duplicates()

        tem2.loc[tem2.employment_type.isin(['perm', 'contract', 'fulltime']), 'employment_type'] = 0  # full time
        tem2.loc[tem2.employment_type.isin(['temp', 'parttime']), 'employment_type'] = 1  # part time
        tem2.loc[tem2.employment_type.isin(['casual', 'consultant']), 'employment_type'] = 2  # casual
        tem2.loc[tem2.employment_type.isin(['labourhire', ]), 'employment_type'] = 3  # labour hire

        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['employment_type', ], ['id', ], 'candidate', logger)
        return tem2

    def update_nationality(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'nationality']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['nationality', ], ['id', ], 'candidate', logger)
        return tem2

    def insert_common_location(self, df, logger):
        """
        can be run many times, the second will fire an exception:
            psycopg2.IntegrityError: duplicate key value violates unique constraint "current_location_candidate_uni_idx"
            DETAIL:  Key (current_location_candidate_id)=(84120) already exists.

        :param df:
        :param logger:
        :return:
        """
        tem2 = df[['location_name', 'address', 'candidate_externalid', ]]

        tem2.address = tem2.address.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        tem2.address = tem2.address.map(lambda x: ', '.join([e.strip() for e in x if e]))

        tem2.location_name = tem2.location_name.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        tem2.location_name = tem2.location_name.map(lambda x: ', '.join([e.strip() for e in x if e]))

        tem2 = tem2.drop_duplicates()

        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'current_location_candidate_id'})
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2.current_location_candidate_id.fillna(-1, inplace=True)

        # tem2['current_location_candidate_id'] = tem2['id']

        # check existed loc, if there are existed loc, then update them
        existed_loc = pd.read_sql("select id, current_location_candidate_id from common_location", self.ddbconn)
        existed_loc.current_location_candidate_id.fillna(-2, inplace=True)

        tem2 = tem2.merge(existed_loc, on='current_location_candidate_id', how='left')
        update_existed_loc = tem2.loc[tem2.id.notnull()]
        insert_loc = tem2.loc[tem2.id.isnull()]

        current_loc_id = pd.read_sql("select max(id) as id from common_location", self.ddbconn)  # 28488
        vincere_custom_migration.psycopg2_bulk_insert_tracking(insert_loc, self.ddbconn, ['location_name', 'address', 'insert_timestamp', 'current_location_candidate_id'], 'common_location', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(update_existed_loc, self.ddbconn, ['location_name', 'address', 'insert_timestamp', ], ['id'], 'common_location', logger)

        # get new loc
        new_loc = pd.read_sql("select id as current_location_id, current_location_candidate_id from common_location --where id > {}".format(current_loc_id.loc[0].get('id')), self.ddbconn)
        tem2 = tem2.merge(new_loc, on='current_location_candidate_id', suffixes=['_x', ''])
        tem2.id = tem2.current_location_candidate_id
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_location_id'], ['id'], 'candidate', logger)
        return tem2

    def insert_common_location_v2(self, df, dbcon_param, logger):
        """
        can be run many times, the second will fire an exception:
            psycopg2.IntegrityError: duplicate key value violates unique constraint "current_location_candidate_uni_idx"
            DETAIL:  Key (current_location_candidate_id)=(84120) already exists.

        :param df:
        :param logger:
        :return:
        """
        tem2 = df[['location_name', 'address', 'candidate_externalid', ]]

        tem2.address = tem2.address.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        tem2.address = tem2.address.map(lambda x: ', '.join([e.strip() for e in x if e]))

        tem2.location_name = tem2.location_name.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        tem2.location_name = tem2.location_name.map(lambda x: ', '.join([e.strip() for e in x if e]))

        tem2 = tem2.drop_duplicates()

        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'current_location_candidate_id'})
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2.current_location_candidate_id.fillna(-1, inplace=True)

        # tem2['current_location_candidate_id'] = tem2['id']

        # check existed loc, if there are existed loc, then update them
        existed_loc = pd.read_sql("select id, current_location_candidate_id from common_location", self.ddbconn)
        existed_loc.current_location_candidate_id.fillna(-2, inplace=True)

        tem2 = tem2.merge(existed_loc, on='current_location_candidate_id', how='left')
        update_existed_loc = tem2.loc[tem2.id.notnull()]
        insert_loc = tem2.loc[tem2.id.isnull()]

        current_loc_id = pd.read_sql("select max(id) as id from common_location", self.ddbconn)  # 28488
        vincere_custom_migration.load_data_to_vincere(insert_loc, dbcon_param, 'insert', 'common_location',['location_name', 'address', 'insert_timestamp', 'current_location_candidate_id'], [], logger)
        vincere_custom_migration.load_data_to_vincere(update_existed_loc, dbcon_param, 'update', 'common_location', ['location_name', 'address', 'insert_timestamp', ], ['id'], logger)

        # get new loc
        new_loc = pd.read_sql("select id as current_location_id, current_location_candidate_id from common_location --where id > {}".format(current_loc_id.loc[0].get('id')), self.ddbconn)
        tem2 = tem2.merge(new_loc, on='current_location_candidate_id', suffixes=['_x', ''])
        tem2.id = tem2.current_location_candidate_id
        vincere_custom_migration.load_data_to_vincere(tem2, dbcon_param, 'update', 'candidate', ['current_location_id'], ['id'], logger)
        return tem2

    def update_common_location_address(self, df, logger):
        """
        can be run many times, the second will fire an exception:
            psycopg2.IntegrityError: duplicate key value violates unique constraint "current_location_candidate_uni_idx"
            DETAIL:  Key (current_location_candidate_id)=(84120) already exists.

        :param df:
        :param logger:
        :return:
        """
        try:
            tem2 = df[['address', 'candidate_externalid', ]]
            tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
            tem2['id'] = tem2['current_location_id']
            vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['address'], ['id'], 'common_location', logger)
            return tem2
        except:
            pass

    def update_common_location_address_2(self, df, conn_param, logger):
        """
        can be run many times, the second will fire an exception:
            psycopg2.IntegrityError: duplicate key value violates unique constraint "current_location_candidate_uni_idx"
            DETAIL:  Key (current_location_candidate_id)=(84120) already exists.

        :param df:
        :param logger:
        :return:
        """
        tem2 = df[['address', 'candidate_externalid', ]]
        tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid, current_location_id from candidate", self.ddbconn), on=['candidate_externalid'])
        tem2['id'] = tem2['current_location_id']
        tem2['location_name'] = tem2['address']
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'common_location', ['address', 'location_name', ], ['id'], logger)
        return tem2

    def update_personal_location_address_2(self, df, conn_param, logger):
        tem2 = df[['address', 'candidate_externalid', ]]
        existed_loc = pd.read_sql("select id as common_location_id, address, personal_location_candidate_id from common_location where address is not null", self.ddbconn)
        tem2 = tem2.merge(existed_loc, on='address', how='left')
        tem2 = tem2.merge(pd.read_sql("select id as candidate_id, external_id as candidate_externalid, personal_location_id from candidate", self.ddbconn), on=['candidate_externalid'])

        loc_upd = tem2.loc[tem2.common_location_id.notnull()]
        loc_ins = tem2.loc[tem2.common_location_id.isnull()]

        loc_upd['common_location_id'] = loc_upd['common_location_id'].astype(int)
        loc_upd['personal_location_id'] = loc_upd['common_location_id']
        loc_upd['personal_location_candidate_id'] = loc_upd['candidate_id']
        loc_upd['location_name'] = loc_upd['address']
        loc_upd['id'] = loc_upd['common_location_id']
        vincere_custom_migration.load_data_to_vincere(loc_upd, conn_param, 'update', 'common_location', ['address', 'location_name', 'personal_location_candidate_id'], ['id'], logger)

        loc_upd['id'] = loc_upd['candidate_id']
        vincere_custom_migration.load_data_to_vincere(loc_upd, conn_param, 'update', 'candidate', ['personal_location_id'], ['id'], logger)

        loc_ins['location_name'] = loc_ins['address']
        loc_ins['personal_location_candidate_id'] = loc_ins['candidate_id']
        loc_ins['insert_timestamp'] = datetime.datetime.now()
        vincere_custom_migration.load_data_to_vincere(loc_ins, conn_param, 'insert', 'common_location', ['location_name', 'address', 'personal_location_candidate_id', 'insert_timestamp'], '', logger)

        loc_ins = loc_ins.drop('common_location_id', axis=1)\
            .merge(pd.read_sql("select id as common_location_id, address from common_location", self.ddbconn), on='address')
        loc_ins['personal_location_id'] = loc_ins['common_location_id']

        loc_ins['id'] = loc_ins['common_location_id']
        vincere_custom_migration.load_data_to_vincere(loc_ins, conn_param, 'update', 'common_location', ['address', 'location_name', 'personal_location_candidate_id'], ['id'], logger)

        loc_ins['id'] = loc_ins['candidate_id']
        vincere_custom_migration.load_data_to_vincere(loc_ins, conn_param, 'update', 'candidate', ['personal_location_id'], ['id'], logger)
        return loc_upd, loc_ins

    def update_common_location_location_name(self, df, logger):
        """
        can be run many times, the second will fire an exception:
            psycopg2.IntegrityError: duplicate key value violates unique constraint "current_location_candidate_uni_idx"
            DETAIL:  Key (current_location_candidate_id)=(84120) already exists.

        :param df:
        :param logger:
        :return:
        """
        try:
            tem2 = df[['location_name', 'candidate_externalid', ]]
            tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
            tem2['id'] = tem2['current_location_id']
            vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['location_name'], ['id'], 'common_location', logger)
        except:
            pass

    def update_location_state(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'state']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['state', ], ['id', ], 'common_location', logger)
        return tem2

    def update_location_district(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'district']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['district', ], ['id', ], 'common_location', logger)
        return tem2

    def update_personal_location_state(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'state']]
        tem2 = tem2.merge(pd.read_sql("select id as candidate_id, external_id as candidate_externalid, personal_location_id from candidate", self.ddbconn), on=['candidate_externalid'])
        tem2['id'] = tem2['personal_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['state', ], ['id', ], 'common_location', logger)
        return tem2

    def update_location_city(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'city']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['city', ], ['id', ], 'common_location', logger)
        return tem2

    def update_personal_location_city(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'city']]
        tem2 = tem2.merge(pd.read_sql("select id as candidate_id, external_id as candidate_externalid, personal_location_id from candidate", self.ddbconn), on=['candidate_externalid'])
        tem2['id'] = tem2['personal_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['city', ], ['id', ], 'common_location', logger)
        return tem2

    def update_location_post_code(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'post_code']]
        tem2 = tem2.merge(pd.read_sql("select id, external_id as candidate_externalid, current_location_id from candidate", self.ddbconn), on=['candidate_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['post_code', ], ['id', ], 'common_location', logger)
        return tem2

    def update_personal_location_post_code(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'post_code']]
        tem2 = tem2.merge(pd.read_sql("select id as candidate_id, external_id as candidate_externalid, personal_location_id from candidate", self.ddbconn), on=['candidate_externalid'])
        tem2['id'] = tem2['personal_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['post_code', ], ['id', ], 'common_location', logger)
        return tem2

    def update_location_latlong(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'latitude', 'longitude']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['candidate_id'] = tem2['id']
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['latitude', 'longitude', ], ['id', ], 'common_location', logger)
        return tem2

    def update_location_latlong_inhouse(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_id', 'latitude', 'longitude']]
        tem2 = tem2.merge(self.candidate, left_on='candidate_id', right_on='id')
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['latitude', 'longitude', ], ['id', ], 'common_location', logger)
        return tem2

    def update_location_country_code(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'country_code']]
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['country_code', ], ['id', ], 'common_location', logger)
        return tem2

    def update_personal_location_country_code(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['candidate_externalid', 'country_code']]
        tem2 = tem2.merge(pd.read_sql("select id as candidate_id, external_id as candidate_externalid, personal_location_id from candidate", self.ddbconn), on=['candidate_externalid'])
        tem2['id'] = tem2['personal_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['country_code', ], ['id', ], 'common_location', logger)
        return tem2

    def process_gender_title(self, df, title_col, gender_col):
        title = {
            'MR': ('mr', 'm r'),
            'MRS': ('mrs',),
            'MS': ('ms', 'm s'),
            'MISS': ('miss', 'mis'),
            'DR': ('dr',),
        }
        gender = {
            'MALE': ('male', 'm'),
            'FEMALE': ('female', 'f'),
        }
        df[gender_col] = ['MALE' if str(x).strip().lower() in gender.get('MALE') else x for x in df[gender_col]]
        df[gender_col] = ['FEMALE' if str(x).strip().lower() in gender.get('FEMALE') else x for x in df[gender_col]]

        df[title_col] = ['MR' if str(x).strip().lower() in title.get('MR') else x for x in df[title_col]]
        df[title_col] = ['MRS' if str(x).strip().lower() in title.get('MRS') else x for x in df[title_col]]
        df[title_col] = ['MS' if str(x).strip().lower() in title.get('MS') else x for x in df[title_col]]
        df[title_col] = ['MISS' if str(x).strip().lower() in title.get('MISS') else x for x in df[title_col]]
        df[title_col] = ['DR' if str(x).strip().lower() in title.get('DR') else x for x in df[title_col]]

        df[title_col] = ['MR' if str(x[title_col]).lower() == 'nan' and str(x[gender_col]).strip().lower() in gender.get('MALE') else x[title_col] for idx, x in df.iterrows()]
        df[title_col] = ['MS' if str(x[title_col]).lower() == 'nan' and str(x[gender_col]).strip().lower() in gender.get('FEMALE') else x[title_col] for idx, x in df.iterrows()]

        df.loc[(df[gender_col].isnull()) & ((df[title_col] == 'MRS') | (df[title_col] == 'MS') | (df[title_col] == 'MISS')), gender_col] = 'FEMALE'
        df.loc[(df[gender_col].isnull()) & (df[title_col] == 'MR'), gender_col] = 'MALE'
        # other are blank
        df[title_col] = [x if x in ('MR', 'MRS', 'MS', 'MISS', 'DR',) else '' for x in df[title_col]]



    # def get_country_code(self, country_name):
    #     """https://www.nationsonline.org/oneworld/country_code_list.htm"""
    #     from common import country_code
    #     return_code = ''
    #     for k, v in country_code.country_codes.items():
    #         # print("code {0}, name {1}".format(k, v))
    #         if str(country_name).lower().strip() in [i.lower() for i in v]:  # check an item exits in the tuple v
    #             return_code = k
    #             break
    #     return return_code

    def set_position_type(self, x):
        try:
            if x.lower() in ('p', 'permanent', 'fulltime'):
                return 'PERMANENT'
            elif x.lower() in ('c', 'contract'):
                return 'CONTRACT'
            elif x.lower() in ('t', 'parttime', 'temporary', 'temporary_to_permanent'):
                # return 'TEMPORARY_TO_PERMANENT'
                """
                Hi All,
                Please do not perform any fields mapping to TEMPORARY job type anymore. All Temp Jobs should be migrated to CONTRACT type.
                Product team is going to deprecate the job type TEMPORARY moving forward. So to make sure your work is not gone, please make sure to map it correctly to the CONTRACT job.
                Many thanks,
                Andi
                """
                return 'CONTRACT'
            else:
                return 'PERMANENT'
        except Exception as e:
            print('input value %s' % x)
            raise TypeError('%s' % e)

    def process_vincere_cand(self, df, logger):
        if 'candidate-email' in df.columns:
            logger.info("Candidate email processing...")
            t0 = datetime.datetime.now()
            df = self.process_vincere_email(df, 'candidate-externalId', 'candidate-email')
            logger.info('Candidate email completed in: %s' % (datetime.datetime.now() - t0))
        if 'candidate-citizenship' in df.columns:
            df['candidate-citizenship'] = [self.get_country_code(x) for x in df['candidate-citizenship']]
        if 'candidate-country' in df.columns:
            df['candidate-country'] = [self.get_country_code(x) for x in df['candidate-country']]
        if 'candidate-Country' in df.columns:
            df['candidate-Country'] = [self.get_country_code(x) for x in df['candidate-Country']]
        if 'candidate-company1' in df.columns:
            df['candidate-company1'] = df['candidate-company1'].fillna('')
            df['candidate-company1'] = df['candidate-company1'].apply(lambda x: x.replace('\n', ','))
            df['candidate-company1'] = df['candidate-company1'].apply(lambda x: x.replace('\r', ','))
            df['candidate-company1'] = df['candidate-company1'].apply(lambda x: re.sub(r"\,{2,}|,\s$|\,\s*\,{1,}", ',', x))
            df['candidate-company1'] = df['candidate-company1'].apply(lambda x: re.sub(r"\,\s{1,}$", '', x))
        if 'candidate-address' in df.columns:
            df['candidate-address'] = df['candidate-address'].fillna('')
            df['candidate-address'] = df['candidate-address'].apply(lambda x: x.replace('\n', ','))
            df['candidate-address'] = df['candidate-address'].apply(lambda x: x.replace('\r', ','))
            df['candidate-address'] = df['candidate-address'].apply(lambda x: re.sub(r"\,{2,}|,\s$|\,\s*\,{1,}", ',', x))
            df['candidate-address'] = df['candidate-address'].apply(lambda x: re.sub(r"\,\s{1,}$", '', x))
            df['candidate-address'] = df['candidate-address'].apply(lambda x: x.replace('\r', ','))
            df['candidate-address'] = df['candidate-address'].map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
            df['candidate-address'] = df['candidate-address'].map(lambda x: ', '.join([e.strip() for e in x if e]))
        if 'candidate-title' in df.columns and 'candidate-gender' in df.columns:
            t0 = datetime.datetime.now()
            logger.info("Candidate gender and title processing...")
            self.process_gender_title(df, 'candidate-title', 'candidate-gender')
            logger.info('Candidate gender and title completed in: %s' % (datetime.datetime.now() - t0))
        if 'candidate-title' in df.columns:
            self.process_title(df, 'candidate-title')
        if 'candidate-note' in df.columns:
            df['candidate-note'] = df['candidate-note'].apply(lambda x: x.replace('\r', ','))
        if 'candidate-gender' in df.columns:
            df['candidate-gender'] = df['candidate-gender'].str.strip()
        if 'candidate-firstName' in df.columns:
            df['candidate-firstName'] = [x if x is not None and str(x) != 'nan' and str(x).strip() != '' else '[DEFAULT_FIRSTNAME]' for x in df['candidate-firstName']]
        if 'candidate-FirstName' in df.columns:
            df['candidate-FirstName'] = [x if x is not None and str(x) != 'nan' and str(x).strip() != '' else '[DEFAULT_FIRSTNAME]' for x in df['candidate-FirstName']]
        if 'candidate-Lastname' in df.columns:
            df['candidate-Lastname'] = [x if x is not None and str(x) != 'nan' and str(x).strip() != '' else '[DEFAULT_LASTNAME]' for x in df['candidate-Lastname']]
        if 'candidate-lastName' in df.columns:
            df['candidate-lastName'] = [x if x is not None and str(x) != 'nan' and str(x).strip() != '' else '[DEFAULT_LASTNAME]' for x in df['candidate-lastName']]
        if 'candidate-middleName' in df.columns:
            pass
        if 'candidate-homePhone' in df.columns:
            pass
        if 'candidate-mobile' in df.columns:
            pass
        if 'candidate-workPhone' in df.columns:
            pass
        if 'candidate-phone' in df.columns:
            df['candidate-phone'].fillna('', inplace=True)
            df['candidate-phone'] = df['candidate-phone'].map(lambda x: re.sub(r'\(|\)', '', x))  # substitue ( or ) by empty char
        if 'candidate-jobTitle1' in df.columns:
            pass
        if 'candidate-jobType' in df.columns:
            df['candidate-jobType'] = df['candidate-jobType'].fillna('')
            df['candidate-jobType'] = df['candidate-jobType'].map(lambda x: re.sub(r"\s{1,}", '', x))
            # PERMANENT, INTERIM_PROJECT_CONSULTING, TEMPORARY, CONTRACT. TEMPORARY_TO_PERMANENT default PERMANENT
            df['candidate-jobType'] = df['candidate-jobType'].apply(lambda x: self.set_position_type(x))
            # df['candidate-jobType'] = df.apply(lambda x: 'PERMANENT' if str(x['candidate-jobType']).lower() in ('permanent')
            #                                                          else ('CONTRACT' if str(x['candidate-jobType']).lower() in ('contract')
            #                                                          else ('TEMPORARY' if str(x['candidate-jobType']).lower() in ('temporary')
            #                                                          else 'PERMANENT'))
            #                                    , axis=1)

        return df.filter(regex='^candidate')

    def insert_industry(self, df_vertical, mylog):
        cols = ['name', 'insert_timestamp']

        # clean industries_cand (vertical)
        mylog.info("cleaning industries_cand")
        vincere_custom_migration.clean_industry(self.ddbconn)  # IF CLEAN INDUSTRIES, GO TO SETTING => GROUPS, TAGS & LOCATIONS => GROUPS TAB => YOUR BRAND => INDUSTRIES => ADD ALL

        # insert new industries_cand (vertical)
        mylog.info("inserting vertical")
        vincere_custom_migration.psycopg2_bulk_insert(df_vertical, self.ddbconn, cols, 'vertical')

        # insert vertical_detail_language: TAKE TIME TO BE EFFECTED
        mylog.info("inserting vertical_detail_language")
        df_vertical = df_vertical.merge(pd.read_sql('select * from vertical', self.ddbconn), left_on=['name', 'insert_timestamp'], right_on=['name', 'insert_timestamp'])
        df_vertical['vertical_id'] = df_vertical['id']
        df_vertical['language'] = 'en'
        cols = ['vertical_id', 'language', 'name', 'insert_timestamp']
        vincere_custom_migration.psycopg2_bulk_insert(df_vertical, self.ddbconn, cols, 'vertical_detail_language')
        vincere_custom_migration.mapping_industries_to_team_brand(self.ddbconn)
        return df_vertical

    def append_industry(self, df_vertical, mylog):
        current_vertical_id = pd.read_sql("select max(id) as current_vertical_id from vertical", self.ddbconn).loc[0].get('current_vertical_id')
        cols = ['name', 'insert_timestamp']
        # insert new industries_cand (vertical)
        mylog.info("inserting vertical")
        df_vertical = df_vertical.merge(pd.read_sql("select id, name from vertical", self.ddbconn), on='name', how='left')
        df_vertical = df_vertical.query("id.isnull()")
        df_vertical = df_vertical[cols].drop_duplicates()

        vincere_custom_migration.psycopg2_bulk_insert(df_vertical, self.ddbconn, cols, 'vertical')

        # insert vertical_detail_language: TAKE TIME TO BE EFFECTED
        mylog.info("inserting vertical_detail_language")
        df_vertical = df_vertical.merge(pd.read_sql('select * from vertical', self.ddbconn), left_on=['name', 'insert_timestamp'], right_on=['name', 'insert_timestamp'])
        df_vertical['vertical_id'] = df_vertical['id']
        df_vertical['language'] = 'en'
        cols = ['vertical_id', 'language', 'name', 'insert_timestamp']
        vincere_custom_migration.psycopg2_bulk_insert(df_vertical, self.ddbconn, cols, 'vertical_detail_language')
        vincere_custom_migration.mapping_industries_to_team_brand(self.ddbconn, from_industry_id=current_vertical_id)
        return df_vertical

    def insert_candidate_industry(self, df, logger):
        tem2 = df[['candidate_externalid', 'name']].dropna().drop_duplicates()
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2.rename(columns={'id':'candidate_id', }, inplace=True)
        cols = ['vertical_id', 'candidate_id', 'insert_timestamp']
        tem2 = tem2.merge(pd.read_sql('select * from vertical', self.ddbconn), on='name')
        tem2.rename(columns={'id': 'vertical_id', }, inplace=True)
        tem2 = tem2.merge(pd.read_sql("select vertical_id, candidate_id, 'existed' as note from candidate_industry", self.ddbconn), on=['vertical_id', 'candidate_id'], how='left')
        tem2 = tem2.loc[tem2['note'].isnull()]
        tem2['seq'] = tem2.groupby('candidate_id').cumcount()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, cols, 'candidate_industry', logger)
        return tem2

    def insert_candidate_industry_inhouse(self, df, logger):
        tem2 = df[['candidate_id', 'name']]
        cols = ['vertical_id', 'candidate_id', 'insert_timestamp']
        tem2 = tem2.merge(pd.read_sql('select * from vertical', self.ddbconn), on='name')
        tem2.rename(columns={'id': 'vertical_id', }, inplace=True)
        tem2 = tem2.merge(pd.read_sql("select vertical_id, candidate_id, 'existed' as note from candidate_industry", self.ddbconn), on=['vertical_id', 'candidate_id'], how='left')
        tem2 = tem2.loc[tem2['note'].isnull()]
        tem2['seq'] = tem2.groupby('candidate_id').cumcount()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, cols, 'candidate_industry', logger)
        return tem2

    def update_national_insurance_number(self, df, field_key = '1ef7ecc7a33336f1f5845a9792da42a0'):
        """
        National Insurance number: field_key = '1ef7ecc7a33336f1f5845a9792da42a0'
        :param df:
        :return:
        """
        tem2 = df[['candidate_externalid', 'ni_number']].dropna()
        # field_key = pd.read_sql("select * from configurable_form_field where lower(name)=lower('National Insurance number')", self.ddbconn)
        vincere_custom_migration.insert_candidate_text_field_values(tem2, 'candidate_externalid', 'ni_number', field_key, self.ddbconn)

    def insert_onboarding_company_details__company_name(self, df, logger):
        tem2 = df[['candidate_externalid', 'company_name']].dropna()
        tem2['index'] = tem2.groupby('candidate_externalid').cumcount() + 1
        pare_cff = pd.read_sql("select * from configurable_form_field where field_key='13ed2b722787d9ea372c1fd895dcbd37';", self.ddbconn)
        chil_cff = pd.read_sql("select * from configurable_form_field where field_key='26be26a7ab140e34626c06d1006b6284';", self.ddbconn)
        parent_id = pare_cff.loc[0].get('id')
        children_id = chil_cff.loc[0].get('id')
        children_constraint_id = chil_cff.loc[0].get('constraint_id')
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'candidate_id'})
        # tem2['candidate_id'] = tem2['id']
        tem2['parent_id'] = parent_id
        tem2['children_id'] = children_id
        tem2['text_data'] = tem2['company_name']
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2['constraint_id'] = children_constraint_id

        # check existed
        ext = pd.read_sql("select id, candidate_id, parent_id, children_id, constraint_id, 'existed' as exsited from configurable_form_group_value", self.ddbconn)
        tem2 = tem2.merge(ext, on=['candidate_id', 'parent_id', 'children_id', 'constraint_id'], how='left')
        update_tem2 = tem2.loc[tem2.exsited.notnull()]
        insert_tem2 = tem2.loc[tem2.exsited.isnull()]
        vincere_custom_migration.psycopg2_bulk_insert_tracking(insert_tem2, self.ddbconn, ['candidate_id', 'parent_id', 'children_id', 'text_data', "index", 'insert_timestamp', 'constraint_id'],
                                                               'configurable_form_group_value', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(update_tem2, self.ddbconn, ['text_data'], ['id'], 'configurable_form_group_value', logger)
        return tem2

    def insert_onboarding_company_details__date_of_incorporation(self, df, logger):
        tem2 = df[['candidate_externalid', 'date_data']].dropna()
        tem2['index'] = tem2.groupby('candidate_externalid').cumcount() + 1
        pare_cff = pd.read_sql("select * from configurable_form_field where field_key='13ed2b722787d9ea372c1fd895dcbd37';", self.ddbconn)
        chil_cff = pd.read_sql("select * from configurable_form_field where field_key='4cd2914683d2c339834b8a982f051ed5';", self.ddbconn)
        parent_id = pare_cff.loc[0].get('id')
        children_id = chil_cff.loc[0].get('id')
        children_constraint_id = chil_cff.loc[0].get('constraint_id')
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'candidate_id'})
        # tem2['candidate_id'] = tem2['id']
        tem2['parent_id'] = parent_id
        tem2['children_id'] = children_id
        # tem2['text_data'] = tem2['company_name']
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2['constraint_id'] = children_constraint_id

        # check existed
        ext = pd.read_sql("select id, candidate_id, parent_id, children_id, constraint_id, 'existed' as exsited from configurable_form_group_value", self.ddbconn)
        tem2 = tem2.merge(ext, on=['candidate_id', 'parent_id', 'children_id', 'constraint_id'], how='left')
        update_tem2 = tem2.loc[tem2.exsited.notnull()]
        insert_tem2 = tem2.loc[tem2.exsited.isnull()]
        vincere_custom_migration.psycopg2_bulk_insert_tracking(insert_tem2, self.ddbconn, ['candidate_id', 'parent_id', 'children_id', 'date_data', "index", 'insert_timestamp', 'constraint_id'],
                                                               'configurable_form_group_value', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(update_tem2, self.ddbconn, ['date_data'], ['id'], 'configurable_form_group_value', logger)
        return tem2

    def insert_onboarding_company_details__vat_registered(self, df, logger):
        tem2 = df[['candidate_externalid', 'vat_registered']].dropna()
        tem2['index'] = tem2.groupby('candidate_externalid').cumcount() + 1
        pare_cff = pd.read_sql("select * from configurable_form_field where field_key='13ed2b722787d9ea372c1fd895dcbd37';", self.ddbconn)
        chil_cff = pd.read_sql("select * from configurable_form_field where field_key='dfc43006425fc021a21ba980c6baa4ac';", self.ddbconn)
        parent_id = pare_cff.loc[0].get('id')
        children_id = chil_cff.loc[0].get('id')
        children_constraint_id = chil_cff.loc[0].get('constraint_id')
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'candidate_id'})
        # tem2['candidate_id'] = tem2['id']
        tem2['parent_id'] = parent_id
        tem2['children_id'] = children_id
        tem2['text_data'] = tem2['vat_registered']
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2['constraint_id'] = children_constraint_id

        # check existed
        ext = pd.read_sql("select id, candidate_id, parent_id, children_id, constraint_id, 'existed' as exsited from configurable_form_group_value", self.ddbconn)
        tem2 = tem2.merge(ext, on=['candidate_id', 'parent_id', 'children_id', 'constraint_id'], how='left')
        update_tem2 = tem2.loc[tem2.exsited.notnull()]
        insert_tem2 = tem2.loc[tem2.exsited.isnull()]
        vincere_custom_migration.psycopg2_bulk_insert_tracking(insert_tem2, self.ddbconn, ['candidate_id', 'parent_id', 'children_id', 'text_data', "index", 'insert_timestamp', 'constraint_id'],
                                                               'configurable_form_group_value', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(update_tem2, self.ddbconn, ['text_data'], ['id'], 'configurable_form_group_value', logger)
        return tem2

    def insert_onboarding_company_details__vat_number(self, df, logger):
        tem2 = df[['candidate_externalid', 'vat_number']].dropna()
        tem2['index'] = tem2.groupby('candidate_externalid').cumcount() + 1
        pare_cff = pd.read_sql("select * from configurable_form_field where field_key='13ed2b722787d9ea372c1fd895dcbd37';", self.ddbconn)
        chil_cff = pd.read_sql("select * from configurable_form_field where field_key='4fc4b0615a42fdd8485454fdae0d4b2c';", self.ddbconn)
        parent_id = pare_cff.loc[0].get('id')
        children_id = chil_cff.loc[0].get('id')
        children_constraint_id = chil_cff.loc[0].get('constraint_id')
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'candidate_id'})
        # tem2['candidate_id'] = tem2['id']
        tem2['parent_id'] = parent_id
        tem2['children_id'] = children_id
        tem2['text_data'] = tem2['vat_number']
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2['constraint_id'] = children_constraint_id

        # check existed
        ext = pd.read_sql("select id, candidate_id, parent_id, children_id, constraint_id, 'existed' as exsited from configurable_form_group_value", self.ddbconn)
        tem2 = tem2.merge(ext, on=['candidate_id', 'parent_id', 'children_id', 'constraint_id'], how='left')
        update_tem2 = tem2.loc[tem2.exsited.notnull()]
        insert_tem2 = tem2.loc[tem2.exsited.isnull()]
        vincere_custom_migration.psycopg2_bulk_insert_tracking(insert_tem2, self.ddbconn, ['candidate_id', 'parent_id', 'children_id', 'text_data', "index", 'insert_timestamp', 'constraint_id'],
                                                               'configurable_form_group_value', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(update_tem2, self.ddbconn, ['text_data'], ['id'], 'configurable_form_group_value', logger)
        return tem2

    def insert_onboarding_company_details__company_number(self, df, logger):
        tem2 = df[['candidate_externalid', 'company_number']].dropna()
        tem2['index'] = tem2.groupby('candidate_externalid').cumcount() + 1
        pare_cff = pd.read_sql("select * from configurable_form_field where field_key='13ed2b722787d9ea372c1fd895dcbd37';", self.ddbconn)
        chil_cff = pd.read_sql("select * from configurable_form_field where field_key='c01a411d0719949126a2d7b9b7041fec';", self.ddbconn)
        parent_id = pare_cff.loc[0].get('id')
        children_id = chil_cff.loc[0].get('id')
        children_constraint_id = chil_cff.loc[0].get('constraint_id')
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'candidate_id'})
        # tem2['candidate_id'] = tem2['id']
        tem2['parent_id'] = parent_id
        tem2['children_id'] = children_id
        tem2['text_data'] = tem2['company_number']
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2['constraint_id'] = children_constraint_id

        # check existed
        ext = pd.read_sql("select id, candidate_id, parent_id, children_id, constraint_id, 'existed' as exsited from configurable_form_group_value", self.ddbconn)
        tem2 = tem2.merge(ext, on=['candidate_id', 'parent_id', 'children_id', 'constraint_id'], how='left')
        update_tem2 = tem2.loc[tem2.exsited.notnull()]
        insert_tem2 = tem2.loc[tem2.exsited.isnull()]
        vincere_custom_migration.psycopg2_bulk_insert_tracking(insert_tem2, self.ddbconn, ['candidate_id', 'parent_id', 'children_id', 'text_data', "index", 'insert_timestamp', 'constraint_id'],
                                                               'configurable_form_group_value', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(update_tem2, self.ddbconn, ['text_data'], ['id'], 'configurable_form_group_value', logger)
        return tem2

    def insert_onboarding_company_details__address(self, df, logger):
        tem2 = df[['candidate_externalid', 'address']].dropna()
        tem2['index'] = tem2.groupby('candidate_externalid').cumcount() + 1
        pare_cff = pd.read_sql("select * from configurable_form_field where field_key='13ed2b722787d9ea372c1fd895dcbd37';", self.ddbconn)
        chil_cff = pd.read_sql("select * from configurable_form_field where field_key='906af3e2b0f1eca4528da6ef7baf3541';", self.ddbconn)
        parent_id = pare_cff.loc[0].get('id')
        children_id = chil_cff.loc[0].get('id')
        children_constraint_id = chil_cff.loc[0].get('constraint_id')
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid']).rename(columns={'id': 'candidate_id'})
        # tem2['candidate_id'] = tem2['id']
        tem2['parent_id'] = parent_id
        tem2['children_id'] = children_id
        tem2['text_data'] = tem2['address']
        tem2['insert_timestamp'] = datetime.datetime.now()
        tem2['constraint_id'] = children_constraint_id

        # check existed
        ext = pd.read_sql("select id, candidate_id, parent_id, children_id, constraint_id, 'existed' as exsited from configurable_form_group_value", self.ddbconn)
        tem2 = tem2.merge(ext, on=['candidate_id', 'parent_id', 'children_id', 'constraint_id'], how='left')
        update_tem2 = tem2.loc[tem2.exsited.notnull()]
        insert_tem2 = tem2.loc[tem2.exsited.isnull()]
        vincere_custom_migration.psycopg2_bulk_insert_tracking(insert_tem2, self.ddbconn, ['candidate_id', 'parent_id', 'children_id', 'text_data', "index", 'insert_timestamp', 'constraint_id'],
                                                               'configurable_form_group_value', logger)
        vincere_custom_migration.psycopg2_bulk_update_tracking(update_tem2, self.ddbconn, ['text_data'], ['id'], 'configurable_form_group_value', logger)
        return tem2

    def update_onboarding_choose_country(self, df, logger):
        tem2 = df[['candidate_externalid', 'country_code']].dropna()
        tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['country_specific'] = tem2['country_code']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['country_specific', ], ['id', ], 'candidate', logger)
        return tem2

    def email_subscribe(self, df, logger):
        """
        'subscribed' = 0  # unsubscribe
        'subscribed' = 1  # subscribe
        :param df:
        :param logger:
        :return:
        """
        tem2 = df[['candidate_externalid', 'email', 'subscribed']].dropna().drop_duplicates()
        tem2 = tem2.query("email != ''")
        # tem2 = tem2.merge(self.candidate, on=['candidate_externalid'])
        tem2['email'] = tem2['email'].apply(str.lower)
        tem2 = tem2.drop_duplicates()
        # load subscribe email
        email_subscription = pd.read_sql("select id, lower(email) as email, last_modified_date, insert_timestamp from email_subscription;", self.ddbconn)

        # main process
        tem2 = tem2.merge(email_subscription, on='email', how='left', indicator=True)
        # new_subscribe_email = tem2.query("_merge == 'left_only'")

        # update
        df_update = tem2.query('id.notnull()')
        df_update['id'] = df_update['id'].astype(int)
        df_update['subscribed'] = df_update['subscribed'].astype(int)
        vincere_custom_migration.psycopg2_bulk_update_tracking(df_update, self.ddbconn, ['subscribed', ], ['id', ], 'email_subscription', logger)

        # insert
        df_insert = tem2.query('id.isnull()')
        df_insert['last_modified_date'] = datetime.datetime.now()
        df_insert['insert_timestamp'] = datetime.datetime.now()
        df_insert['subscribed'] = df_insert['subscribed'].astype(int)
        df_insert = df_insert[['email', 'subscribed', 'last_modified_date', 'insert_timestamp']].drop_duplicates()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(df_insert, self.ddbconn, ['email', 'subscribed', 'last_modified_date', 'insert_timestamp'], 'email_subscription', logger)
        return tem2