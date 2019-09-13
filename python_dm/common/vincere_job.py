# -*- coding: UTF-8 -*-
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common
import numpy as np
import pandas as pd
import datetime


class Job(vincere_common.Common):
    def __init__(self, ddbconn):
        if ddbconn:
            self.ddbconn = ddbconn
            self.job = pd.read_sql("select id, external_id as job_externalid from position_description", ddbconn)

        """
        temporary and temp-to-perm are not be used any more
        """
        tem = np.array([[1, 5, 4, 2, 3], list(map(str.lower, ['permanent', 'project consulting', 'temporary', 'contract', 'temp-to-perm']))])
        self.jobtype = pd.DataFrame(tem.transpose(), columns=['position_type', 'desc'])
        self.jobtype['position_type'] = self.jobtype['position_type'].astype(np.int8)

        # contract_length_type: 4: months, 5: years
        tem = np.array([[1,2,3,4, 5], ['hour', 'day', 'week', 'month', 'year']])
        self.contract_length_type = pd.DataFrame(tem.transpose(), columns=['contract_length_type', 'desc'])
        self.contract_length_type['contract_length_type'] = self.contract_length_type['contract_length_type'].astype(np.int8)

        tem = np.array([[1, 2, 3, 4], list(map(str.lower, ['Hourly', 'Daily', 'Weekly', 'Monthly']))])
        self.contract_rate_type = pd.DataFrame(tem.transpose(), columns=['contract_rate_type', 'desc'])

    def update_job_type(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'job_type']]
        assert set(tem2['job_type'].value_counts().keys()) \
            .issubset(set(self.jobtype['desc'].values)), \
            "There are some invalid job types values"
        tem2 = tem2.merge(self.jobtype, left_on='job_type', right_on='desc')

        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['position_type', ], ['id', ], 'position_description', logger)
        return tem2

    def update_job_type_2(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'job_type']]
        tem2.job_type.replace({
            'Permanent': 'permanent',
            'Contract': 'contract',
        }, inplace=True)

        assert set(tem2['job_type'].value_counts().keys()) \
            .issubset(set(self.jobtype['desc'].values)), \
            "There are some invalid job types values"
        tem2 = tem2.merge(self.jobtype, left_on='job_type', right_on='desc')

        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['position_type', ], ['id', ], 'position_description', logger)
        return tem2

    def update_pay_interval_hourly(self, df, logger):
        tem2 = df[['job_externalid']].dropna()
        tem2['contract_rate_type'] = 1
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_type', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_interval_daily(self, df, logger):
        tem2 = df[['job_externalid']].dropna()
        tem2['contract_rate_type'] = 2
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_type', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_interval_weekly(self, df, logger):
        tem2 = df[['job_externalid']].dropna()
        tem2['contract_rate_type'] = 3
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_type', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_interval_monthly(self, df, logger):
        tem2 = df[['job_externalid']].dropna()
        tem2['contract_rate_type'] = 4
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_type', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_interval(self, df, logger):
        tem2 = df[['job_externalid', 'pay_interval']].dropna()
        assert set(tem2['pay_interval'].value_counts().keys()) \
            .issubset(set(self.contract_rate_type['desc'].values)), \
            "There are some invalid pay interval values"

        tem2 = tem2.merge(self.contract_rate_type, left_on='pay_interval', right_on='desc')
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        tem2.contract_rate_type = tem2.contract_rate_type.astype(int)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_type', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_interval_2(self, df, logger):
        tem2 = df[['job_externalid', 'pay_interval']].dropna()
        tem2.loc[tem2['pay_interval'].str.contains('NotSpecified', False), 'pay_interval'] = 'monthly'
        tem2.loc[tem2['pay_interval'].str.contains('year', False), 'pay_interval'] = 'monthly' # Vincere does not support yearly payment
        tem2.loc[tem2['pay_interval'].str.contains('month', False), 'pay_interval'] = 'monthly'
        tem2.loc[tem2['pay_interval'].str.contains('week', False), 'pay_interval'] = 'weekly'
        tem2.loc[tem2['pay_interval'].str.contains('day', False), 'pay_interval'] = 'daily'
        tem2.loc[tem2['pay_interval'].str.contains('hour', False), 'pay_interval'] = 'hourly'

        assert set(tem2['pay_interval'].value_counts().keys()) \
            .issubset(set(self.contract_rate_type['desc'].values)), \
            "There are some invalid pay interval values %s <--> %s " % ((', '.join(set(tem2['pay_interval'].value_counts().keys()))), (', '.join(set(self.contract_rate_type['desc'].values))))

        tem2 = tem2.merge(self.contract_rate_type, left_on='pay_interval', right_on='desc')
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        tem2.contract_rate_type = tem2.contract_rate_type.astype(int)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_type', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_compensation_markup_percent(self, df, logger):
        tem2 = df[['job_externalid', 'markup_percent']].dropna()
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['markup_percent', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_reg_date(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'reg_date']]
        tem2['insert_timestamp'] = tem2['reg_date']
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['insert_timestamp'], ['id'], 'position_description', logger)
        return tem2

    def update_internal_description(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'internal_description']]
        tem2['full_description'] = tem2['internal_description']
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['full_description', ], ['id', ], 'position_description', logger)
        return tem2

    def update_public_description(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'public_description']]
        # tem2['public_description'] = tem2['public_description']
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['public_description', ], ['id', ], 'position_description', logger)
        return tem2

    def update_public_description2(self, df, conn_param, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'public_description']]
        # tem2['public_description'] = tem2['public_description']
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'position_description', ['public_description'], ['id'], logger)
        return tem2

    def update_currency_type(self, df, logger):
        """
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'currency_type']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['currency_type', ], ['id', ], 'position_description', logger)
        tem2['position_id'] = tem2.id
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['currency_type', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_start_date(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'start_date']].dropna()
        tem2['head_count_open_date'] = tem2['start_date']
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['head_count_open_date', ], ['id', ], 'position_description', logger)
        return tem2

    def update_close_date(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'close_date']].dropna()
        tem2['head_count_close_date'] = tem2['close_date']
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['head_count_close_date', ], ['id', ], 'position_description', logger)
        return tem2

    def update_salary_from(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'salary_from']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        tem2['annual_salary_from'] = tem2['salary_from']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['annual_salary_from', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_use_quick_fee_forecast(self, df, logger):
        """
        use_quick_fee_forecast: 1/0 = used/notused
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'use_quick_fee_forecast']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['use_quick_fee_forecast', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_percentage_of_annual_salary(self, df, logger):
        """
        use_quick_fee_forecast: 1/0 = used/notused
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'percentage_of_annual_salary']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['percentage_of_annual_salary', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_salary_to(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'salary_to']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        tem2['annual_salary_to'] = tem2['salary_to']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['annual_salary_to', ], ['position_id', ], 'compensation', logger)
        return tem2

    def update_actual_salary(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'actual_salary']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        tem2['gross_annual_salary'] = tem2['actual_salary']
        tem2['pay_rate'] = tem2['actual_salary']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['gross_annual_salary', 'pay_rate'], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_rate(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'pay_rate']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['pay_rate'], ['position_id', ], 'compensation', logger)
        return tem2

    def update_profit(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'profit']]
        tem2['projected_profit'] = tem2.profit
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['profit', 'projected_profit'], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_rate_from(self, df, logger):
        """
        only show in the compensation tab when the job is temporary
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'pay_rate_from']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        tem2['contract_rate_from'] = tem2['pay_rate_from']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_from'], ['position_id', ], 'compensation', logger)
        return tem2

    def update_pay_rate_to(self, df, logger):
        """
        only show in the compensation tab when the job is temporary
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'pay_rate_to']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        tem2['contract_rate_to'] = tem2['pay_rate_to']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_rate_to'], ['position_id', ], 'compensation', logger)
        return tem2

    def update_charge_rate(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'charge_rate']]
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['charge_rate'], ['position_id', ], 'compensation', logger)
        return tem2


    def update_contract_length(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'contract_length', 'contract_length_type']]
        try:
            assert set(tem2['contract_length_type'].value_counts().keys()).issubset(set(self.contract_length_type['desc'])), 'Client have some contract length_video type values cannot be mapped.'
        except AssertionError as ae:
            logger.info(ae)
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2 = tem2.merge(self.contract_length_type, left_on='contract_length_type', right_on='desc', suffixes=['_x', ''])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_length', 'contract_length_type'], ['position_id', ], 'compensation', logger)
        return tem2

    def update_contract_length_2(self, df, logger):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :return:
        """
        tem2 = df[['job_externalid', 'contract_length', 'contract_length_type']]
        tem2.contract_length_type.replace({
            'Hours': 'hour',
            'Days': 'day',
            'Weeks': 'week',
            'Months': 'month',
            'Years': 'year',
        }, inplace=True)
        try:
            assert set(tem2['contract_length_type'].value_counts().keys()).issubset(set(self.contract_length_type['desc'])), 'Client have some contract length_video type values cannot be mapped.'
        except AssertionError as ae:
            logger.info(ae)
        # transform data
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2 = tem2.merge(self.contract_length_type, left_on='contract_length_type', right_on='desc', suffixes=['_x', ''])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['contract_length', 'contract_length_type'], ['position_id', ], 'compensation', logger)
        return tem2

    def set_job_location_by_company_location(self, logger):
        """
        if a company have serveral locations, the function will pick the first location and assign to contacts
        :param logger:
        :return:
        """
        # load data
        company_location = pd.read_sql('select id as company_location_id, company_id from company_location;', self.ddbconn)
        job = pd.read_sql('select company_id, id from position_description where company_location_id is null', self.ddbconn)

        # find companies have 1 location
        company_location['rn'] = company_location.groupby('company_id').cumcount()
        company_location = company_location.query("rn == 0")

        # companies_has_one_loc = company_location.groupby('company_id').count().query("id==1")
        companies_has_one_loc = company_location  # get the first location
        companies_has_one_loc.reset_index(level=0, inplace=True)

        # find jobs belong to companies have one location
        job_of_companies_have_one_loc = job.merge(companies_has_one_loc, on='company_id')

        vincere_custom_migration.psycopg2_bulk_update_tracking(job_of_companies_have_one_loc, self.ddbconn, ['company_location_id'], ['id'], 'position_description', logger)
        return job_of_companies_have_one_loc

    def update_note(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['job_externalid', 'note']]
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['note', ], ['id', ], 'position_description', logger)
        vincere_custom_migration.execute_sql_update(r"update position_description set note=replace(note, '\n', chr(10)) where note is not null;", self.ddbconn)
        return tem2

    def update_head_count(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['job_externalid', 'head_count']]
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['head_count', ], ['id', ], 'position_description', logger)
        return tem2

    def update_key_words(self, df, logger):
        """
        key_words
        :rtype: object
        """
        tem2 = df[['job_externalid', 'key_words']].groupby('job_externalid')['key_words'].apply(', '.join).reset_index()
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['key_words', ], ['id', ], 'position_description', logger)
        return tem2

    def update_default_currency(self, currency, logger):
        """
        set default currency for job and offer (for ones have not had currency values)
        :rtype: object
        """
        vincere_custom_migration.execute_sql_update(r"update position_description set currency_type='{}' where currency_type is null;".format(currency), self.ddbconn)
        vincere_custom_migration.execute_sql_update(r"update offer set currency_type='{}' where currency_type is null;".format(currency), self.ddbconn)
        vincere_custom_migration.execute_sql_update(r"""
            update compensation set currency_type = data.val
            from (
                    select pd.currency_type, c.id
                    from compensation c
                    join position_description pd on c.position_id = pd.id
                 ) as data (val, id)
            where compensation.id = data.id;
        """.format(currency), self.ddbconn)

    def insert_fe_sfe(self, df, ddbconn, logger):
        tem2 = ['functional_expertise_id', 'job_externalid', 'sub_functional_expertise_id']
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        vincere_custom_migration.execute_sql_update("delete from position_description_functional_expertise", ddbconn)
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, ddbconn, ['functional_expertise_id', 'position_id', 'sub_functional_expertise_id'], 'position_description_functional_expertise', logger)

    def insert_fe_sfe2(self, df, logger):
        tem2 = df[['job_externalid', 'fe', 'sfe']]

        tem2 = tem2.merge(pd.read_sql('select id as functional_expertise_id, name as fe from functional_expertise', self.ddbconn), on='fe', how='left')
        tem2 = tem2.merge(pd.read_sql('select functional_expertise_id, id as sub_functional_expertise_id, name as sfe from sub_functional_expertise', self.ddbconn), on=['functional_expertise_id', 'sfe'], how='left')
        tem2 = tem2.where(tem2.notnull(), None)
        tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'] = tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'].astype(int)

        tem2 = tem2.merge(self.job, on=['job_externalid'])
        tem2['position_id'] = tem2['id']
        # tem2['insert_timestamp'] = datetime.datetime.now()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['functional_expertise_id', 'position_id', 'sub_functional_expertise_id'], 'position_description_functional_expertise', logger)
        return tem2

    def insert_owner(self, df, logger):
        tem = df[['job_externalid', 'email']]
        tem = tem.merge(pd.read_sql("select id as user_id, email from user_account", self.ddbconn), on='email')
        tem = tem.merge(self.job, on='job_externalid')
        tem['position_id'] = tem['id']
        tem['insert_timestamp'] = datetime.datetime.now()
        tem = tem.merge(pd.read_sql("select position_id, user_id, 'has_owner' as note from position_agency_consultant", self.ddbconn), on=['position_id', 'user_id'], how='left')
        tem = tem.query("note.isnull()")
        # assert 1==2
        # khong can delete: chay lai lan 2, loc theo dk note.isnull() luc nay khong co dong nao can insert lai
        # vincere_custom_migration.execute_sql_update("delete from position_agency_consultant where insert_timestamp='{}'".format(vincere_common.my_insert_timestamp))
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem, self.ddbconn, ['position_id', 'user_id', 'insert_timestamp'], 'position_agency_consultant', logger)
        return tem

    def process_vincere_job(self, df, logger):
        if 'position-employmentType' in df.columns:
            df['position-employmentType'] = df['position-employmentType'].map(lambda x: self.set_position_employment_type(x))
        if 'position-currency' in df.columns:
            df['position-currency'] = df['position-currency'].map(lambda x: self.map_currency_code(x))
        if 'position-type' in df.columns:
            df['position-type'] = df['position-type'].map(lambda x: self.set_position_type(x))
        if 'position-endDate' in df.columns:
            df['position-endDate'] = df['position-endDate'].dt.date
        if 'position-startDate' in df.columns:
            df['position-startDate'] = df['position-startDate'].dt.date
        if 'position-actualSalary' in df.columns:
            df['position-actualSalary'].fillna(0, inplace=True)
            df['position-actualSalary'] = df['position-actualSalary'].astype(np.int64)
        if 'position-title' in df.columns:
            # process prosition title duplicated
            df['position-title'] = df['position-title'].str.strip()
            df['position-title'] = df.apply(lambda x: '[NO_JOB_TITLE] %s' % x['position-externalId'] if (str(x['position-title']) == '') or (x['position-title'] == None) else x['position-title'], axis=1)
            # df['position-checkdup'] = [str(x['position-title']).lower() + str(x['position-startDate']) for index, x in df.iterrows()]
            # df['position-title-rn'] = df.groupby(df['position-checkdup']).cumcount()+1
            # df['position-title'] = ["%s_%i" % (x['position-title'], x['position-title-rn']) if x['position-title-rn']>1 else x['position-title'] for index, x in df.iterrows()]
            df['position-checkdup'] = [str(x['position-title']).lower() for index, x in df.iterrows()]
            df['position-title-rn'] = df.groupby(df['position-checkdup']).cumcount() + 1
            df['position-title'] = ["%s_%s" % (x['position-title'], x['position-externalId']) if x['position-title-rn'] > 1 else x['position-title'] for index, x in df.iterrows()]
        return df.filter(regex='^position')

    def insert_job_industry(self, df, logger):
        tem2 = df[['job_externalid', 'name']]
        tem2 = tem2.merge(pd.read_sql('select * from vertical', self.ddbconn), on='name')
        tem2.rename(columns={'id': 'vertical_id', }, inplace=True)
        tem2 = tem2.merge(self.job, on=['job_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['vertical_id', ], ['id', ], 'position_description', logger)
        return tem2

    def update_job_industry_inhouse(self, df, logger):
        tem2 = df[['job_id', 'name']]
        tem2 = tem2.merge(pd.read_sql('select * from vertical', self.ddbconn), on='name')
        tem2.rename(columns={'id': 'vertical_id', }, inplace=True)
        tem2.rename(columns={'job_id': 'id', }, inplace=True)
        tem2 = tem2.merge(pd.read_sql("select vertical_id, id, 'existed' as note from position_description where vertical_id is not null;", self.ddbconn), on=['id', 'vertical_id'], how='left')
        tem2 = tem2.loc[tem2['note'].isnull()]
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['vertical_id', ], ['id', ], 'position_description', logger)
        return tem2

