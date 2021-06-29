# -*- coding: UTF-8 -*-
import common.vincere_custom_migration as vincere_custom_migration
import numpy as np
import pandas as pd
import datetime
import common.vincere_job as vincere_job


class PlacementDetail(vincere_job.Job):
    def __init__(self, ddbconn):
        super().__init__(ddbconn)

        self.ddbconn = ddbconn
        # self.offer = pd.read_sql("""
        #                 select
        #                    o.id
        #                    , pd.external_id as job_externalid
        #                    , c.external_id as candidate_externalid
        #                 from offer o
        #                 join position_candidate pc on o.position_candidate_id = pc.id
        #                 join candidate c on pc.candidate_id = c.id
        #                 join position_description pd on pc.position_description_id = pd.id
        #                 """, ddbconn)

        self.user = pd.read_sql("select id as user_id, email as user_email from user_account", ddbconn)
        self.position_candidate = pd.read_sql("""
        select
          pc.id as position_cadidate_id
        , pc.position_description_id
        , pc.candidate_id
        , opi.id as offer_personal_info_id
        , o.id as offer_id
        , pd.external_id as job_externalid
        , c.external_id as candidate_externalid
        from position_candidate pc
        left join offer o on pc.id = o.position_candidate_id
        left join offer_personal_info opi on o.id = opi.offer_id
        left join position_description pd on pc.position_description_id = pd.id
        left join candidate c on pc.candidate_id = c.id
        ;
        """, ddbconn)

        self.position_candidate_not_null_id = pd.read_sql("""
                select
                  pc.id as position_cadidate_id
                , pc.position_description_id
                , pc.candidate_id
                , opi.id as offer_personal_info_id
                , o.id as offer_id
                , pd.external_id as job_externalid
                , c.external_id as candidate_externalid
                from position_candidate pc
                join offer o on pc.id = o.position_candidate_id
                join offer_personal_info opi on o.id = opi.offer_id
                join position_description pd on pc.position_description_id = pd.id
                join candidate c on pc.candidate_id = c.id
                ;
                """, ddbconn)

        tem = np.array([[1, 2, 3, 4, 5], list(map(str.lower, ['Hourly', 'Daily', 'Weekly', 'Monthly','Annual']))])
        self.contract_rate_type = pd.DataFrame(tem.transpose(), columns=['contract_rate_type', 'desc'])
        self.contract_rate_type['contract_rate_type'] = self.contract_rate_type['contract_rate_type'].astype(np.int8)

        tem = np.array([['profit', 'margin', 'markup', 'chargeRate'], ['Profit | Margin', 'Margin', 'Markup', 'Charge rate']])
        self.calculate_charge_using = pd.DataFrame(tem.transpose(), columns=['calculate_charge_using', 'desc'])

    def update_startdate_enddate(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'start_date', 'end_date']]
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['start_date', 'end_date', ], ['id', ], 'offer_personal_info', logger)
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['start_date', 'end_date', ], ['id', ], 'position_candidate', logger)
        return tem2

    def update_invoice_date(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'invoice_date']]
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['invoice_date', ], ['id', ], 'offer_personal_info', logger)
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['start_date', 'end_date', ], ['id', ], 'position_candidate', logger)
        return tem2

    def update_invoice_note(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'invoice_message']]
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['invoice_message', ], ['id', ], 'offer_personal_info', logger)
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['start_date', 'end_date', ], ['id', ], 'position_candidate', logger)
        return tem2

    def insert_sub_status(self, df, logger):
        tem2 = df[['name']].dropna().drop_duplicates()
        tem2['color_code'] = '#dbf6b9'
        tem2['creator_id'] = -10
        tem2['insert_timestamp'] = datetime.datetime.now()

        existed_sub_status = pd.read_sql("select name, id from sub_status", self.ddbconn)
        tem2 = tem2.merge(existed_sub_status, on='name', how='left')
        tem2 = tem2.query("id.isnull()")
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['name', 'color_code', 'creator_id', 'insert_timestamp'], 'sub_status', logger)
        return tem2

    def update_startdate_only_for_placement_detail(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'start_date']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['start_date', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_default_enddate_by_startdate_for_contract_jobs(self, logger):
        """
        Andi: 4.
        Why the End date of All CONTRACTS jobs are empty ? No contract jobs should have no end dates.
        They should all be filled with end date. If blank in the original system, they should be assumed 1 year in the future from the date of the placement.
        :param df:
        :param logger:
        :return:
        """

        # set default end date for contract job
        vincere_custom_migration.execute_sql_update("""
        update offer_personal_info set start_date=data.start_date, end_date=data.end_date
            from (
           select id, start_date, placed_date, placed_date + interval '1 year' as end_date
           from offer_personal_info
           where start_date is not null
             and placed_date is not null
             and end_date is null
             and offer_id in (select id from offer where position_type = 2)
         ) data where offer_personal_info.id=data.id
        """, self.ddbconn)

        # set default contract length for contract job
        vincere_custom_migration.execute_sql_update("""
        update offer set contract_length = data.contract_length, contract_length_type = data.contract_length_type
        from (
               select o.id, extract(month from age(opi.end_date, opi.start_date)) as contract_length, 4 as contract_length_type, opi.start_date, opi.end_date
               from offer o
                      join offer_personal_info opi on o.id = opi.offer_id
               where o.position_type = 2 -- contract
                 and start_date is not null
                 and end_date is not null
                 and contract_length is null
             ) data where offer.id=data.id
        """, self.ddbconn)

    def update_placeddate(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['job_externalid', 'candidate_externalid', 'placed_date']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['hire_date'] = tem2['placed_date']
        tem2['id'] = tem2['position_cadidate_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['placed_date', 'hire_date'], ['id', ], 'position_candidate', logger)
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['placed_date', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_sub_status(self, df, logger):
        """
        :rtype: object
        """
        tem2 = df[['job_externalid', 'candidate_externalid', 'sub_status_name']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['position_cadidate_id']
        existed_sub_status = pd.read_sql("select name as sub_status_name, id as sub_status_id from sub_status", self.ddbconn)
        tem2 = tem2.merge(existed_sub_status, on='sub_status_name')
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['sub_status_id'], ['id', ], 'position_candidate', logger)
        return tem2

    def update_sent_date(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'sent_date']].dropna()
        tem2 = tem2.merge(pd.read_sql("""
            select
                  pc.id as position_cadidate_id
                , pc.position_description_id
                , pc.candidate_id
                , pd.external_id as job_externalid
                , c.external_id as candidate_externalid
                from position_candidate pc
                join position_description pd on pc.position_description_id = pd.id
                join candidate c on pc.candidate_id = c.id
        """, self.ddbconn), on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['position_cadidate_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['sent_date'], ['id', ], 'position_candidate', logger)
        return tem2

    def update_offerdate(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'offer_date']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['offer_date', ], ['id', ], 'offer_personal_info', logger)
        tem2['id'] = tem2['position_cadidate_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['offer_date', ], ['id', ], 'position_candidate', logger)
        return tem2

    def update_contact_name_billing(self, df, ddbconn, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'client_contact_name', 'client_contact_id']]
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['client_contact_name', 'client_contact_id'], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_contact_name_billing_2(self, df, ddbconn, logger):
        tem = df[['job_externalid', 'candidate_externalid', 'client_contact_name', 'client_contact_id']]
        tem['client_contact_id'] = list(map(str, map(int, tem['client_contact_id'])))
        tem = tem.merge(pd.read_sql('select external_id, first_name, last_name, email, id as vc_client_contact_id from contact', ddbconn), left_on='client_contact_id', right_on='external_id')
        tem['client_contact_id'] = tem['vc_client_contact_id']
        return self.update_contact_name_billing(tem, ddbconn, logger)

    def update_contact_email_billing(self, df, ddbconn, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'client_contact_email']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['client_contact_email', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_contact_phone_billing(self, df, ddbconn, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'client_contact_phone']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['client_contact_phone', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_purchase_order_number(self, df, ddbconn, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'client_purchase_order']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['client_purchase_order', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_department(self, df, ddbconn, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'client_department']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['client_department', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_trading_name(self, df, ddbconn, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'client_trading_name']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['client_trading_name', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_company_number(self, df, ddbconn, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'company_number']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, ddbconn, ['company_number', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def update_notice_period(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'notice_period']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['notice_period', ], ['id', ], 'offer_personal_info', logger)
        return tem2

    def insert_allowances_and_optional_factors__optional_factors(self, df, ddbconn, logger, **kwargs):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :param kwargs: override=True/False
        :return:
        """
        tem2 = df[['job_externalid', 'candidate_externalid', 'amount', 'name']]
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['type'] = 1
        cols = ['offer_id', 'name', 'amount', 'type']
        for k, v in kwargs.items():
            if k == 'override':
                if v:
                    vincere_custom_migration.execute_sql_update("delete from offer_other_cost", ddbconn)
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, ddbconn, cols, 'offer_other_cost', logger)
        return tem2

    def insert_profit_split_mode_numeric(self, df, ddbconn, logger, **kwargs):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :param kwargs: override=True/False; mode=numeric/percentage
        :return:
        """
        # tem2 = df[['job_externalid', 'candidate_externalid', 'user_email', 'amount']]
        tem2 = df[df.columns]
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2 = tem2.merge(self.user, on='user_email')
        tem2['profit_split_mode'] = 2
        tem2['shared'] = 0
        cols = ['offer_id', 'user_id', 'shared', 'amount', 'profit_split_mode']
        for k, v in kwargs.items():
            if k == 'override':
                if v:
                    vincere_custom_migration.execute_sql_update("delete from offer_revenue_split where profit_split_mode = 2", ddbconn)
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, ddbconn, cols, 'offer_revenue_split', logger)
        return tem2

    def insert_profit_split_mode_percentage(self, df, logger, **kwargs):
        """
        :param df:
        :param ddbconn:
        :param logger:
        :param kwargs: override=True/False; mode=numeric/percentage
        :return:
        """
        tem2 = df[['job_externalid', 'candidate_externalid', 'user_email', 'shared']]
        tem2 = df[df.columns]
        tem2 = tem2.merge(self.position_candidate_not_null_id, on=['job_externalid', 'candidate_externalid'])
        tem2 = tem2.merge(self.user, on='user_email')
        tem2['profit_split_mode'] = 1
        tem2['amount'] = 0
        ts = datetime.datetime.now()
        tem2['insert_timestamp'] = ts
        cols = ['offer_id', 'user_id', 'shared', 'amount', 'profit_split_mode', 'insert_timestamp']
        for k, v in kwargs.items():
            if k == 'override':
                if v:
                    vincere_custom_migration.execute_sql_update("delete from offer_revenue_split where profit_split_mode = 1", self.ddbconn)
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, cols, 'offer_revenue_split', logger)

        # update profit split amount
        newoffrs = pd.read_sql("""
        select 
            offrs.id
            , offrs.insert_timestamp
            , offrs.shared * offr.profit / 100 as amount
        from offer_revenue_split offrs
        join offer offr on offrs.offer_id = offr.id
        ;
        """, self.ddbconn)
        newoffrs = newoffrs.loc[newoffrs['insert_timestamp'] == ts]
        vincere_custom_migration.psycopg2_bulk_update_tracking(newoffrs, self.ddbconn, ['amount'], ['id'], 'offer_revenue_split', logger)
        return tem2

    def update_internal_note(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'note']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['note', ], ['id', ], 'offer_personal_info', logger)
        vincere_custom_migration.execute_sql_update(r"update offer_personal_info set note=replace(note, '\n', chr(10)) where note is not null;", self.ddbconn)
        return tem2

    def update_placementtype_or_jobtype(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['job_externalid', 'candidate_externalid', 'placement_type']]
        assert set(tem2['placement_type'].value_counts().keys()) \
            .issubset(set(self.jobtype['desc'].values)), \
            "There are some invalid job types values"
        tem2 = tem2.merge(self.jobtype, left_on='placement_type', right_on='desc')

        # transform data
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'])
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['position_type', ], ['id', ], 'offer', logger)
        return tem2

    def update_pay_interval(self, df, logger):
        tem2 = df[['job_externalid', 'candidate_externalid', 'pay_interval']].dropna()
        assert set(tem2['pay_interval'].value_counts().keys()) \
            .issubset(set(self.contract_rate_type['desc'].values)), \
            "There are some invalid pay interval values"

        tem2 = tem2.merge(self.contract_rate_type, left_on='pay_interval', right_on='desc')

        # transform data
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['contract_rate_type', ], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_margin_percent(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'margin_percent']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['margin_percent', ], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_markup_percent(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'markup_percent']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['markup_percent', ], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_offer_annual_salary(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'annual_salary']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['gross_annual_salary'] = tem2['annual_salary']
        tem2['pay_rate'] = tem2['annual_salary']
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['gross_annual_salary', 'pay_rate'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_profit(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'profit']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['profit',], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_offer_employment_type_FULLTIME(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        tem2['employment_type'] = 0
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['employment_type'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_offer_currency_type(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'currency_type']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['currency_type'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_use_quick_fee_forecast(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', ]].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['use_quick_fee_forecast'] = 1
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['use_quick_fee_forecast'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_percentage_of_annual_salary(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'percentage_of_annual_salary']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        updating_off = tem2.query("_merge=='both'")
        vincere_custom_migration.psycopg2_bulk_update_tracking(updating_off, self.ddbconn, ['percentage_of_annual_salary'], ['id', ], 'offer', logger)

        # update profit
        off = pd.read_sql("""
            select 
                id
                , percentage_of_annual_salary*gross_annual_salary/100 as profit
                , percentage_of_annual_salary*gross_annual_salary/100 as projected_profit
            from offer
        """, self.ddbconn)

        off = off.loc[off['id'].isin(updating_off['id'])]
        vincere_custom_migration.psycopg2_bulk_update_tracking(off, self.ddbconn, ['profit', 'projected_profit'], ['id', ], 'offer', logger)

        return updating_off, off

    def update_use_quick_fee_forecast_for_permanent_job(self):
        vincere_custom_migration.execute_sql_update("update offer set use_quick_fee_forecast=1 where position_type=1;", self.ddbconn)

    def update_contract_length(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'contract_length']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['contract_length'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_contract_length_type(self, df, type, logger):
        contract_length_type = {
            'day': 2, 'week': 3, 'month': 4, 'year': 5
        }
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid',]]
        if type not in ['day', 'week', 'month', 'year']:
            raise Exception
        tem2['contract_length_type'] = contract_length_type.get(type)
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['contract_length_type'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_charge_rate(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'candidate_externalid', 'charge_rate']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['charge_rate_type'] = 'chargeRate'
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['charge_rate_type', 'charge_rate'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_salary_type(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """""
        #Monthly 2 Annual 1
        # prepare position type values

        tem2 = df[['job_externalid', 'candidate_externalid', 'salary_type']].dropna()
        # transform data
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['salary_type'],['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_salary_monthly(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """""
        #Monthly 2 Annual 1
        # prepare position type values

        tem2 = df[['job_externalid', 'candidate_externalid', 'present_salary_rate']]
        # transform data
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer',
                          indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn,['present_salary_rate'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_pay_rate(self, df, logger):
        # (Base Pay Rate)
        # get offer ids need to be updated
        #
        tem2 = df[['job_externalid', 'candidate_externalid', 'pay_rate']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['pay_rate'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_hire_date_position_candidate(self, df, logger):
        # (Base Pay Rate)
        # get offer ids need to be updated
        #
        tem2 = df[['job_externalid', 'candidate_externalid', 'hire_date']].dropna()
        tem2 = tem2.merge(self.position_candidate, on=['job_externalid', 'candidate_externalid'], how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['hire_date'], ['id', ], 'position_candidate', logger)
        return tem2.query("_merge=='both'")




    def update_jobtype_job_only(self, df, logger):
        """
        id: offer.id
        :param df: columns job_externalid, candidate_externalid, placement_type are must have!!!
        :param ddbconn:
        :param logger:
        :return:
        """

        tem2 = df[['job_externalid', 'job_type', 'perm_sub_type']]
        assert set(tem2['job_type'].value_counts().keys()) \
            .issubset(set(self.jobtype['desc'].values)), \
            "There are some invalid job types values"

        assert set(tem2['perm_sub_type'].value_counts().keys()) \
            .issubset(set(self.perm_sub_type['desc'].values)), \
            "There are some invalid job types values"

        tem2 = tem2.merge(self.jobtype, left_on='job_type', right_on='desc')
        tem2 = tem2.merge(self.perm_sub_type, left_on='perm_sub_type', right_on='desc')

        # transform data
        # tem2 = tem2.merge(self.job, on=['job_externalid'])
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn,['position_type', 'position_sub_type'], ['id', ],'position_description', logger)
        # return tem2

        # prepare position type values

        # tem2 = df[['job_externalid', 'candidate_externalid', 'placement_type']]
        # assert set(tem2['placement_type'].value_counts().keys()) \
        #     .issubset(set(self.jobtype['desc'].values)), \
        #     "There are some invalid job types values"
        # tem2 = tem2.merge(self.jobtype, left_on='placement_type', right_on='desc')

        # transform data
        tem2 = tem2.merge(self.position_candidate, on='job_externalid')
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['position_type', 'position_sub_type' ], ['id', ], 'offer', logger)
        return tem2

    def update_offer_annual_salary_jobonly(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'annual_salary']].dropna()
        tem2 = tem2.merge(self.position_candidate, on='job_externalid', how='outer', indicator=True)
        tem2['gross_annual_salary'] = tem2['annual_salary']
        tem2['pay_rate'] = tem2['annual_salary']
        tem2['id'] = tem2['offer_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2.query("_merge=='both'"), self.ddbconn, ['gross_annual_salary', 'pay_rate'], ['id', ], 'offer', logger)
        return tem2.query("_merge=='both'")

    def update_percentage_of_annual_salary_jobonly(self, df, logger):
        # get offer ids need to be updated
        tem2 = df[['job_externalid', 'percentage_of_annual_salary']].dropna()
        tem2 = tem2.merge(self.position_candidate, on='job_externalid', how='outer', indicator=True)
        tem2['id'] = tem2['offer_id']
        updating_off = tem2.query("_merge=='both'")
        vincere_custom_migration.psycopg2_bulk_update_tracking(updating_off, self.ddbconn, ['percentage_of_annual_salary'], ['id', ], 'offer', logger)

        # update profit
        off = pd.read_sql("""
            select 
                id
                , percentage_of_annual_salary*gross_annual_salary/100 as profit
                , percentage_of_annual_salary*gross_annual_salary/100 as projected_profit
            from offer
        """, self.ddbconn)

        off = off.loc[off['id'].isin(updating_off['id'])]
        vincere_custom_migration.psycopg2_bulk_update_tracking(off, self.ddbconn, ['profit', 'projected_profit'], ['id', ], 'offer', logger)

        return updating_off, off

    def update_startdate_only_for_placement_detail_jobonly(self, df, logger):
        tem2 = df[['job_externalid', 'start_date']].dropna()
        tem2 = tem2.merge(self.position_candidate, on='job_externalid')
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['start_date', ], ['id', ], 'offer_personal_info', logger)
        return tem2


    def update_placeddate_jobonly(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['job_externalid', 'placed_date']].dropna()
        tem2 = tem2.merge(self.position_candidate, on='job_externalid')
        tem2['hire_date'] = tem2['placed_date']
        tem2['id'] = tem2['position_cadidate_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['placed_date', 'hire_date'], ['id', ], 'position_candidate', logger)
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['placed_date', ], ['id', ], 'offer_personal_info', logger)
        return tem2


    def update_offerdate_jobonly(self, df, logger):
        tem2 = df[['job_externalid', 'offer_date']].dropna()
        tem2 = tem2.merge(self.position_candidate, on='job_externalid')
        tem2['id'] = tem2['offer_personal_info_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['offer_date', ], ['id', ], 'offer_personal_info', logger)
        tem2['id'] = tem2['position_cadidate_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['offer_date', ], ['id', ], 'position_candidate', logger)
        return tem2