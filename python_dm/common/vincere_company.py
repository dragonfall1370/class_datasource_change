# -*- coding: UTF-8 -*-
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common
import numpy as np
import pandas as pd
import datetime
import re


class Company(vincere_common.Common):
    def __init__(self, ddbconn):
        if ddbconn:
            self.ddbconn = ddbconn
            self.company = pd.read_sql("select id, external_id as company_externalid, name as company_name, company_owners from company", ddbconn)
            self.company_location = pd.read_sql("""
            select
            c.external_id as company_externalid
            , c.id as company_id
            , cl.id
            -- , cl.address
            from company c
            join company_location cl on c.id = cl.company_id;
            """, self.ddbconn)
            # get first address only
            self.company_location['rn'] = self.company_location.groupby(['company_externalid', 'company_id']).cumcount()
            self.company_location = self.company_location.loc[self.company_location['rn']==0]

    def get_company_location(self):
        tem2 = pd.read_sql("""
        select
            c.external_id as company_externalid
            , c.id as company_id
            , cl.id as company_location_id
            , c."name" as company_name
            , cl.address
            , cl.district
            , cl.city
            , cl.state
            , cl.country
            , cl.latitude
            , cl.longitude
            , cl.post_code
            , cl.location_type
            from company c
            join company_location cl on c.id = cl.company_id;
        """, self.ddbconn)
        return tem2

    def insert_company_location(self, company_loc, logger, allow_dup = False):
        tem2 = company_loc[['address', 'company_externalid']].drop_duplicates()

        tem2.address = tem2.address.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        tem2.address = tem2.address.map(lambda x: ', '.join([e.strip() for e in x if e]))

        tem2 = tem2.drop_duplicates()

        tem2 = tem2.merge(self.company, on=['company_externalid'])
        tem2.rename(columns={'id': 'company_id'}, inplace=True)
        tem2 = tem2.where(tem2.notnull(), None)
        if not allow_dup:
            existed_add = pd.read_sql("select address, company_id, id from company_location", self.ddbconn)
            existed_add = existed_add.where(existed_add.notnull(), None)

            tem2 = tem2.merge(existed_add, on=['address', 'company_id'], how='left')
            tem2 = tem2.query("id.isnull()")

        if tem2.shape[0]>0:
            tem2['insert_timestamp'] = datetime.datetime.now()
            tem2['location_name'] = tem2['address']
            vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['location_name', 'address', 'insert_timestamp', 'company_id'], 'company_location', logger)
            return tem2

    def insert_company_location_2(self, company_loc, conn_param, logger, allow_dup = False):
        tem2 = company_loc[['address', 'company_externalid']].drop_duplicates()

        tem2.address = tem2.address.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        tem2.address = tem2.address.map(lambda x: ', '.join([e.strip() for e in x if e]))

        tem2 = tem2.drop_duplicates()

        tem2 = tem2.merge(self.company, on=['company_externalid'])
        tem2.rename(columns={'id': 'company_id'}, inplace=True)
        tem2 = tem2.where(tem2.notnull(), None)
        if not allow_dup:
            existed_add = pd.read_sql("select address, company_id, id from company_location", self.ddbconn)
            existed_add = existed_add.where(existed_add.notnull(), None)

            tem2 = tem2.merge(existed_add, on=['address', 'company_id'], how='left')
            tem2 = tem2.query("id.isnull()")

        if tem2.shape[0]>0:
            tem2['insert_timestamp'] = datetime.datetime.now()
            tem2['location_name'] = tem2['address']
            # vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['location_name', 'address', 'insert_timestamp', 'company_id'], 'company_location', logger)
            vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'insert', 'company_location', ['location_name', 'address', 'insert_timestamp', 'company_id'], [], logger)
            return tem2

    def insert_company_mailling_address(self, company_loc, logger, allow_dup = False):
        tem2 = company_loc[['address', 'company_externalid']].drop_duplicates()
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        tem2.rename(columns={'id': 'company_id'}, inplace=True)
        tem2 = tem2.where(tem2.notnull(), None)
        if not allow_dup:
            existed_add = pd.read_sql("select address, company_id, id from company_location", self.ddbconn)
            existed_add = existed_add.where(existed_add.notnull(), None)

            tem2 = tem2.merge(existed_add, on=['address', 'company_id'], how='left')
            tem2 = tem2.query("id.isnull()")

        if tem2.shape[0]>0:
            tem2['insert_timestamp'] = datetime.datetime.now()
            tem2['location_name'] = tem2['address']
            tem2['location_type'] = 'MAILING_ADDRESS'
            vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['location_type', 'location_name', 'address', 'insert_timestamp', 'company_id'], 'company_location', logger)
            return tem2

    def insert_company_billing_address(self, company_loc, logger, allow_dup = False):
        tem2 = company_loc[['address', 'company_externalid']].drop_duplicates()

        tem2.address = tem2.address.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        tem2.address = tem2.address.map(lambda x: ', '.join([e.strip() for e in x if e]))

        tem2 = tem2.drop_duplicates()

        tem2 = tem2.merge(self.company, on=['company_externalid'])
        tem2.rename(columns={'id': 'company_id'}, inplace=True)
        tem2 = tem2.where(tem2.notnull(), None)
        if not allow_dup:
            existed_add = pd.read_sql("select address, company_id, id from company_location", self.ddbconn)
            existed_add = existed_add.where(existed_add.notnull(), None)

            tem2 = tem2.merge(existed_add, on=['address', 'company_id'], how='left')
            tem2 = tem2.query("id.isnull()")

        if tem2.shape[0]>0:
            tem2['insert_timestamp'] = datetime.datetime.now()
            tem2['location_name'] = tem2['address']
            tem2['location_type'] = 'BILLING_ADDRESS'
            vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['location_type', 'location_name', 'address', 'insert_timestamp', 'company_id'], 'company_location', logger)
            return tem2

    def update_note(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'note']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['note', ], ['id', ], 'company', logger)
        vincere_custom_migration.execute_sql_update(r"update company set note=replace(note, '\n', chr(10)) where note is not null;", self.ddbconn)
        return tem2

    def update_note_2(self, df, conn_param, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'note']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['note', ], ['id', ], 'company', logger)
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company', ['note', ], ['id'], logger)
        vincere_custom_migration.execute_sql_update(r"update company set note=replace(note, '\n', chr(10)) where note is not null;", self.ddbconn)
        return tem2

    def update_website(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'website']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['website', ], ['id', ], 'company', logger)
        return tem2

    def update_employees_number(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'employees_number']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['employees_number', ], ['id', ], 'company', logger)
        return tem2

    def update_trading_name(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'trading_name']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['trading_name', ], ['id', ], 'company', logger)
        return tem2

    def update_parent_company(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'parent_externalid']]
        tem2 = tem2.merge(self.company, left_on='parent_externalid', right_on='company_externalid', suffixes=['', '_y'])
        tem2.rename(columns={'id': 'parent_id'}, inplace=True)

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['parent_id', ], ['id', ], 'company', logger)
        return tem2

    def update_head_quarter(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'head_quarter_externalid']]
        tem2 = tem2.merge(self.company, left_on='head_quarter_externalid', right_on='company_externalid', suffixes=['', '_y'])
        tem2.rename(columns={'company_name': 'head_quarter'}, inplace=True)
        tem2.drop('id', axis=1, inplace=True)

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['head_quarter', ], ['id', ], 'company', logger)
        return tem2

    def update_fax(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'fax']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['fax', ], ['id', ], 'company', logger)
        return tem2

    def update_switch_board(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'switch_board']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['switch_board'], ['id'], 'company', logger)
        return tem2

    def update_switch_board_2(self, df, conn_param, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'switch_board']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['switch_board'], ['id'], 'company', logger)
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company', ['switch_board'], ['id'], logger)
        return tem2

    def update_phone(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'phone']]

        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['phone'], ['id'], 'company', logger)
        return tem2

    def update_reg_date(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'reg_date']]
        tem2['insert_timestamp'] = tem2['reg_date']
        # transform data
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['insert_timestamp'], ['id'], 'company', logger)
        return tem2

    def update_owner(self, df, logger):
        tem = df[['company_externalid', 'email']]
        tem1 = tem.merge(pd.read_sql("select id as user_account_id, email from user_account", self.ddbconn), on='email')
        tem2 = tem1.groupby('company_externalid')['user_account_id'].apply(list).reset_index()
        tem2.rename(columns={'user_account_id': 'recruiters'}, inplace=True)

        tem3 = tem2.merge(self.company, on=['company_externalid'])

        # convert recruiter format
        tem3['recruiters'] = tem3['recruiters'].map(lambda x: ','.join(('%s' % i for i in x)))

        # if company have no owner, assign it by recruiter
        tem3.loc[(tem3['company_owners'] == '') | (tem3['company_owners'].isnull()), 'company_owners'] = tem3.loc[(tem3['company_owners'] == '') | (tem3['company_owners'].isnull()), 'recruiters']

        tem3['recruiters'] = tem3.apply(lambda x: ','.join( \
            set( \
                ('%s,%s' % (x['recruiters'], x['company_owners'])).split(',')
            )
        ), axis=1)
        tem3['company_owners'] = tem3['recruiters']

        vincere_custom_migration.psycopg2_bulk_update_tracking(tem3, self.ddbconn, ['company_owners', ], ['id', ], 'company', logger)
        return tem3

    def update_location_city(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'city']]

        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['city', ], ['id', ], 'company_location', logger)
        return tem2

    def update_location_city_2(self, df, conn_param, logger):
        """
        deal with company has multi location case
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        company_location = pd.read_sql("""
                    select
                    c.external_id as company_externalid
                    , c.id as company_id
                    , cl.id
                    , cl.address
                    from company c
                    join company_location cl on c.id = cl.company_id;
                    """, self.ddbconn)
        tem2 = df[['company_externalid', 'city', 'address']]

        # transform data
        tem2 = tem2.merge(company_location, on=['company_externalid', 'address'])
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['city', ], ['id', ], 'company_location', logger)
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company_location', ['city', ], ['id'], logger)
        return tem2

    def update_location_address(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'address']]
        tem2['location_name'] = tem2['address']
        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['address', 'location_name', ], ['id', ], 'company_location', logger)
        return tem2

    def update_location_address_2(self, df, conn_param, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'address']]
        tem2['location_name'] = tem2['address']
        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_externalid'])
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['address', 'location_name', ], ['id', ], 'company_location', logger)
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company_location', ['address', 'location_name', ], ['id'], logger)
        return tem2

    def update_location_name_equalto_address(self):
        vincere_custom_migration.execute_sql_update('update company_location set location_name = address where address is not null', self.ddbconn)

    def update_location_state(self, df: object, logger: object) -> object:
        """
        :rtype: object
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'state']]

        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['state', ], ['id', ], 'company_location', logger)
        return tem2

    def update_location_state_2(self, df, conn_param, logger):
        """
        deal with company has multi location case
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        company_location = pd.read_sql("""
                    select
                    c.external_id as company_externalid
                    , c.id as company_id
                    , cl.id
                    , cl.address
                    from company c
                    join company_location cl on c.id = cl.company_id;
                    """, self.ddbconn)
        tem2 = df[['company_externalid', 'state', 'address']]

        # transform data
        tem2 = tem2.merge(company_location, on=['company_externalid', 'address'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company_location', ['state', ], ['id'], logger)
        return tem2

    def update_location_district_2(self, df, conn_param, logger):
        """
        deal with company has multi location case
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        company_location = pd.read_sql("""
                    select
                    c.external_id as company_externalid
                    , c.id as company_id
                    , cl.id
                    , cl.address
                    from company c
                    join company_location cl on c.id = cl.company_id;
                    """, self.ddbconn)
        tem2 = df[['company_externalid', 'district', 'address']]

        # transform data
        tem2 = tem2.merge(company_location, on=['company_externalid', 'address'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company_location', ['district', ], ['id'], logger)
        return tem2

    def update_location_post_code(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'post_code']]

        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['post_code', ], ['id', ], 'company_location', logger)
        return tem2

    def update_location_post_code_2(self, df, conn_param, logger):
        """
        deal with company has multi location case
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        company_location = pd.read_sql("""
                    select
                    c.external_id as company_externalid
                    , c.id as company_id
                    , cl.id
                    , cl.address
                    from company c
                    join company_location cl on c.id = cl.company_id;
                    """, self.ddbconn)
        tem2 = df[['company_externalid', 'post_code', 'address']]

        # transform data
        tem2 = tem2.merge(company_location, on=['company_externalid', 'address'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company_location', ['post_code', ], ['id'], logger)
        return tem2

    def update_location_country(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'country', 'country_code']]

        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['country', 'country_code'], ['id', ], 'company_location', logger)
        return tem2

    def update_location_country_2(self, df, conn_param, logger):
        """
        deal with company has multi location case
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        company_location = pd.read_sql("""
                    select
                    c.external_id as company_externalid
                    , c.id as company_id
                    , cl.id
                    , cl.address
                    from company c
                    join company_location cl on c.id = cl.company_id;
                    """, self.ddbconn)
        tem2 = df[['company_externalid', 'country', 'country_code', 'address']]

        # transform data
        tem2 = tem2.merge(company_location, on=['company_externalid', 'address'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'company_location', ['country', 'country_code', ], ['id'], logger)
        return tem2

    def update_location_latitude_longitude(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_externalid', 'latitude', 'longitude']]

        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['latitude', 'longitude'], ['id', ], 'company_location', logger)
        return tem2

    def update_location_latitude_longitude_inhouse(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['company_id', 'latitude', 'longitude']]

        # transform data
        tem2 = tem2.merge(self.company_location, on=['company_id'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['latitude', 'longitude'], ['id', ], 'company_location', logger)
        return tem2

    def get_country_code(self, country_name):
        """https://www.nationsonline.org/oneworld/country_code_list.htm"""
        from common import country_code
        return_code = ''
        for k, v in country_code.country_codes.items():
            # print("code {0}, name {1}".format(k, v))
            if str(country_name).lower().strip() in [i.lower() for i in v]:  # check an item exits in the tuple v
                return_code = k
                break
        return return_code

    def process_vincere_comp(self, df, logger):
        if 'company-locationCountry' in df.columns:
            df['company-locationCountry'] = [self.get_country_code(x) for x in df['company-locationCountry']]
        if 'company-name' in df.columns:
            # CLEAN1: companies names must be unique
            df['company-name'].fillna('DEFAULT COMPANY', inplace=True)
            df['company-name'] = df['company-name'].str.strip()
            df['company-name'].replace('', 'DEFAUT NAME', inplace=True)
            df['rn'] = df.groupby(df['company-name'].str.lower()).cumcount() + 1  # group by string case insensitive
            df['company-name'] = df.apply(lambda x: x['company-name'] if x['rn'] == 1 else '%s_%s' % (x['company-name'], x['rn']), axis=1)
        if 'company-locationAddress' in df.columns:
            # FORMAT address
            df['company-locationAddress'] = df['company-locationAddress'].fillna('')
            df['company-locationAddress'] = df['company-locationAddress'].apply(lambda x: x.replace('\n', ','))
            df['company-locationAddress'] = df['company-locationAddress'].apply(lambda x: x.replace('\r', ','))
            df['company-locationAddress'] = df['company-locationAddress'].apply(lambda x: re.sub(r"\,{2,}|,\s$|\,\s*\,{1,}", ',', x))
            df['company-locationAddress'] = df['company-locationAddress'].apply(lambda x: re.sub(r"\,\s{1,}$", '', x))

            df['company-locationAddress'] = df['company-locationAddress'].map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
            df['company-locationAddress'] = df['company-locationAddress'].map(lambda x: ', '.join([e.strip() for e in x if e]))
        return df.filter(regex='^company')

    def insert_company_industry(self, df, logger):
        tem2 = df[['company_externalid', 'name']].dropna().drop_duplicates()
        tem2 = tem2.merge(self.company, on=['company_externalid'])
        tem2.rename(columns={'id':'company_id', }, inplace=True)
        cols = ['industry_id', 'company_id', 'insert_timestamp', 'seq']
        tem2 = tem2.merge(pd.read_sql('select * from vertical', self.ddbconn), on='name')
        tem2.rename(columns={'id': 'industry_id', }, inplace=True)
        tem2 = tem2.merge(pd.read_sql("select industry_id, company_id, 'existed' as note from company_industry", self.ddbconn), on=['industry_id', 'company_id'], how='left')
        tem2 = tem2.loc[tem2['note'].isnull()]
        tem2['seq'] = tem2.groupby('company_id').cumcount()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, cols, 'company_industry', logger)
        return tem2

    def insert_company_industry_inhouse(self, df, logger):
        tem2 = df[['company_id', 'name']]
        cols = ['industry_id', 'company_id', 'insert_timestamp', 'seq']
        tem2 = tem2.merge(pd.read_sql('select * from vertical', self.ddbconn), on='name')
        tem2.rename(columns={'id': 'industry_id', }, inplace=True)
        tem2 = tem2.merge(pd.read_sql("select industry_id, company_id, 'existed' as note from company_industry", self.ddbconn), on=['industry_id', 'company_id'], how='left')
        tem2 = tem2.loc[tem2['note'].isnull()]
        tem2['seq'] = tem2.groupby('company_id').cumcount()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, cols, 'company_industry', logger)
        return tem2