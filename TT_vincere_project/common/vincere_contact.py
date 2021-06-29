# -*- coding: UTF-8 -*-
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common
import numpy as np
import pandas as pd
import datetime
import re


class Contact(vincere_common.Common):
    def __init__(self, ddbconn):
        if ddbconn:
            self.ddbconn = ddbconn
            self.contact = pd.read_sql(
                "select id, external_id as contact_externalid, contact_owners, current_location_id from contact where deleted_timestamp is null",
                ddbconn)

    def insert_contact_work_location(self, contact_loc, logger):
        tem2 = contact_loc[['contact_externalid', 'company_externalid', 'address']].drop_duplicates()
        # tem2.address = tem2.address.map(lambda x: re.findall("[a-zA-Z0-9 \-\#áº\'\?\£]*", x))
        # tem2.address = tem2.address.map(lambda x: ', '.join([e.strip() for e in x if e]))
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2.rename(columns={'id': 'contact_id'}, inplace=True)
        tem2 = tem2.merge(
            pd.read_sql("select id as company_id, external_id as company_externalid from company", self.ddbconn),
            on='company_externalid')

        tem2 = tem2.merge(
            pd.read_sql("select id as company_location_id, company_id, address from company_location", self.ddbconn),
            on=['company_id', 'address'])
        tem2 = tem2.merge(pd.read_sql("select contact_id, company_location_id, 'existed' as note from contact_location",
                                      self.ddbconn), on=['contact_id', 'company_location_id'], how='left')
        tem2 = tem2.query("note.isnull()")
        tem2['insert_timestamp'] = datetime.datetime.now()

        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, ['contact_id', 'insert_timestamp',
                                                                                    'company_location_id'],
                                                               'contact_location', logger)
        return tem2

    def insert_contact_department(self, contact_loc, logger):
        tem2 = contact_loc[['contact_externalid', 'company_externalid', 'department_name']].drop_duplicates()
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2.rename(columns={'id': 'contact_id'}, inplace=True)
        tem2 = tem2.merge(
            pd.read_sql("select id as company_id, external_id as company_externalid from company", self.ddbconn),
            on='company_externalid')

        tem2 = tem2.merge(pd.read_sql("select id as department_id, company_id, department_name from company_department",
                                      self.ddbconn), on=['company_id', 'department_name'])
        tem2 = tem2.merge(
            pd.read_sql("select department_id, contact_id, 'existed' as note from contact_department", self.ddbconn),
            on=['contact_id', 'department_id'], how='left')
        tem2 = tem2.query("note.isnull()")
        tem2['insert_timestamp'] = datetime.datetime.now()

        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn,
                                                               ['contact_id', 'insert_timestamp', 'department_id'],
                                                               'contact_department', logger)
        return tem2

    def insert_current_location(self, df, logger):
        """
        can be run many times, the second will fire an exception:
            psycopg2.IntegrityError: duplicate key value violates unique constraint "current_location_candidate_uni_idx"
            DETAIL:  Key (current_location_candidate_id)=(84120) already exists.

        :param df:
        :param logger:
        :return:
        """
        try:
            tem2 = df[['address', 'contact_externalid', ]]
            tem2 = tem2.merge(self.contact, on=['contact_externalid'])
            tem2['insert_timestamp'] = datetime.datetime.now()

            # avoid dup address
            tem3 = tem2.merge(pd.read_sql("select address, 'existed' as note from common_location", self.ddbconn),
                              on='address', how='left')
            tem3 = tem3.query("note.isnull()")
            tem3['location_name'] = tem3['address']
            vincere_custom_migration.psycopg2_bulk_insert_tracking(tem3, self.ddbconn,
                                                                   ['location_name', 'address', 'insert_timestamp'],
                                                                   'common_location', logger)

            # loc
            new_loc = pd.read_sql("select id as current_location_id, address from common_location", self.ddbconn)
            tem2 = tem2.merge(new_loc, on='address')
            tem2['current_location_id'] = tem2['current_location_id_y']
            vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['current_location_id'], ['id'],
                                                                   'contact', logger)

            return tem2
        except Exception as ex:
            logger.error(ex)

    def update_current_location_city(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'city']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['city', ], ['id', ],
                                                               'common_location', logger)
        return tem2

    def update_current_location_city_v2(self, df, conn_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'city']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'common_location', ['city', ], ['id'],
                                                      logger)
        return tem2

    def update_current_location_post_code(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'post_code']]
        tem2 = tem2.merge(pd.read_sql(
            "select id, external_id as contact_externalid, contact_owners, current_location_id from contact",
            self.ddbconn), on=['contact_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['post_code', ], ['id', ],
                                                               'common_location', logger)
        return tem2

    def update_current_location_post_code_v2(self, df, conn_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'post_code']]
        tem2 = tem2.merge(pd.read_sql(
            "select id, external_id as contact_externalid, contact_owners, current_location_id from contact",
            self.ddbconn), on=['contact_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'common_location', ['post_code', ],
                                                      ['id'], logger)
        return tem2

    def set_work_location_by_company_location(self, logger):
        """
        if a company have serveral locations, the function will pick the first location and assign to contacts
        :param logger:
        :return:
        """
        # load data
        company_location = pd.read_sql('select * from company_location;', self.ddbconn)
        contact = pd.read_sql('select company_id, id as contact_id from contact', self.ddbconn)
        contact_location = pd.read_sql('select * from contact_location', self.ddbconn)

        # find companies have 1 location
        pd.DataFrame(company_location.groupby('company_id').count().query("id==1"))
        company_location['rn'] = company_location.groupby('company_id').cumcount()
        company_location = company_location.query("rn == 0")

        # companies_has_one_loc = company_location.groupby('company_id').count().query("id==1")
        companies_has_one_loc = company_location  # get the first location
        companies_has_one_loc.reset_index(level=0, inplace=True)

        # find contacts belong to companies have one location
        contact_of_companies_have_one_loc = contact.merge(companies_has_one_loc[['company_id']], on='company_id')

        # find contacts had not been assigned location by default
        contact_of_companies_have_one_loc_but_not_assigned_loc = contact_of_companies_have_one_loc.query(
            "contact_id not in @contact_location.contact_id")

        # assign default location for contacts have no default location
        company_location = company_location[['id', 'company_id']]
        company_location.rename(columns={'id': 'company_location_id'}, inplace=True)
        contact_of_companies_have_one_loc_but_not_assigned_loc = contact_of_companies_have_one_loc_but_not_assigned_loc.merge(
            company_location, on='company_id')
        contact_of_companies_have_one_loc_but_not_assigned_loc['insert_timestamp'] = datetime.datetime.now()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(contact_of_companies_have_one_loc_but_not_assigned_loc,
                                                               self.ddbconn, ['contact_id', 'insert_timestamp',
                                                                              'company_location_id'],
                                                               'contact_location', logger)

    def update_current_location_country_code(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'country_code']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['country_code', ], ['id', ],
                                                               'common_location', logger)
        return tem2

    def update_current_location_state(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'state']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['state', ], ['id', ],
                                                               'common_location', logger)
        return tem2

    def update_current_location_state_v2(self, df, conn_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'state']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2['id'] = tem2['current_location_id']
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'common_location', ['state', ],
                                                      ['id'], logger)
        return tem2

    def update_mobile_phone(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'mobile_phone']]

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['mobile_phone', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_mobile_phone2(self, df, conn_param, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'mobile_phone']]
        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'contact', ['mobile_phone', ], ['id'],
                                                      logger)
        return tem2

    def update_status_board(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'board', 'status']]

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['board', 'status'], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_start_date(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'start_date']]

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['start_date', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_last_activity_date(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'last_activity_date']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2 = tem2.merge(pd.read_sql("select contact_id from contact_extension", self.ddbconn), left_on='id',
                          right_on='contact_id')
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['last_activity_date', ],
                                                               ['contact_id', ], 'contact_extension', logger)
        return tem2

    def update_personal_location_address_2(self, df, conn_param, logger):
        tem2 = df[['address', 'contact_externalid', ]]
        existed_loc = pd.read_sql(
            "select id as common_location_id, address from common_location where address is not null", self.ddbconn)
        tem2 = tem2.merge(existed_loc, on='address', how='left')
        tem2 = tem2.merge(
            pd.read_sql("select id as contact_id, external_id as contact_externalid, current_location_id from contact",
                        self.ddbconn), on=['contact_externalid'])

        loc_upd = tem2.loc[tem2.common_location_id.notnull()]
        loc_ins = tem2.loc[tem2.common_location_id.isnull()]

        loc_upd['common_location_id'] = loc_upd['common_location_id'].astype(int)
        loc_upd['current_location_id'] = loc_upd['common_location_id']
        loc_upd['location_name'] = loc_upd['address']
        loc_upd['id'] = loc_upd['common_location_id']
        vincere_custom_migration.load_data_to_vincere(loc_upd, conn_param, 'update', 'common_location',
                                                      ['address', 'location_name'], ['id'], logger)

        loc_upd['id'] = loc_upd['contact_id']
        vincere_custom_migration.load_data_to_vincere(loc_upd, conn_param, 'update', 'contact', ['current_location_id'],
                                                      ['id'], logger)

        loc_ins['location_name'] = loc_ins['address']
        loc_ins['insert_timestamp'] = datetime.datetime.now()
        tem3 = loc_ins[['location_name', 'address', 'insert_timestamp']].drop_duplicates()
        vincere_custom_migration.load_data_to_vincere(tem3, conn_param, 'insert', 'common_location',
                                                      ['location_name', 'address', 'insert_timestamp'], '', logger)

        loc_ins = loc_ins.drop('common_location_id', axis=1).merge(
            pd.read_sql("select id as common_location_id, address from common_location", self.ddbconn), on='address')

        loc_ins['current_location_id'] = loc_ins['common_location_id']
        loc_ins['id'] = loc_ins['contact_id']
        vincere_custom_migration.load_data_to_vincere(loc_ins, conn_param, 'update', 'contact', ['current_location_id'],
                                                      ['id'], logger)
        return loc_upd, loc_ins

    def update_active(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        # passive: 0
        # active: 1
        # do not contact: 2
        # blacklisted: 3

        tem2 = df[['contact_externalid', 'active']]

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['active', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_home_phone(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'home_phone']]

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['home_phone', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_home_phone2(self, df, conn_param, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'home_phone']]
        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'contact', ['home_phone', ], ['id'],
                                                      logger)
        return tem2

    def update_personal_email(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'personal_email']]

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['personal_email', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_email(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'email']]

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['email', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_preferred_name(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'preferred_name']]
        tem2['nick_name'] = tem2['preferred_name']

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['nick_name', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_switchboard_phone(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'switchboard_phone']]
        tem2['nick_name'] = tem2['switchboard_phone']

        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['switchboard_phone', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_reg_date(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'reg_date']]
        tem2['insert_timestamp'] = tem2['reg_date']
        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['insert_timestamp'], ['id'],
                                                               'contact', logger)
        return tem2

    def update_dob(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'date_of_birth']]
        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['date_of_birth'], ['id'], 'contact',
                                                               logger)
        return tem2

    def update_primary_phone(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'primary_phone']]
        tem2['phone'] = tem2['primary_phone']
        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['phone', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_primary_phone2(self, df, conn_param, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values

        tem2 = df[['contact_externalid', 'primary_phone']]
        tem2['phone'] = tem2['primary_phone']
        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        # vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['phone', ], ['id', ], 'contact', logger)
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'contact', ['phone', ], ['id'],
                                                      logger)
        return tem2

    def update_skills(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'skills']].drop_duplicates().groupby('contact_externalid')['skills'].apply(
            ','.join).reset_index()
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['skills', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_job_title(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'job_title']].dropna().drop_duplicates().groupby('contact_externalid')[
            'job_title'].apply(', '.join).reset_index()
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['job_title', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_job_title2(self, df, conn_param, logger):
        """
        """
        tem2 = df[['contact_externalid', 'job_title']].dropna().drop_duplicates().groupby('contact_externalid')[
            'job_title'].apply(', '.join).reset_index()
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'contact', ['job_title', ], ['id'],
                                                      logger)
        return tem2

    def update_linkedin(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'linkedin']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['linkedin', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_skype(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'skype']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['skype', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_xing(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'xing']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['xing', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_twitter(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'twitter']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['twitter', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_department(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'department']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['department', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_middle_name(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'middle_name']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['middle_name', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_facebook(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'facebook']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['facebook', ], ['id', ], 'contact',
                                                               logger)
        return tem2

    def update_method_contact(self, df, logger):
        """
        """
        tem2 = df[['contact_externalid', 'method_of_contact']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['method_of_contact', ], ['id', ],
                                                               'contact', logger)
        return tem2

    def update_note(self, df, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'note']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['note', ], ['id', ], 'contact',
                                                               logger)
        vincere_custom_migration.execute_sql_update(
            r"update contact set note=replace(note, '\n', chr(10)) where note is not null;", self.ddbconn)
        return tem2

    def update_note_2(self, df, conn_param, logger):
        """

        :rtype: object
        """
        tem2 = df[['contact_externalid', 'note']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.load_data_to_vincere(tem2, conn_param, 'update', 'contact', ['note', ], ['id'], logger)
        vincere_custom_migration.execute_sql_update(
            r"update contact set note=replace(note, '\n', chr(10)) where note is not null;", self.ddbconn)
        return tem2

    def append_company_address_to_note(self, logger):
        df = pd.read_sql("""
                select
                   cont.id as id
                    , cl.address
                    from contact cont 
                    join company com on cont.company_id = com.id
                    join company_location cl on com.id = cl.company_id
                   and cl.address is not null
                """, self.ddbconn)
        df = df.groupby('id').apply(lambda subdf: '\n'.join(subdf.get('address'))).reset_index().rename(
            columns={0: 'address'})

        df = df.merge(pd.read_sql("select id, external_id as contact_externalid, note from contact", self.ddbconn),
                      on='id')
        df.address = 'Company Address: \n' + df.address
        df.note = df[['note', 'address']].apply(lambda x: '\n\n'.join([e for e in x if e]), axis=1)
        return self.update_note(df, logger)

    def update_owner(self, df, logger):
        tem = df[['contact_externalid', 'email']]
        tem1 = tem.merge(pd.read_sql("select id as user_account_id, email from user_account", self.ddbconn), on='email')
        tem2 = tem1.groupby('contact_externalid')['user_account_id'].apply(list).reset_index()
        tem2.rename(columns={'user_account_id': 'recruites'}, inplace=True)

        tem3 = tem2.merge(self.contact, on=['contact_externalid'])

        # convert contact format
        tem3['contact_owners'] = tem3['contact_owners'].map(
            lambda x: re.sub(r"""\[|\]|\"""", '', x) if x else x)  # replace [ and ] in contact_owners column
        # convert recruiter format from list to string splited by comma
        tem3['recruites'] = tem3['recruites'].map(lambda x: ','.join(('%s' % i for i in x)))
        # if contact have no owner, assign it by recruiter
        tem3.loc[tem3['contact_owners'] == '', 'contact_owners'] = tem3.loc[tem3['contact_owners'] == '', 'recruites']
        tem3.loc[tem3['contact_owners'].isnull(), 'contact_owners'] = tem3.loc[
            tem3['contact_owners'].isnull(), 'recruites']
        # get distinct contact recruiter and owner into recruiter
        tem3['recruites'] = tem3.apply(lambda x: ','.join( \
            set( \
                ('%s,%s' % (x['recruites'], x['contact_owners'])).split(',')
            )
        ), axis=1)
        # reformat contact recruiter
        tem3['recruites'] = tem3['recruites'].map(lambda x: ['"%s"' % i for i in x.split(',')])
        tem3['recruites'] = tem3['recruites'].map(lambda x: '[{}]'.format(','.join(i for i in x)))  # [id1,id2,id3]
        tem3['contact_owners'] = tem3['recruites']  # format: ["id1","id2","id3"]
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem3, self.ddbconn, ['contact_owners'], ['id'],
                                                               'contact', logger)
        return tem3

    def insert_fe_sfe(self, df, logger):
        tem2 = df[['functional_expertise_id', 'contact_externalid', 'sub_functional_expertise_id']]
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2['contact_id'] = tem2['id']
        tem2['insert_timestamp'] = datetime.datetime.now()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn,
                                                               ['functional_expertise_id', 'contact_id',
                                                                'insert_timestamp', 'sub_functional_expertise_id'],
                                                               'contact_functional_expertise', logger)
        return tem2

    def insert_fe_sfe2(self, df, logger):
        tem2 = df[['contact_externalid', 'fe', 'sfe']]
        tem2['matcher_fe'] = tem2['fe'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
        tem2['matcher_sfe'] = tem2['sfe'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
        fe = pd.read_sql('select id as functional_expertise_id, name as fe from functional_expertise', self.ddbconn)
        fe['matcher_fe'] = fe['fe'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
        sfe = pd.read_sql(
            'select functional_expertise_id, id as sub_functional_expertise_id, name as sfe from sub_functional_expertise',
            self.ddbconn)
        sfe['matcher_sfe'] = sfe['sfe'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
        tem2 = tem2.merge(fe, on='matcher_fe', how='left')
        tem2 = tem2.loc[tem2['functional_expertise_id'].notnull()]
        tem2['functional_expertise_id'] = tem2['functional_expertise_id'].astype(int)
        tem2 = tem2.merge(sfe, on=['functional_expertise_id', 'matcher_sfe'], how='left')
        # tem2 = tem2.where(tem2.notnull(), None)
        tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'] = tem2.loc[
            tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'].astype(int)

        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2['contact_id'] = tem2['id']
        tem2['insert_timestamp'] = datetime.datetime.now()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn,
                                                               ['functional_expertise_id', 'contact_id',
                                                                'insert_timestamp', 'sub_functional_expertise_id'],
                                                               'contact_functional_expertise', logger)
        return tem2

    def insert_fe_sfe2_inhouse(self, df, logger):
        tem2 = df[['contact_id', 'fe', 'sfe']]
        tem2 = tem2.where(tem2.notnull(), None)
        tem2['fe'] = tem2['fe'].apply(lambda x: str.strip(x).lower() if x else x)
        tem2['sfe'] = tem2['sfe'].apply(lambda x: str.strip(x).lower() if x else x)

        tem2 = tem2.merge(
            pd.read_sql('select id as functional_expertise_id, lower(trim(name)) as fe from functional_expertise',
                        self.ddbconn), on='fe', how='left')
        tem2 = tem2.merge(pd.read_sql(
            'select functional_expertise_id, id as sub_functional_expertise_id, lower(trim(name)) as sfe from sub_functional_expertise',
            self.ddbconn), on=['functional_expertise_id', 'sfe'], how='left')
        tem2 = tem2.where(tem2.notnull(), None)
        tem2.loc[tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'] = tem2.loc[
            tem2['sub_functional_expertise_id'].notnull(), 'sub_functional_expertise_id'].astype(int)

        tem2['insert_timestamp'] = datetime.datetime.now()
        existed_fesfe = pd.read_sql(
            "select id, contact_id, functional_expertise_id, sub_functional_expertise_id from contact_functional_expertise;",
            self.ddbconn)
        existed_fesfe = existed_fesfe.where(existed_fesfe.notnull(), None)
        tem2 = tem2.merge(existed_fesfe, on=['contact_id', 'functional_expertise_id', 'sub_functional_expertise_id'],
                          how='left')
        tem2 = tem2.query('id.isnull()')
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn,
                                                               ['functional_expertise_id', 'contact_id',
                                                                'insert_timestamp', 'sub_functional_expertise_id'],
                                                               'contact_functional_expertise', logger)
        return tem2

    def process_vincere_contact(self, df):
        if 'contact-email' in df.columns:
            df = self.process_vincere_email(df, 'contact-externalId', 'contact-email')
        if 'contact-Email' in df.columns:
            df = self.process_vincere_email(df, 'contact-externalId', 'contact-Email')
        if 'contact-lastName' in df.columns:
            df['contact-lastName'].fillna('', inplace=True)
            df.loc[df['contact-lastName'].str.strip() == '', 'contact-lastName'] = "CONTACT"
        if 'contact-firstName' in df.columns:
            df['contact-firstName'].fillna('', inplace=True)
            df.loc[df['contact-firstName'].str.strip() == '', 'contact-firstName'] = 'DEFAULT'
        if 'contact.date_of_birth' in df.columns:
            df['contact.date_of_birth'] = [pd.Timestamp(x).date() if str(x) != 'nan' else x for x in
                                           df['contact.date_of_birth']]  # convert datetime to date
        return df.filter(regex='^contact')

    def insert_contact_industry_subindustry(self, df, logger):
        tem2 = df[['contact_externalid', 'name']].dropna().drop_duplicates()
        tem2['matcher'] = tem2['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2.rename(columns={'id': 'contact_id', }, inplace=True)
        cols = ['industry_id', 'contact_id', 'insert_timestamp', 'seq']
        ver = pd.read_sql('select * from vertical', self.ddbconn)
        ver['matcher'] = ver['name'].apply(lambda x: ''.join(re.findall('[a-zA-Z0-9]', x)).lower())
        tem2 = tem2.merge(ver, on='matcher')
        tem2.rename(columns={'id': 'industry_id', }, inplace=True)
        tem2 = tem2.merge(
            pd.read_sql("select industry_id, contact_id, 'existed' as note from contact_industry", self.ddbconn),
            on=['industry_id', 'contact_id'], how='left')
        tem2 = tem2.loc[tem2['note'].isnull()]
        tem2['seq'] = tem2.groupby('contact_id').cumcount()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, cols, 'contact_industry', logger)
        return tem2

    def insert_contact_industry_inhouse(self, df, logger):
        tem2 = df[['contact_id', 'name']]
        cols = ['industry_id', 'contact_id', 'insert_timestamp', 'seq']
        tem2 = tem2.merge(pd.read_sql('select * from vertical', self.ddbconn), on='name')
        tem2.rename(columns={'id': 'industry_id', }, inplace=True)
        tem2 = tem2.merge(
            pd.read_sql("select industry_id, contact_id, 'existed' as note from contact_industry", self.ddbconn),
            on=['industry_id', 'contact_id'], how='left')
        tem2 = tem2.loc[tem2['note'].isnull()]
        tem2['seq'] = tem2.groupby('contact_id').cumcount()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn, cols, 'contact_industry', logger)
        return tem2

    def update_gender_title(self, df, logger):
        """
        :param ddbconn:
        :param logger:
        :return:
        """
        # prepare position type values
        tem2 = df[['contact_externalid', 'gender_title']]
        # transform data
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2[['id', 'gender_title']].dropna().drop_duplicates(),
                                                               self.ddbconn, ['gender_title'], ['id'], 'contact',
                                                               logger)
        return tem2

    def create_distribution_list(self, df, logger):
        tem2 = df.dropna().drop_duplicates()
        if 'insert_timestamp' not in tem2.columns:
            tem2['insert_timestamp'] = datetime.datetime.now()
        if 'share_permission' not in tem2.columns:
            tem2['share_permission'] = 1
        df_owner = pd.read_sql("select id as owner_id, email from user_account", self.ddbconn)
        tem2 = tem2.merge(df_owner, left_on='owner', right_on='email', how='left')
        tem2['owner_id'] = tem2['owner_id'].map(lambda x: x if x else -10)
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn,
                                                               ['owner_id', 'name', 'share_permission',
                                                                'insert_timestamp'], 'contact_group', logger)

    def add_contact_distribution_list(self, df, logger):
        tem2 = df[['contact_externalid', 'group_name']].dropna().drop_duplicates()
        if 'insert_timestamp' not in tem2.columns:
            tem2['insert_timestamp'] = datetime.datetime.now()
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        df_group = pd.read_sql("select id as contact_group_id, name from contact_group", self.ddbconn)
        tem2 = tem2.merge(df_group, left_on='group_name', right_on='name', how='left')
        tem2 = tem2.drop_duplicates()
        tem2['contact_id'] = tem2['id']
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn,
                                                               ['contact_group_id', 'contact_id', 'insert_timestamp'],
                                                               'contact_group_contact', logger)

    def create_status_list(self, df, logger):
        tem2 = df.fillna('')
        tem2 = tem2.drop_duplicates()
        if 'insert_timestamp' not in tem2.columns:
            tem2['insert_timestamp'] = datetime.datetime.now()
        status = pd.read_sql("select id , name from contact_status", self.ddbconn)
        tem2 = tem2.merge(status, on='name', how='left', indicator=True)
        tem2 = tem2.query('id.isnull()')
        df_owner = pd.read_sql("select id as creator_id, email from user_account", self.ddbconn)
        tem2 = tem2.merge(df_owner, left_on='owner', right_on='email', how='left')
        tem2['creator_id'] = tem2['creator_id'].fillna(-10)
        tem2['type'] = 1
        vincere_custom_migration.psycopg2_bulk_insert_tracking(tem2, self.ddbconn,
                                                               ['creator_id', 'name', 'type', 'insert_timestamp'],
                                                               'contact_status', logger)

    def add_contact_status(self, df, logger):
        tem2 = df[['contact_externalid', 'name']]
        status = pd.read_sql("select id as active, name from contact_status", self.ddbconn)
        tem2 = tem2.merge(status, on='name')
        tem2 = tem2.merge(self.contact, on=['contact_externalid'])
        tem2 = tem2.drop_duplicates()
        tem2['active'] = tem2['active'].astype(int)
        vincere_custom_migration.psycopg2_bulk_update_tracking(tem2, self.ddbconn, ['active'], ['id'], 'contact',
                                                               logger)

    def email_subscribe(self, df, logger):
        """
        'subscribed' = 0  # unsubscribe
        'subscribed' = 1  # subscribe
        :param df:
        :param logger:
        :return:
        """
        tem2 = df[['email', 'subscribed']].dropna().drop_duplicates()
        tem2 = tem2.query("email != ''")
        tem2['email'] = tem2['email'].apply(str.lower)
        tem2 = tem2.drop_duplicates()
        # load subscribe email
        email_subscription = pd.read_sql(
            "select id, lower(email) as email, last_modified_date, insert_timestamp from email_subscription;",
            self.ddbconn)

        # main process
        tem2 = tem2.merge(email_subscription, on='email', how='left', indicator=True)
        # new_subscribe_email = tem2.query("_merge == 'left_only'")

        # update
        df_update = tem2.query('id.notnull()')
        df_update['id'] = df_update['id'].astype(int)
        df_update['subscribed'] = df_update['subscribed'].astype(int)
        vincere_custom_migration.psycopg2_bulk_update_tracking(df_update, self.ddbconn, ['subscribed', ], ['id', ],
                                                               'email_subscription', logger)

        # insert
        df_insert = tem2.query('id.isnull()')
        df_insert['last_modified_date'] = datetime.datetime.now()
        df_insert['insert_timestamp'] = datetime.datetime.now()
        df_insert['subscribed'] = df_insert['subscribed'].astype(int)
        df_insert = df_insert[['email', 'subscribed', 'last_modified_date', 'insert_timestamp']].drop_duplicates()
        vincere_custom_migration.psycopg2_bulk_insert_tracking(df_insert, self.ddbconn,
                                                               ['email', 'subscribed', 'last_modified_date',
                                                                'insert_timestamp'], 'email_subscription', logger)
        return tem2

    def insert_source(self, df):
        tem2 = df[['contact_externalid', 'source']].dropna()
        vincere_custom_migration.inject_contact_source(tem2, 'contact_externalid', 'source', self.ddbconn)
