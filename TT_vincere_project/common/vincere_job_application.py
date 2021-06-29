# -*- coding: UTF-8 -*-
import common.vincere_custom_migration as vincere_custom_migration
import numpy as np
import pandas as pd
import re


class JobApplication:
    def __init__(self, ddbconn):
        """

        :rtype: object
        """
        self.ddbconn = ddbconn
        self.stage_desc = {
            1: 'SHORTLISTED',
            1.1: 'SHORTLISTED',  # rejected
            2: 'SENT',
            2.1: 'SENT',  # rejected
            3: 'FIRST_INTERVIEW',
            3.1: 'FIRST_INTERVIEW',  # rejected
            4: 'SECOND_INTERVIEW',
            4.1: 'SECOND_INTERVIEW',  # rejected
            5: 'OFFERED',
            5.1: 'OFFERED',  # rejected
            6: 'PLACEMENT_PERMANENT',
            7: 'ONBOARDING',
        }

    def process_jobapp(self, df):
        tem = self.jobapp_map_only(df)
        tem = tem.groupby(['application-positionExternalId', 'application-candidateExternalId'])['stage'].max().reset_index()
        tem['application-stage'] = tem['stage'].map(lambda s: self.stage_desc.get(s))
        tem['application-stage-note'] = tem['stage'].map(lambda s: 'rejected' if str(s)[-1] == '1' else '')
        return tem

    def process_jobapp_v2(self, df):
        tem = self.jobapp_map_only(df)
        tem = tem.groupby(['application-positionExternalId', 'application-candidateExternalId']).apply(lambda subdf: subdf.sort_values(['stage', 'application-actionedDate'], ascending=False))[['stage', 'application-actionedDate']]
        tem = tem.groupby(['application-positionExternalId', 'application-candidateExternalId']).apply(lambda subdf: subdf.loc[subdf['stage'].idxmax(), ['stage', 'application-actionedDate']]).reset_index()
        tem['application-stage'] = tem['stage'].map(lambda s: self.stage_desc.get(s))
        tem['application-stage-note'] = tem['stage'].map(lambda s: 'rejected' if str(s)[-1] == '1' else '')
        return tem

    def process_jobapp_v3(self, df, *extra_cols):
        print(extra_cols)
        cols = ['stage', 'application-actionedDate'] + list(extra_cols)
        print(cols)
        tem = self.jobapp_map_only(df)
        tem = tem.groupby(['application-positionExternalId', 'application-candidateExternalId']).apply(lambda subdf: subdf.sort_values(['stage', 'application-actionedDate'], ascending=False))[cols]
        tem = tem.groupby(['application-positionExternalId', 'application-candidateExternalId']).apply(lambda subdf: subdf.loc[subdf['stage'].idxmax(), cols]).reset_index()
        tem['application-stage'] = tem['stage'].map(lambda s: self.stage_desc.get(s))
        tem['application-stage-note'] = tem['stage'].map(lambda s: 'rejected' if str(s)[-1] == '1' else '')
        return tem

    def jobapp_map_only(self, df):
        df['stage'] = -1.0
        df['application-stage'] = [str(x).lower().strip() for x in df['application-stage']]
        df['application-stage'] = df['application-stage'].map(lambda x: ''.join(re.findall('[a-zA-Z0-9_]*', x)))
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('placement', flags=re.IGNORECASE)), 'stage'] = 6
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('placed', flags=re.IGNORECASE)), 'stage'] = 6
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('place', flags=re.IGNORECASE)), 'stage'] = 6

        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('offer', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 5.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('offered', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 5.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('offfered', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 5.1

        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('offer', flags=re.IGNORECASE)), 'stage'] = 5
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('offered', flags=re.IGNORECASE)), 'stage'] = 5
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('offfered', flags=re.IGNORECASE)), 'stage'] = 5

        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('interview2', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 4.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('2nd_interview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 4.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('2ndinterview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 4.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('second_interview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 4.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('secondinterview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 4.1

        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('interview2', flags=re.IGNORECASE)), 'stage'] = 4
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('2nd_interview', flags=re.IGNORECASE)), 'stage'] = 4
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('2ndinterview', flags=re.IGNORECASE)), 'stage'] = 4
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('second_interview', flags=re.IGNORECASE)), 'stage'] = 4
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('secondinterview', flags=re.IGNORECASE)), 'stage'] = 4

        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('1st_interview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 3.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('1stinterview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 3.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('interview1', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 3.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('first_interview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 3.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('firstinterview', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE)), 'stage'] = 3.1

        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('1st_interview', flags=re.IGNORECASE)), 'stage'] = 3
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('1stinterview', flags=re.IGNORECASE)), 'stage'] = 3
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('interview1', flags=re.IGNORECASE)), 'stage'] = 3
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('first_interview', flags=re.IGNORECASE)), 'stage'] = 3
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('firstinterview', flags=re.IGNORECASE)), 'stage'] = 3

        df.loc[(df['stage'] == -1) &
               (df['application-stage'].str.contains('sent', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE))
        , 'stage'] = 2.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('sent', flags=re.IGNORECASE)), 'stage'] = 2

        df.loc[(df['stage'] == -1) &
               (df['application-stage'].str.contains('SHORTLISTED', flags=re.IGNORECASE)) &
               (df['application-stage'].str.contains('rejected', flags=re.IGNORECASE))
        , 'stage'] = 1.1
        df.loc[(df['stage'] == -1) & (df['application-stage'].str.contains('SHORTLISTED', flags=re.IGNORECASE)), 'stage'] = 1
        try:
            assert df.loc[df['stage'] == -1].shape[0] == 0, 'There are some un-map stages'
        except Exception as ex:
            print(ex)
            print(df.loc[df['stage'] == -1])
        return df
