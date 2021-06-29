from common.vincere_common import *


# def process_gender_title(df, title_col, gender_col):
#     title = {
#         'MR':('mr', 'm r'),
#         'MRS':('mrs', ),
#         'MS':('ms', 'm s'),
#         'MISS':('miss', 'mis'),
#         'DR':('dr', ),
#         }
#     gender = {
#         'MALE': ('male', 'm'),
#         'FEMALE': ('female', 'f'),
#         }
#     df[gender_col] = ['MALE'   if str(x).strip().lower() in gender.get('MALE')   else x for x in df[gender_col]]
#     df[gender_col] = ['FEMALE' if str(x).strip().lower() in gender.get('FEMALE') else x for x in df[gender_col]]
#
#     df[title_col] = ['MR' if str(x).strip().lower()   in title.get('MR')     else x for x in df[title_col]]
#     df[title_col] = ['MRS' if str(x).strip().lower()  in title.get('MRS')    else x for x in df[title_col]]
#     df[title_col] = ['MS' if str(x).strip().lower()   in title.get('MS')     else x for x in df[title_col]]
#     df[title_col] = ['MISS' if str(x).strip().lower() in title.get('MISS')   else x for x in df[title_col]]
#     df[title_col] = ['DR' if str(x).strip().lower()   in title.get('DR')     else x for x in df[title_col]]
#
#     df[title_col] = ['MR' if str(x[title_col]).lower()=='nan' and str(x[gender_col]).strip().lower() in gender.get('MALE') else x[title_col] for idx, x in df.iterrows()]
#     df[title_col] = ['MS' if str(x[title_col]).lower()=='nan' and str(x[gender_col]).strip().lower() in gender.get('FEMALE') else x[title_col] for idx, x in df.iterrows()]
#
#     df.loc[(df[gender_col].isnull()) & ((df[title_col]=='MRS') | (df[title_col]=='MS') | (df[title_col]=='MISS')), gender_col]='FEMALE'
#     df.loc[(df[gender_col].isnull()) & (df[title_col]=='MR'), gender_col]='MALE'
#     # other are blank
#     df[title_col] = [x if x in ('MR', 'MRS', 'MS', 'MISS', 'DR', )     else '' for x in df[title_col]]


# def process_title(df, title_col):
#     title = {
#         'MR':('mr', 'm r'),
#         'MRS':('mrs', ),
#         'MS':('ms', 'm s'),
#         'MISS':('miss', 'mis'),
#         'DR':('dr', ),
#         }
#     df[title_col] = ['MR' if str(x).strip().lower()   in title.get('MR')     else x for x in df[title_col]]
#     df[title_col] = ['MRS' if str(x).strip().lower()  in title.get('MRS')    else x for x in df[title_col]]
#     df[title_col] = ['MS' if str(x).strip().lower()   in title.get('MS')     else x for x in df[title_col]]
#     df[title_col] = ['MISS' if str(x).strip().lower() in title.get('MISS')   else x for x in df[title_col]]
#     df[title_col] = ['DR' if str(x).strip().lower()   in title.get('DR')     else x for x in df[title_col]]
#     # other are blank
#     df[title_col] = [x if x in ('MR', 'MRS', 'MS', 'MISS', 'DR', )     else '' for x in df[title_col]]

                                                                                            
def process_vincere_cand(df, logger):
    from common import vincere_candidate
    vc = vincere_candidate.Candidate(None)
    return vc.process_vincere_cand(df, logger)


def process_vincere_comp(df, logger):
    from common import vincere_company
    vc = vincere_company.Company(None)
    return vc.process_vincere_comp(df, logger)


def process_vincere_job(df, logger):
    """

    :rtype: object
    """
    from common import vincere_job
    vj = vincere_job.Job(None)
    return vj.process_vincere_job(df, logger)

def process_vincere_job_2(df, logger):
    """

    :rtype: object
    """
    from common import vincere_job
    vj = vincere_job.Job(None)
    job = vj.process_vincere_job(df, logger)
    job['position-companyId'].fillna('DEFAULT_COMPANY', inplace=True)
    # generate default contact external id base on company id (so that each company has max 1 default contact)
    job.loc[job['position-contactId'].isnull(), 'position-contactId'] = 'DEFAULT_CONTACT' + job.loc[job['position-contactId'].isnull(), 'position-companyId']
    default_contacts = job.loc[job['position-contactId'].str.contains('DEFAULT_CONTACT')][['position-contactId', 'position-companyId']].drop_duplicates()
    default_contacts.rename(columns={
        'position-contactId': 'contact-externalId',
        'position-companyId': 'contact-companyId',
    }, inplace=True)
    default_contacts['contact-firstName'] = 'DEFAULT'
    default_contacts['contact-lastName'] = 'CONTACT'
    default_contacts['rn'] = default_contacts.reset_index().index
    default_contacts['rn'] = default_contacts['rn'].astype(str)
    default_contacts['contact-email'] = 'default_email_' + default_contacts['rn'] + '@vincere.io'
    default_contacts.drop('rn', axis=1, inplace=True)
    return job, default_contacts

def process_vincere_contact(df):
    from common import vincere_contact
    vc = vincere_contact.Contact(None)
    return vc.process_vincere_contact(df)

def process_vincere_contact_2(df):
    from common import vincere_contact
    vc = vincere_contact.Contact(None)
    contact = vc.process_vincere_contact(df)
    contact['contact-companyId'].fillna('DEFAULT_COMPANY', inplace=True)
    default_company = contact.loc[contact['contact-companyId'] == 'DEFAULT_COMPANY'][['contact-companyId']].drop_duplicates()
    default_company.rename(columns={'contact-companyId': 'company-externalId'}, inplace=True)
    default_company['company-name'] = 'DEFAULT COMPANY'
    return contact, default_company

def process_vincere_jobapp(df):
    from common import vincere_job_application
    vja = vincere_job_application.JobApplication(None)
    return vja.process_jobapp_v2(df)


def process_vincere_jobapp_map_only(df):
    from common import vincere_job_application
    vja = vincere_job_application.JobApplication(None)
    return vja.jobapp_map_only(df)






