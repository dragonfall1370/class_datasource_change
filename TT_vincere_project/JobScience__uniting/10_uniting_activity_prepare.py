# -*- coding: UTF-8 -*-
import common.logger_config as log
import configparser
import pathlib
import time
import datetime
import re
# from edenscott._edenscott_dtypes import *
import os
import psycopg2
import common.vincere_common as vincere_common
import common.vincere_custom_migration as vincere_custom_migration
import common.vincere_standard_migration as vincere_standard_migration
import sqlalchemy
from functools import reduce
from common import vincere_job_application
import pandas as pd

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('un_config.ini')
log_file = cf['default'].get('log_file')
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
mylog = log.get_info_logger(log_file)
sqlite_path = cf['default'].get('sqlite_path')
review_db = cf[cf['default'].get('dest_db')]


def html_to_text(html):
    from bs4 import BeautifulSoup
    # url = "http://news.bbc.co.uk/2/hi/health/2284783.stm"
    # html = urllib.urlopen(url).read()
    soup = BeautifulSoup(html)

    # kill all script and style elements
    for script in soup(["script", "style"]):
        script.extract()  # rip it out

    # get text
    text = soup.get_text()

    # break into lines and remove leading and trailing space on each
    lines = (line.strip() for line in text.splitlines())
    # break multi-headlines into a line each
    chunks = (phrase.strip() for line in lines for phrase in line.split("  "))
    # drop blank lines
    text = '\n'.join(chunk for chunk in chunks if chunk)

    return text


# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% extract data
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(review_db.get('user'), review_db.get('password'), review_db.get('server'), review_db.get('port'), review_db.get('database'))

dtype = {
        'company_id': sqlalchemy.types.INT,
        'contact_id': sqlalchemy.types.INT,
        'candidate_id': sqlalchemy.types.INT,
        'position_id': sqlalchemy.types.INT,
        'user_account_id': sqlalchemy.types.INT,
        'insert_timestamp': sqlalchemy.types.DateTime(),
        'content': sqlalchemy.types.NVARCHAR,
        'category': sqlalchemy.types.VARCHAR
    }
# %%
def task_to_db(index, limit):
    print('batch: ' + str(index + 1) + ' size: '+ str(limit))
    task = pd.read_sql("""
    select 
    t.AccountId as companyid, 
    t.WhatId as jobid, 
    t.WhoId as contactid, 
    t.WhoId as candidateid, 
    u.Email as owner,
    t.CreatedDate, 
    u.FirstName || ' ' || u.LastName as assignedTo, 
    t.Subject, 
    t.Status, 
    t.Priority, 
    t.Description,
    t.EmailMessageId,
    em.TextBody as ori_textbody,
    IFNULL(em.HtmlBody, em.TextBody) as TextBody,
    em.Subject as Email_Subject,
    em.FromAddress, em.FromName,
    em.ToAddress, em.CcAddress, em.BccAddress, t.ActivityOriginType,
    c.FirstName || ' ' || c.LastName as contactname
    from Tasks t 
    left join EmailMessage em on t.EmailMessageId = em.Id
    left join Contact c on t.WhoId = c.Id
    left join "User" u on t.OwnerId = u.Id
    where t.IsDeleted = 0
    limit %s offset %s
    """ % (str(limit), str(index*limit)), engine_sqlite)

    task['ori_textbody'] = task['ori_textbody'].apply(lambda x: ''.join(['\n', x]) if x else x)
    task['CreatedDate'] = pd.to_datetime(task['CreatedDate'])
    task['activity'] = task['CreatedDate'].apply(lambda x: datetime.datetime.strftime(x, '%d/%m/%Y'))

    task_is_email_message = task.loc[task['EmailMessageId'].notnull()]
    task_is_email_message['content'] = task_is_email_message[['Email_Subject', 'FromAddress', 'ToAddress', 'CcAddress', 'BccAddress', 'ori_textbody']].apply(lambda x: '\n'.join([': '.join([i for i in e if i]) for e in zip(['Subject', '\nFrom', 'To', 'Cc', 'Bcc', None], x) if e[1]]), axis=1)
    task_is_email_message['content'] = task_is_email_message['content'].apply(lambda x: x.replace('   ', '\n\n'))

    task_isnot_email_message = task.loc[task['EmailMessageId'].isnull()]
    task_isnot_email_message['content'] = task_isnot_email_message[['activity', 'assignedTo', 'contactname', 'Subject', 'Status', 'Priority', 'Description']].apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Activity', 'Assigned To', 'Contact', 'Subject', 'Status', 'Priority', 'Description'], x) if e[1]]), axis=1)

    # company_other_typenone = company.loc[(company['EmailMessageId'].isnull()) & (company['ActivityOriginType'].isnull())]
    # company_other_type1 = company.loc[(company['EmailMessageId'].isnull()) & (company['ActivityOriginType']==1)]
    # company_other_type2 = company.loc[(company['EmailMessageId'].isnull()) & (company['ActivityOriginType']==2)]
    # company_other_type5 = company.loc[(company['EmailMessageId'].isnull()) & (company['ActivityOriginType']==5)]
    # company_other_type1['content'] = company_other_type1[['Subject', 'Description']].apply(lambda x: '\n'.join(x), axis=1)
    # company_other_typenone['content'] = company_other_typenone[['activity', 'assignedTo', 'contactname', 'Subject', 'Status', 'Priority', 'Description']].apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Activity', 'Assigned To', 'Contact', 'Subject', 'Status', 'Priority', 'Description'], x) if e[1]]), axis=1)
    # company_other_type5['content'] = company_other_type5[['activity', 'assignedTo', 'contactname', 'Subject', 'Status', 'Priority', 'Description']].apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Activity', 'Assigned To', 'Contact', 'Subject', 'Status', 'Priority', 'Description'], x) if e[1]]), axis=1)

    all_activity = pd.concat([e for e in
        [
    task_is_email_message,
    task_isnot_email_message,
        ] if len(e)
    ])
    all_activity['insert_timestamp'] = pd.to_datetime(all_activity['CreatedDate'])
    all_activity['company_external_id'] = all_activity['companyid']
    all_activity['contact_external_id'] = all_activity['contactid']
    all_activity['candidate_external_id'] = all_activity['candidateid']
    all_activity['position_external_id'] = all_activity['jobid']


    re1 = vincere_activity.transform_activities_temp(all_activity, conn_str_ddb, mylog)
    re1 = re1.where(re1.notnull(), None)

    if index == 0:
        print('write to db 1st index '+ str(index))
        re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
    else:
        print('write to db not 1st index ' + str(index))
        re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
limit = 1000000
assert False
for i in range(7):
    task_to_db(i, limit)

# %%
feed = pd.read_sql("""
select fp.*, u.Email as owner
from FeedPost fp 
left join "User" u on fp.CreatedById = u.id
where fp.Type!='LinkPost';
""", engine_sqlite) # LinkPost feeds are dup with tasks
feed['Body'] = feed['Body'].apply(lambda x: x.replace('</p><p>', '</p>\n<p>') if x else '')
feed['Body'] = feed['Body'].apply(lambda x: html_to_text(x))
feed['content'] = feed[['Type', 'Title', 'Body',]].apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Type', 'Title', '', ], x) if e[1]]), axis=1)
feed['insert_timestamp'] = pd.to_datetime(feed['CreatedDate'])
feed['company_external_id'] = feed['ParentId']
feed['contact_external_id'] = feed['ParentId']
feed['candidate_external_id'] = feed['ParentId']
feed['position_external_id'] = feed['ParentId']
# assert False

feedcomment = pd.read_sql("""
select fc.FeedItemId, fc.CommentBody, fc.CreatedDate, u.Email as CreatedBy 
from FeedComment fc 
left join "User" u on fc.CreatedById = u.Id;
""", engine_sqlite)
feedcomment['CreatedDate'] = pd.to_datetime(feedcomment['CreatedDate'])
feedcomment['CreatedDate'] = feedcomment['CreatedDate'].apply(lambda x: datetime.datetime.strftime(x, '%d %B %Y at %H:%M'))
feedcomment['comment'] = feedcomment[['CreatedDate', 'CreatedBy', 'CommentBody',]].apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Comment Date', 'Comment By', '', ], x) if e[1]]), axis=1)
feedcomment = feedcomment.groupby('FeedItemId')['comment'].apply('\n\n'.join).reset_index()

feed = feed.merge(feedcomment, on='FeedItemId', how='left')
feed['comment'] = feed['comment'].where(feed['comment'].notnull(), None)
feed['content'] = feed[['content', 'comment']].apply(lambda x: '\n\n'.join([e for e in x if e]), axis=1)

re2 = vincere_activity.transform_activities_temp(feed, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)

# %%
event = pd.read_sql("""
select
	u.Email as owner
	, e.WhoId
	, e.WhatId
	, e.AccountId
	, e.Subject
	, e.CreatedDate
	, e.ActivityDateTime
	, e.DurationInMinutes
	, e.Description
from Event e
left join "User" u on e.OwnerId = u.Id
--and e.WhoId='0032400001N6JPaAAN';
""", engine_sqlite)
event.loc[event['ActivityDateTime'].isnull(), 'ActivityDateTime'] = event.loc[event['ActivityDateTime'].isnull(), 'CreatedDate']
event['ActivityDateTime'] = pd.to_datetime(event['ActivityDateTime'])
event['ActivityDateTimeEnd'] = event[['ActivityDateTime', 'DurationInMinutes']].apply(lambda x: x[0] + datetime.timedelta(minutes=int(x[1])), axis=1)
event['ActivityDateTime'] = event['ActivityDateTime'].apply(lambda x: datetime.datetime.strftime(x, '%d %B %Y %H:%M'))
event['ActivityDateTimeEnd'] = event['ActivityDateTimeEnd'].apply(lambda x: datetime.datetime.strftime(x, '%d %B %Y %H:%M'))
event['content'] = event[['Subject', 'ActivityDateTime', 'ActivityDateTimeEnd', 'Description']].apply(lambda x: '\n'.join([': '.join([i for i in e]) for e in zip(['Subject', 'Start', 'End', 'Description', ], x) if e[1]]), axis=1)
event.dtypes
event['insert_timestamp'] = pd.to_datetime(event['CreatedDate'])
event['company_external_id'] = event['AccountId']
event['contact_external_id'] = event['WhoId']
event['candidate_external_id'] = event['WhoId']
event['position_external_id'] = event['WhatId']

re3 = vincere_activity.transform_activities_temp(event, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)
re3.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
