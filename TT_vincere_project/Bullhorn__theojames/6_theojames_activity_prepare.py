# -*- coding: UTF-8 -*-
import configparser
import os
import pathlib

import pandas as pd
import psycopg2
import sqlalchemy

import common.logger_config as log
import common.vincere_custom_migration as vincere_custom_migration
from common import vincere_common

# set the pandas dataframe so it doesn't line wrap
pd.set_option('expand_frame_repr', False)
pd.set_option('display.max_rows', 100)
pd.set_option('display.max_columns', 500)
pd.set_option('max_colwidth', 200)
pd.set_option('display.width', 1000)

# %% loading configuration
cf = configparser.RawConfigParser()
cf.read('theo_config.ini')
mylog = log.get_info_logger(cf['default'].get('log_file'))
data_folder = cf['default'].get('data_folder')
data_input = os.path.join(data_folder, 'data_input')
standard_file_upload = os.path.join(data_folder, 'standard_file_upload')
dest_db = cf[cf['default'].get('dest_db')]
sqlite_path = cf['default'].get('sqlite_path')
src_db = cf[cf['default'].get('src_db')]

# %% create the data folder if not exist
pathlib.Path(data_folder).mkdir(parents=True, exist_ok=True)
pathlib.Path(data_input).mkdir(parents=True, exist_ok=True)
pathlib.Path(standard_file_upload).mkdir(parents=True, exist_ok=True)

# %% connect to database
ddbconn = psycopg2.connect(host=dest_db.get('server'), user=dest_db.get('user'), password=dest_db.get('password'), database=dest_db.get('database'), port=dest_db.get('port'))
ddbconn.set_client_encoding('UTF8')
engine_sqlite = sqlalchemy.create_engine('sqlite:///%s' % sqlite_path, encoding='utf8')
engine_mssql = sqlalchemy.create_engine('mssql+pymssql://'+src_db.get('user')+':'+src_db.get('password')+'@'+src_db.get('server')+':1433'+'/' + src_db.get('database') + '?charset=utf8')

# %% company comment (note) to activities
comment = pd.read_sql("""
 select 
         contact.clientID as contact_externalid
       , contact_info.clientCorporationID as company_externalid
       , job.jobPostingID as job_externalid
       , cand.candidateID as candidate_externalid
       , comment.dateAdded
       , author.email as owner
       , comment.action
       , author.name as author
       , concat(contact_info.firstName, ' ', contact_info.lastName) as comment_about_name
       , contact_info.email as comment_about_email
       , comment.comments
from bullhorn1.BH_UserComment comment
left join bullhorn1.BH_Client contact on comment.userID = contact.userID
left join bullhorn1.BH_UserContact contact_info ON contact.userID = contact_info.userID
left join bullhorn1.BH_UserContact author ON comment.commentingUserID = author.userID
left join bullhorn1.BH_JobPosting job on comment.jobPostingID = job.jobPostingID
left join bullhorn1.Candidate cand on comment.userID = cand.userID
""", engine_mssql)

appointment = pd.read_sql("""
select
    contact.clientID as contact_externalid
     , cand.candidateID as candidate_externalid
     , apoint.jobPostingID as job_externalid
     , owner_info.email as owner
     , apoint.dateAdded
     , concat_ws(char(10), 'APPOINTMENT: '
    , coalesce('Contact: ' + NULLIF(coalesce(coalesce(nullif(contact_info.FirstName,'') + ' ', NULL) + coalesce(nullif(contact_info.LastName,'') + ' - ', NULL) + nullif(contact_info.email,''), NULL), ''), NULL) --a.clientUserID
    , coalesce('Candidate: ' + NULLIF(coalesce(coalesce(nullif(cand_info.FirstName,'') + ' ', NULL) + coalesce(nullif(cand_info.LastName,'') + ' - ', NULL) + nullif(cand_info.email,''), NULL), ''), NULL) --a.candidateUserID
    , coalesce('Job: ' + NULLIF(cast(job_info.title as nvarchar(max)), ''), NULL) --jobPostingID
    , coalesce('Communication Method: ' + NULLIF(convert(nvarchar(max) ,apoint.communicationMethod), ''), NULL)
    , coalesce('Owner: ' + NULLIF(coalesce(coalesce(nullif(owner_info.FirstName,'') + ' ', NULL) + coalesce(nullif(owner_info.LastName,'') + ' - ', NULL) + nullif(owner_info.email,''), NULL), ''), NULL)
    , coalesce('Lead: ' + NULLIF(coalesce(coalesce(nullif(lead_info.FirstName,'') + ' ', NULL) + coalesce(nullif(lead_info.LastName,'') + ' - ', NULL) + nullif(lead_info.email,''), NULL), ''), NULL)
    , coalesce('Date Begin: ' + NULLIF(convert(nvarchar(max), apoint.dateBegin, 120), ''), NULL)
    , coalesce('Date End: ' + NULLIF(convert(nvarchar(max),apoint.dateEnd, 120), ''), NULL)
    , coalesce('Type: ' + NULLIF(convert(nvarchar(max),apoint.type), ''), NULL) --CA.activePlacements
    , coalesce('Subject: ' + NULLIF(convert(nvarchar(max),apoint.subject), ''), NULL)
    , coalesce('Reminder: ' + NULLIF(convert(nvarchar(max),apoint.notificationMinutes), ''), NULL)
    , coalesce('Opportunity: ' + NULLIF(convert(nvarchar(max),opportunity_info.title), ''), NULL)
    , coalesce('Location: ' + NULLIF(convert(nvarchar(max),apoint.location), ''), NULL)
    --, coalesce('File Name: ' + NULLIF(convert(nvarchar(max),af.name), ''), NULL)
    , coalesce(char(10) + 'Description: ' + [bullhorn1].[fn_ConvertHTMLToText](NULLIF(convert(nvarchar(max),apoint.description), '')), NULL)
    ) as [content]
from bullhorn1.View_Appointment apoint
left join bullhorn1.View_AppointmentFile af on af.appointmentID = apoint.appointmentID
left join bullhorn1.BH_Client contact on contact.userID = apoint.ClientUserID
left join bullhorn1.BH_UserContact contact_info ON contact_info.userID = contact.userID
left join bullhorn1.BH_UserContact cand_info ON apoint.candidateUserID = cand_info.userID
left join bullhorn1.BH_UserContact owner_info ON apoint.userID = owner_info.userID
left join bullhorn1.BH_UserContact lead_info ON lead_info.userID = apoint.LeaduserID
left join bullhorn1.BH_JobPosting job_info on job_info.jobPostingID = apoint.jobPostingID
left join bullhorn1.BH_JobPosting opportunity_info on opportunity_info.jobPostingID = apoint.opportunityJobPostingID
left join bullhorn1.Candidate cand on apoint.candidateUserID = cand.userID
""", engine_mssql)

task = pd.read_sql("""
select 
        Cl.clientID as contact_externalid
      , a.candidateUserID as candidate_externalid
      , a.jobPostingID as job_externalid
      , UC1.email as owner
      , a.dateAdded
      , concat_ws(char(10), 'TASK: '
                , coalesce('Candidate: ' + NULLIF(coalesce(coalesce(nullif(UC2.FirstName,'') + ' ', NULL) + coalesce(nullif(UC2.LastName,'') + ' - ', NULL) + nullif(UC2.email,''), NULL), ''), NULL) --a.candidateUserID 
                --, coalesce('Assigned To: ' + NULLIF(cast(a.childTaskOwners as nvarchar(max)), ''), NULL)
                , coalesce('Assigned To: ' + NULLIF(coalesce(coalesce(nullif(UC4.FirstName,'') + ' ', NULL) + coalesce(nullif(UC4.LastName,'') + ' - ', NULL) + nullif(UC4.email,''), NULL), ''), NULL) --a.userID
                , coalesce('Contact: ' + NULLIF(coalesce(coalesce(nullif(UC1.FirstName,'') + ' ', NULL) + coalesce(nullif(UC1.LastName,'') + ' - ', NULL) + nullif(UC1.email,''), NULL), ''), NULL) --a.clientUserID 
                , coalesce('Due Date And Time: ' + NULLIF(convert(nvarchar(max), a.dateBegin, 120), ''), NULL)
                , coalesce('Date End: ' + NULLIF(convert(nvarchar(max),a.dateEnd, 120), ''), NULL)
                , coalesce('Job: ' + NULLIF(cast(j.title as nvarchar(max)), ''), NULL) --jobPostingID
                , coalesce('Lead: ' + NULLIF(coalesce(coalesce(nullif(UC3.FirstName,'') + ' ', NULL) + coalesce(nullif(UC3.LastName,'') + ' - ', NULL) + nullif(UC3.email,''), NULL), ''), NULL) --a.leadUserID
                --, coalesce('Reminder: ' + NULLIF(cast(a.notificationMinutes as nvarchar(max)), ''), NULL)
                , coalesce('Opportunity: ' + NULLIF(cast(a.opportunityJobPostingID as nvarchar(max)), ''), NULL)
                , coalesce('Placement: ' + NULLIF(cast(a.placementID as nvarchar(max)), ''), NULL)
                , coalesce('Priority: ' + NULLIF(cast(a.priority as nvarchar(max)), ''), NULL)
                --, coalesce('Visibility: ' + NULLIF(cast(a.isPrivate as nvarchar(max)), ''), NULL)
                , coalesce('Subject: ' + NULLIF(cast(a.subject as nvarchar(max)), ''), NULL)
                , coalesce('Type: ' + NULLIF(cast(a.type as nvarchar(max)), ''), NULL)
                , coalesce(char(10) + 'Description: ' + [bullhorn1].[fn_ConvertHTMLToText](NULLIF(convert(nvarchar(max),a.description), '')), NULL)
            ) as [content] 
from bullhorn1.View_Task a
left join bullhorn1.BH_Client Cl on Cl.userID = a.ClientUserID
left join bullhorn1.BH_UserContact UC1 ON Cl.userID = UC1.userID
left join bullhorn1.BH_UserContact UC2 ON a.candidateUserID = UC2.userID
left join bullhorn1.BH_JobPosting j on j.jobPostingID = a.jobPostingID
left join bullhorn1.BH_UserContact UC3 ON a.leadUserID = UC3.userID
left join bullhorn1.BH_UserContact UC4 ON a.userID = UC4.userID
""", engine_mssql)

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
temstr = r""".ExternalClass {width: 100%;}
td { font-family: 'Arial W01 Rounded', Helvetica, sans-serif; font-size: 12px; padding: 0px; }
.container {text-align: left;}
a, a:link, a:visited {color: #0570bc;text-decoration: none;}
body, .body{font-family: Arial;background-color: #f0f0f0;}
.paddedContainer {padding-left: 8px;padding-right: 8px;padding-top: 8px;padding-bottom: 8px;}
@media only screen and (min-width: 600px) {
tr.desktopOnly {display: table-row !important;}
table.desktopOnly {display: table !important;}
table.container tr.mobileOnly {font-size: inherit !important;}
}
@media only screen and (max-width: 599px) {
*.desktopOnly {display: none !important;}
table.container tr.mobileOnly {display: table-row !important;}
a.mobileOnly {display:block !important;}
a.mobileOnly table {max-height:initial !important;display: table !important;}
table.container {width: 300px;}
body, table.body {background-color: #ffffff;}
td.mobileOnlySpacer {height: 5px !important;}
tr.mobileOnly {font-size: inherit !important;display:table-row !important;}
tr.mobileOnly img.logo {max-height:initial !important;}
tr.mobileOnly img.advert {max-height :initial !important; max-width:initial !important;}
*.paddedContainer {padding-left: 10px !important;}
}
@media only screen and (max-width: 599px) {
table.container td.label-col {
white-space: normal !important;
min-width: 90px;
}
}"""
comment.comments = comment.comments.map(lambda x: html_to_text(x))
# comment['test'] = comment.comments.map(lambda x: x.replace(temstr, x))
comment['insert_timestamp'] = comment.dateAdded
comment['aboutwho'] = comment[['comment_about_name', 'comment_about_email']].apply(lambda x: ' - '.join([e for e in x if e]), axis='columns')
comment['content'] = comment[['action', 'author', 'aboutwho', 'comments']] \
    .apply(lambda x: '\n'.join([': '.join(e) for e in zip(['Action', 'Author', 'About Who', 'Comments'], x) if e[1]]), axis='columns')
comment.company_externalid = comment.company_externalid.where(comment.company_externalid.notnull(), None)
comment.company_externalid = comment.company_externalid.map(lambda x: str(int(x)) if x else x)
comment.contact_externalid = comment.contact_externalid.where(comment.contact_externalid.notnull(), None)
comment.contact_externalid = comment.contact_externalid.map(lambda x: str(int(x)) if x else x)
comment.candidate_externalid = comment.candidate_externalid.where(comment.candidate_externalid.notnull(), None)
comment.candidate_externalid = comment.candidate_externalid.map(lambda x: str(int(x)) if x else x)
comment.job_externalid = comment.job_externalid.where(comment.job_externalid.notnull(), None)
comment.job_externalid = comment.job_externalid.map(lambda x: str(int(x)) if x else x)

appointment
appointment['insert_timestamp'] = appointment.dateAdded
appointment.contact_externalid = appointment.contact_externalid.where(appointment.contact_externalid.notnull(), None)
appointment.contact_externalid = appointment.contact_externalid.map(lambda x: str(int(x)) if x else x)
appointment.job_externalid = appointment.job_externalid.where(appointment.job_externalid.notnull(), None)
appointment.job_externalid = appointment.job_externalid.map(lambda x: str(int(x)) if x else x)
appointment.candidate_externalid = appointment.candidate_externalid.where(appointment.candidate_externalid.notnull(), None)
appointment.candidate_externalid = appointment.candidate_externalid.map(lambda x: str(int(x)) if x else x)

task
task['insert_timestamp'] = task.dateAdded
task.contact_externalid = task.contact_externalid.where(task.contact_externalid.notnull(), None)
task.contact_externalid = task.contact_externalid.map(lambda x: str(int(x)) if x else x)
task.job_externalid = task.job_externalid.where(task.job_externalid.notnull(), None)
task.job_externalid = task.job_externalid.map(lambda x: str(int(x)) if x else x)
task.candidate_externalid = task.candidate_externalid.where(task.candidate_externalid.notnull(), None)
task.candidate_externalid = task.candidate_externalid.map(lambda x: str(int(x)) if x else x)

# %% transform activity
comment.rename(columns={'company_externalid': 'company_external_id', 'contact_externalid': 'contact_external_id', 'candidate_externalid': 'candidate_external_id', 'job_externalid': 'position_external_id'}, inplace=True)
appointment.rename(columns={'company_externalid': 'company_external_id', 'contact_externalid': 'contact_external_id', 'candidate_externalid': 'candidate_external_id', 'job_externalid': 'position_external_id'}, inplace=True)
task.rename(columns={'company_externalid': 'company_external_id', 'contact_externalid': 'contact_external_id', 'candidate_externalid': 'candidate_external_id', 'job_externalid': 'position_external_id'}, inplace=True)

from common import vincere_activity
conn_str_ddb = 'postgresql+psycopg2://{0}:{1}@{2}:{3}/{4}'.format(dest_db.get('user'), dest_db.get('password'), dest_db.get('server'), dest_db.get('port'), dest_db.get('database'))
re1 = vincere_activity.transform_activities_temp(comment, conn_str_ddb, mylog)
re1 = re1.where(re1.notnull(), None)
re2 = vincere_activity.transform_activities_temp(appointment, conn_str_ddb, mylog)
re2 = re2.where(re2.notnull(), None)
re3 = vincere_activity.transform_tasks_temp(task, conn_str_ddb, mylog)
re3 = re3.where(re3.notnull(), None)

# re1.content = re1.content.map(lambda x: x.replace(temstr, ''))

# %% load to temp db
dtype = {
    'company_id': sqlalchemy.types.INT,
    'contact_id': sqlalchemy.types.INT,
    'candidate_id': sqlalchemy.types.INT,
    'position_id': sqlalchemy.types.INT,
    'user_account_id': sqlalchemy.types.INT,
    'insert_timestamp': sqlalchemy.types.DateTime(),
    'content': sqlalchemy.types.NVARCHAR,
    'category': sqlalchemy.types.VARCHAR,
    'type': sqlalchemy.types.VARCHAR
}
re1.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='replace', dtype=dtype, index=False)
re2.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)
re3.to_sql(con=engine_sqlite, name='vincere_activity', if_exists='append', dtype=dtype, index=False)





















