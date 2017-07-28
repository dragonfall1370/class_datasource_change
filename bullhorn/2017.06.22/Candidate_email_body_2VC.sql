with EmailBody as (

--EMAIL OF CANDIDATE - RECEIVER
select ca.candidateid, ca.userid,ca.recruiteruserid,ca.firstname,ca.lastname,ca.companyname,
        um.userMessageID,um.sendingUserID,um.subject, um.externalFrom, convert(nvarchar(max),um.comments) as comments , convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
        --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
        --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
        ,umf.name, umf.directory
from bullhorn1.BH_UserMessage um
left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
left join bullhorn1.Candidate ca on ca.userid = mr.userid
where mr.isDeleted = 0 and mr.isSpam = 0
and ca.userid is not null --and ca.candidateid = 565
--and convert(nvarchar(max),um.comments) != '' and um.comments is not null
--and convert(nvarchar(max),um.externalCC) != '' and um.externalCC is not null and convert(nvarchar(max),um.externalBCC) != '' and um.externalBCC is not null

UNION
--EMAIL OF CANDIDATE - SENDER

select ca.candidateid, ca.userid,ca.recruiteruserid,ca.firstname,ca.lastname,ca.companyname,
        um.userMessageID,um.sendingUserID,um.subject,um.externalFrom, convert(nvarchar(max),um.comments) as comments ,convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
        --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
        --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
        ,umf.name, umf.directory
from bullhorn1.BH_UserMessage um
left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
left join bullhorn1.Candidate ca on ca.userid = um.sendingUserID
where um.isSenderDeleted = 0
and ca.userid is not null) --and ca.candidateid = 565

select count(*) from EmailBody where candidateid between 124602 and 128601

 candidateid between 5 and 90000 -> 13388

 candidateid between 90001 and 12000 -> 0

 candidateid between 12000 and 122601 -> 45813

candidateid between 122602 and 124601 -> 21439

 candidateid between 124602 and 128601 -> 25914
 
 /* MAIN SCRIPT */
 ---CREATE INSERT CANDIDATE_COMMENTS
with EmailBody as (

--EMAIL OF CANDIDATE - RECEIVER
select ca.candidateid, ca.userid,ca.recruiteruserid,ca.firstname,ca.lastname,ca.companyname,
        um.userMessageID,um.sendingUserID,um.subject, um.externalFrom, convert(nvarchar(max),um.comments) as comments , convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
        --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
        --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
        ,umf.name, umf.directory
from bullhorn1.BH_UserMessage um
left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
left join bullhorn1.Candidate ca on ca.userid = mr.userid
where mr.isDeleted = 0 and mr.isSpam = 0
and ca.userid is not null --and ca.candidateid = 565
--and convert(nvarchar(max),um.comments) != '' and um.comments is not null
--and convert(nvarchar(max),um.externalCC) != '' and um.externalCC is not null and convert(nvarchar(max),um.externalBCC) != '' and um.externalBCC is not null

UNION
--EMAIL OF CANDIDATE - SENDER

select ca.candidateid, ca.userid,ca.recruiteruserid,ca.firstname,ca.lastname,ca.companyname,
        um.userMessageID,um.sendingUserID,um.subject,um.externalFrom, convert(nvarchar(max),um.comments) as comments ,convert(nvarchar(max),um.externalTo) as externalTo, convert(nvarchar(max),um.externalCC) as externalCC, convert(nvarchar(max),um.externalBCC) as externalBCC, um.SMTPSendDate
        --mr.userID,mr.folder,mr.recipientType, mr.email, mr.sendingUserID, mr.subjectSort
        --,mr.userID,mr.folder,mr.recipientType, mr.email, mr.externalFrom
        ,umf.name, umf.directory
from bullhorn1.BH_UserMessage um
left join bullhorn1.BH_MessageRecipient mr on mr.userMessageID = um.userMessageID
left join bullhorn1.BH_UserMessageFile umf on umf.userMessageID = um.userMessageID
left join bullhorn1.Candidate ca on ca.userid = um.sendingUserID
where um.isSenderDeleted = 0
and ca.userid is not null)

select candidateid 
, concat('insert into position_candidate_feedback (candidate_id, user_account_id, feedback_timestamp, comment_body, feedback_score, insert_timestamp, contact_method, json_relate_info, related_status)
values ( (select id from candidate where external_id = ''',candidateid,'''), ',-10,', now(), '''
,replace(concat('Date Created: ',convert(varchar,SMTPSendDate,120),char(10),'From: ',externalFrom,char(10),'To: ',externalTo,char(10),'CC: ',externalCC,char(10),'Subject: ',subject,char(10),char(10),'Content: ',comments),'''','''''')
,''', 0, now(), 4, ''[]'', 1);') as EmailSQL
from EmailBody where candidateid = 565
order by SMTPSendDate desc


--CREATE DUPLICATE TABLE
CREATE TABLE EmailSQL2
(
  candidate_id integer,
  user_account_id integer,
  feedback_timestamp timestamp without time zone,
  comment_body character varying NOT NULL,
  feedback_score double precision,
  insert_timestamp timestamp without time zone,
  contact_method integer,
  json_relate_info text,
  related_status smallint);
  
 CREATE TABLE EMAILSQL (
candidateid int,
EmailSQL text);