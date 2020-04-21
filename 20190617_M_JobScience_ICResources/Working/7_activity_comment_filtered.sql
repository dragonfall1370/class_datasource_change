-->>> EVENT after 17/10/2017
select
         e.id
       , a.id as "company_id", a.name as "company_name"
       , con.id as "contact_id", con.fullname as "contact_name"
       , can.id as "candidate_id", can.fullname as "candidate_name"
       , j.id as "job_id", j.name as "job_title"
       , -10 as "user_account_id"
       , 'comment' as "category"
       , 'candidate' as "type"
       , e.createddate::timestamp as "insert_timestamp"
       , concat_ws(chr(10), '[EVENT]'
              , coalesce('Company Name: ' || nullif(a.Name,'Candidates'),NULL)
              , coalesce('Contact Name: ' || nullif(con.fullname,''),NULL)
              , coalesce('Job Name: ' || nullif(j.Name,''),NULL)
              , coalesce('Candidate Name: ' || nullif(can.fullname,''),NULL)
              , coalesce('Assigned To: ' || nullif(u.fullname,''),null)
              , coalesce('Subject: ' || nullif(e.subject,''),null)
              , coalesce('Start: ' || nullif(e.activitydatetime,''),null)
              , coalesce('End: ' || nullif( (e.activitydatetime::timestamp + concat(e.durationinminutes,' minutes')::interval)::varchar ,''),null)
              , coalesce('Activity Currency: ' || nullif(e.currencyisocode,''),null)
              , coalesce('Location: ' || nullif( left(e.location,10) ,''),null)
              , coalesce('Show Time As: ' || nullif( left(e.showas,10) ,''),null)
              , coalesce('Comments: ' || nullif( left(e.description,10),''),null)
              ) as "content"
-- select count(*) --4726 -- select activitydatetime, durationinminutes
from Event e --where e.id = '00U0J000012DyCXUA0'
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u on u.id = e.OwnerId
left join (select id, name from Account) a on a.id = e.AccountId --COMPANY
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = e.WhoId --CONTACT
left join (select Id, name from ts2_job_c) j on j.id = e.WhatId --JOB
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')) can on can.id = e.WhoId --CANDIDATE
where e.createddate::timestamp > '2017-10-17' --4932 rows

-->>> FEEDPOST after 17/10/2017
select fp.Id
       --, fp.FeedItemId
       , a.id as "company_id", a.name as "company_name"
       , con.id as "contact_id", con.fullname as "contact_name"
       , can.id as "candidate_id", can.fullname as "candidate_name"
       , j.id as "job_id", j.name as "job_title"
--       , case when fp.ParentId in (select Id from Account) then fp.ParentId
--           else NULL end as CompanyExtID
--       , case when fp.ParentId in (select Id from Contact) then fp.ParentId
--           else NULL end as CandidateExtID
--       , case when fp.ParentId in (select Id from ts2_job_c) then fp.ParentId
--           else NULL end as JobExtID
       , -10 as "user_account_id"
       , 'comment' as "category"
       , 'candidate' as "type"
       --, case when fc.CreatedDate is not NULL then cast(fc.CreatedDate as datetime) else cast(fp.CreatedDate as datetime) end as "insert_timestamp"
       , fp.CreatedDate::timestamp as "insert_timestamp"
       --, fc.Status
       , concat_ws(chr(10), '[FEEDPOST]'
           , coalesce('Related To: ' || nullif(a1.name,''),NULL)
           , coalesce('Created Date: ' || nullif(left(fp.CreatedDate,10),''),NULL)
           , coalesce('Created By: ' || nullif(u.fullname,''),NULL)
           , coalesce('Body: ' || nullif(fp.Body,''),NULL)
           --, coalesce(chr(10) || 'Commented on: ' || nullif(left(fc.CreatedDate,10),''),NULL)
           --, coalesce('Created by: ' || nullif(u2.fullname,''),NULL)
           , coalesce('Status: ' || nullif(fc.Status,''),NULL)
           , coalesce('Comment: ' || nullif(fc.CommentBody,''),NULL)
           ) as "content"

-- select count(*) --306449 -- select *
from FeedPost fp
left join FeedComment fc on fc.FeedItemId = fp.FeedItemId
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u on u.Id = fp.CreatedById
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u2 on u2.Id = fc.CreatedById --comment created
left join (select id, name from Account) a1 on a1.id = fp.relatedrecordid --COMPANY
left join (select id, name from Account) a on a.id = fp.ParentId --COMPANY
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = fp.ParentId --CONTACT
left join (select Id, name from ts2_job_c) j on j.id = fp.ParentId --JOB
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')) can on can.id = fp.ParentId --CANDIDATE
--where can.id is not null and can.id in ('0031n00001rJaWrAAK') --,'0031n00001gTdpnAAC','0031n00001gTZKvAAO')
--where fc.FeedItemId = '0F71n00000CvAI4CAN'
--where a.id is not null
--order by fp.Id --3614 rows
where fp.CreatedDate::timestamp > '2017-10-17' --265218
and (a.id is not NULL or con.id is not NULL or can.id is not NULL or j.id is not NULL) --212959


-->>> TASK after 17/10/2017
select t.Id --known as activity ID
       , a1.id as "company_id", a1.name as "company_name"
       , con.id as "contact_id", con.fullname as "contact_name"
       , can.id as "candidate_id", can.fullname as "candidate_name"
       , j.id as "job_id", j.name as "job_title"
       --, case when t.AccountId in (select Id from Account) then t.AccountId else NULL end as CompanyExtID
       --, case when t.WhoId in (select Id from Contact /*where RecordTypeId = '012w0000000GS3f'*/) then t.WhoId else NULL end as CandidateExtID
       --, case when t.WhoId in (select Id from Contact /*where RecordTypeId = '012w0000000GS3g'*/) then t.WhoId else NULL end as ContactExtID
       --, case when t.WhatId in (select Id from ts2_job_c) then t.WhatId else NULL end as JobExtID
       --, e.FromName, e.FromAddress, e.ToAddress, e.CcAddress, e.BccAddress, e.Subject, e.TextBody
       , -10 as "user_account_id"
       , 'comment' as "category"
       , 'candidate' as "type"
       , t.createdDate::timestamp as "insert_timestamp"
       , concat_ws(chr(10), '[TASK]'
              , case
                     when t.EmailMessageId = '' or t.EmailMessageId is null
                     then concat_ws(
                            chr(10)
                            , coalesce('Related To: ' || nullif(a1.name,''),NULL)
                            , coalesce('Created Date: ' || nullif(t.CreatedDate,''),NULL)
                            , coalesce('Created By: ' || nullif(u2.fullname,''),NULL)                     
                            , coalesce('Assigned To: ' || nullif(u.fullname,''),NULL)
                            , coalesce('Client ' || nullif(con.fullname,''),NULL)
                            , coalesce('Due Date: ' || nullif(t.completeddatetime,''),NULL) --t.activitydate
                            , coalesce('Status: ' || nullif(t.Status,''),NULL)
                            , coalesce('Priority: ' || nullif(t.Priority,''),NULL)
                            , coalesce('Activity Currency: ' || nullif(t.currencyisocode,''),NULL)
                            , coalesce('Type: ' || nullif(t.type,''),NULL)
                            , coalesce('Subject: ' || nullif(t.Subject,''),NULL)
                            , coalesce('Comments ' || nullif(t.Description,''),NULL))
                      else
                      concat_ws(chr(10)
                          , coalesce('Related To: ' || nullif(a2.name,''),NULL)
                          , coalesce('Status: ' || nullif(e.Status,''),NULL)
                          , coalesce('Created Date: ' || nullif(e.CreatedDate,''),NULL)
                          , coalesce('Created By: ' || nullif(u3.fullname,''),NULL)                   
                          , coalesce('Message Date: ' || nullif(e.MessageDate,''),NULL)
                          , coalesce('From: ' || nullif( concat_ws(' ',e.FromName,e.FromAddress),''),NULL)
                          , coalesce('To: ' || nullif(e.ToAddress,''),NULL)
                          , coalesce('Cc: ' || nullif(e.CcAddress,''),NULL)
                          , coalesce('Bcc: ' || nullif(e.BccAddress,''),NULL)
                          , coalesce('Subject: ' || nullif(e.Subject,''),NULL)
                          , coalesce('Body: ' || chr(10) || nullif( regexp_replace(regexp_replace(e.textbody, E'<.*?>', '', 'g' ), E'&nbsp;', '', 'g') ,''),NULL)
                          ) 
                     end) as "content"
       --, e.Subject as "Email Subject"
-- select *
from Task t --where t.WhoId = '0030Y00001Y3DOXQA3'
left join EmailMessage e on e.Id = t.EmailMessageId --mail messages
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u on u.id = t.OwnerId
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u2 on u2.id = t.createdbyid
left join (select id, concat(firstname,' ',lastname) as fullname, email from "user") u3 on u3.id = e.createdbyid
left join (select id, name from Account) a1 on a1.id = t.AccountId --COMPANY
left join (select id, name from Account) a2 on a2.id = e.relatedtoid --COMPANY
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact where recordtypeid in ('0120Y0000013O5d')) con on con.id = t.WhoId --CONTACT
left join (select Id, name from ts2_job_c) j on j.id = t.WhatId --JOB
left join (select id, concat(firstname,' ',lastname) as fullname, email from contact where recordtypeid in ('0120Y0000013O5c','0120Y000000RZZV')) can on can.id = t.WhoId --CANDIDATE
where t.createdDate::timestamp > '2017-10-17' --|95624