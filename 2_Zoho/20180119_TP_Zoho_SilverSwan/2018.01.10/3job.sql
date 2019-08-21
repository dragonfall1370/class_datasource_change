
with
attachment as ( SELECT ParentID, STUFF((SELECT ',' + replace(filename,',','') from Attachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM Attachments as c GROUP BY c.ParentID )
-- SELECT filename,* from JobAttachments where filename like '%,%'
--select * from attachment a left join JobOpenings c on c.JobOpeningID = a.ParentID where c.JobOpeningID is not null

--JOB DUPLICATION REGCONITION
, job (JobOpeningId,ClientId,PostingTitle,DateOpened,rn) as (
	SELECT  a.JobOpeningId as JobOpeningId
		, cl.clientID as clientID
		, a.PostingTitle as PostingTitle
		, convert(varchar(10),CONVERT(date, CONVERT(datetime, replace(convert(varchar(50),a.DateOpened),'',''),120), 103) ) as starDate
		--, CONVERT(date, replace(convert(varchar(50),a.DateOpened),'',''),120) as starDate
		--, CONVERT(varchar(10), CONVERT(date, a.DateOpened, 103), 120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY cl.ClientId,a.PostingTitle,convert(varchar(10),CONVERT(date, CONVERT(datetime, replace(convert(varchar(50),a.DateOpened),'',''),120), 103) ) ORDER BY a.JobOpeningId) AS rn 
	from jobopenings a 
	left join (select ClientId, ClientName from Clients) cl on cl.ClientId = a.ClientId )
--select * from job

select
  j.JobOpeningId As 'position-externalId'
, 'FULL_TIME' as 'position-employmentType' /* This field only accepts FULL_TIME, PART_TIME, CASUAL */
, case j.JobType
        when 'Contract' then 'CONTRACT'
        when 'Full time' then 'PERMANENT'
        when 'Seasonal' then 'INTERIM_PROJECT_CONSULTING'
        when 'Temporary' then 'TEMPORARY'
        when 'Temporary to Permanent' then 'TEMPORARY_TO_PERMANENT'
        end As 'position-Type' /* This field only accepts PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT */
, case when job.rn > 1 then concat(job.PostingTitle,' ',rn) else job.PostingTitle end as 'position-title' --, j.PostingTitle As 'position-title'
--, j.JobOpeningStatus As 'position-status, j.position-note'
,CONVERT(date, CONVERT(datetime, replace(convert(varchar(50),j.DateOpened),'',''),120), 103) as 'position-startDate' --, j.DateOpened As 'position'
,CONVERT(date, CONVERT(datetime, replace(convert(varchar(50),j.TargetDate),'',''),120), 103) as 'position-endDate' --, j.TargetDate As 'position-endDate'
, cl.ClientId As 'position-companyId' --, j.ClientName As 'External Company ID'
, case when (ltrim(j.ContactID) = '' or j.ContactID is null) then concat(cl.ClientId,'','_default') else ltrim(j.ContactID) end as 'position-ContactID' --, j.ContactID As 'position-ContactID'
, u0.email As 'position-owners' --, j.AssignedRecruiter_s 
, j.NumberofPositions As 'position-headcount'
, j.JobDescription As 'position-publicDescription'

/*, j.WorkExperience As 'position-internalDescription'
, j.Skillset As 'position-internalDescription' */
        , ltrim(Stuff(    Coalesce('Work Experience: ' + NULLIF(j.WorkExperience, '') + char(10), '')
                        + Coalesce('Skill Set: ' + NULLIF(j.Skillset, '') + char(10), '')
                , 1, 0, '') ) as 'position-internalDescription'
--note
/*, j.AccountManager As 'position-note'
, j.IsHotJobOpening As 'position-note'
, j.NoofCandidatesAssociated As 'position-note'
, j.AssociatedTags As 'position-note'
, j.LastActivityTime As 'position-note'
, j.JobOpeningStatus As 'position-status, j.position-note'
, j.CreatedBy As 'position-note'
, j.ModifiedBy As 'position-note'
, j.CreatedTime As 'position-note'
, j.ModifiedTime As 'position-note'
, j.Stage As 'position-note'
, j.IsAttachmentPresent As 'position-note'
, j.IsLocked As 'position-note'
, j.Country As 'position-note'
, j.Location As 'position-note'
, j.NoofCandidatesHired As 'position-note' */
        , ltrim(Stuff(    Coalesce('AccountManager: ' + NULLIF(u.email, '') + char(10), '') --j.AccountManagerID
                        + Coalesce('Is Hot JobOpening: ' + NULLIF(j.IsHotJobOpening, '') + char(10), '')
                        + Coalesce('Publish: ' + NULLIF(j.Publish, '') + char(10), '')
                        + Coalesce('No of Candidates Associated: ' + NULLIF(j.NoofCandidatesAssociated, '') + char(10), '')
                        + Coalesce('Associated Tags: ' + NULLIF(j.AssociatedTags, '') + char(10), '')
                        + Coalesce('Last Activity Time: ' + NULLIF(j.LastActivityTime, '') + char(10), '')
                        + Coalesce('Job Opening Status: ' + NULLIF(j.JobOpeningStatus, '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(j.ModifiedBy, '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        + Coalesce('Modified Time: ' + NULLIF(j.ModifiedTime, '') + char(10), '')
                        + Coalesce('Stage: ' + NULLIF(j.Stage, '') + char(10), '')
                        + Coalesce('Is Attachment Present: ' + NULLIF(j.IsAttachmentPresent, '') + char(10), '')
                        + Coalesce('IsLocked: ' + NULLIF(j.IsLocked, '') + char(10), '')
                        + Coalesce('Country: ' + NULLIF(j.Country, '') + char(10), '')
                        + Coalesce('Location: ' + NULLIF(j.Location, '') + char(10), '')
                        + Coalesce('No of Candidates Hired: ' + NULLIF(j.NoofCandidatesHired, '') + char(10), '')
                , 1, 0, '') ) as 'position-note'
, a.filename as 'position-document'
-- select count(*) -- select distinct jobtype -- select top 100 *
from JobOpenings J
left join (select ClientId, ClientName from Clients) cl on cl.ClientId = j.ClientId
--left join (select ContactId, FullName from Contacts) co on co.FullName = c.ContactId
left join attachment a on a.ParentId = j.JobOpeningId
left join job on j.JobOpeningId = job.JobOpeningId
left join users u0 on u0.userid = j.AssignedRecruiter_s
left join users u on u.userid = j.AccountManagerID
/*
-- CREATE DEFAULT CONTACT FOR JOBS WHICH NOT LINKED TO ANY CONTACT
with t as (
select
  JobOpeningId As 'job-externalId'
, case when (ltrim(cl.ClientId) = '' or cl.ClientId is null) then 'company_default' else ltrim(cl.ClientId) end as 'companyId' --, cl.ClientId As 'company-externalId' --, j.ClientName As 'External Company ID'
, case when (ltrim(j.ContactID) = '' or j.ContactID is null) then concat(cl.ClientId,'','_default') else ltrim(j.ContactID) end as 'contact-externalId' --, j.ContactID As 'position-ContactID'
, 'Default Contact' as 'lastname'
-- select count(*)
from JobOpenings J
left join (select ClientId, ClientName from Clients) cl on cl.ClientId = j.ClientId
)
select distinct [contact-externalId],[companyId],lastname from t where [contact-externalId] like '%_default%'
*/


/*
----
----------

with comment as (
        select
                   j.ParentID
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Note Owner: ' + NULLIF(u1.email, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u2.email, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select top 100 * 
        from Notes J
        left join (select * from users) u1 on u1.userid = j.NoteOwnerId
        left join (select * from users) u2 on u2.userid = j.CreatedBy
        --left join Contacts c on c.ContactID = j.ParentID where c.ContactID is not null
UNION ALL
        select
                   j.EntityId
                 , CONVERT(datetime, replace(convert(varchar(50),j.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                                + Coalesce('Modified Time: ' + NULLIF(j.ModifiedTime, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Subject: ' + NULLIF(j.Subject, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from Emails J
        ---left join Contacts c on c.ContactID = j.EntityId where c.ContactID is not null
UNION ALL
        select
                   i.JobOpeningId
                 --, i.CandidateId
                 , CONVERT(datetime, replace(convert(varchar(50),i.CreatedTime),'',''),120) as 'comment_timestamp|insert_timestamp'
                 , ltrim(Stuff(   'INTERVIEW NOTES:' + char(10) +
                                + Coalesce('Company : ' + NULLIF(j1.ClientName, '') + char(10), '') --i.ClientId
                                + Coalesce('Consultant: ' + NULLIF(u1.email, '') + char(10), '') --i.InterviewOwnerId
                                + Coalesce('Type: ' + NULLIF(i.Type, '') + char(10), '')
                                + Coalesce('Job Name: ' + NULLIF(j2.PostingTitle, '') + char(10), '') --i.JobOpeningId
                                + Coalesce('Interview Subject: ' + NULLIF(i.InterviewName, '') + char(10), '')
                                + Coalesce('Interviewer: ' + NULLIF(u2.email, '') + char(10), '') --i.Interviewer
                                + Coalesce('Location: ' + NULLIF(i.Location, '') + char(10), '')
                                + Coalesce('From: ' + NULLIF(i.From_, '') + char(10), '')
                                + Coalesce('To: ' + NULLIF(i.To_, '') + char(10), '')
                                + Coalesce('Comments: ' + NULLIF(i.ScheduleComments, '') + char(10), '')
                                + Coalesce('Created Date Time: ' + NULLIF(i.CreatedTime, '') + char(10), '')
                                + Coalesce('Modified Date Time: ' + NULLIF(i.ModifiedTime, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(u3.email, '') + char(10), '') --i.CreatedBy
                                + Coalesce('Modified By: ' + NULLIF(u4.email, '') + char(10), '') --i.ModifiedBy
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from Interviews i
        left join (select * from Users) u1 on u1.UserID = i.InterviewOwnerId
        left join (select * from Users) u2 on u2.UserID = i.InterviewName
        left join (select * from Users) u3 on u3.UserID = i.CreatedBy
        left join (select * from Users) u4 on u4.UserID = i.ModifiedBy
        left join (select ClientId,ClientName from Clients) j1 on j1.ClientId = i.ClientId --where j1.ClientId is not null
        left join (select JobOpeningId,PostingTitle from JobOpenings) j2 on j2.JobOpeningID = i.JobOpeningId --where j2.JobOpeningId is not null
        --left join (select CandidateId,FullName from Candidates) j3 on j3.CandidateId = i.CandidateId here j3.CandidateId is not null
)
--select count(*) from comment where comment.comment is not null --8157
select
        c.JobOpeningId as 'externalId'
        , cast('-10' as int) as 'user_account_id'
        --, CONVERT(datetime, replace(convert(varchar(50),comment.CreatedTime),'',''),120) 'comment_timestamp|insert_timestamp'
        , [comment_timestamp|insert_timestamp]
        , comment.comment  as 'comment_body'
from JobOpenings c
left join comment on comment.ParentID = c.JobOpeningId 
where c.JobOpeningId is not null and comment.comment is not null

*/