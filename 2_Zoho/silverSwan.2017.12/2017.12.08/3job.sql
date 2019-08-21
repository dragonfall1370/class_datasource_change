

with
attachment as ( SELECT ParentID, STUFF((SELECT ',' + replace(filename,',','') from JobAttachments WHERE ParentID = c.ParentID and filename is not NULL and filename <> '' FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS filename FROM JobAttachments as c GROUP BY c.ParentID )
-- SELECT filename,* from JobAttachments where filename like '%,%'
--select * from attachment

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
	left join (select ClientId, ClientName from Clients) cl on cl.ClientName = a.ClientName )
--select * from job

select
  j.JobOpeningId As 'position-externalId'
, case j.JobType
        when 'Contract' then 'CASUAL'
        when 'Full time' then 'FULL_TIME'
        when 'Part time' then 'PART_TIME'
        when 'Temporary' then 'PART_TIME'
        when 'Temporary to Permanent' then 'PART_TIME'
        end As 'position-employmentType' /* This field only accepts FULL_TIME, PART_TIME, CASUAL */
, case when job.rn > 1 then concat(job.PostingTitle,' ',rn) else job.PostingTitle end as 'position-title' --, j.PostingTitle As 'position-title'
--, j.JobOpeningStatus As 'position-status, j.position-note'
,CONVERT(date, CONVERT(datetime, replace(convert(varchar(50),j.DateOpened),'',''),120), 103) as 'position-startDate' --, j.DateOpened As 'position'
,CONVERT(date, CONVERT(datetime, replace(convert(varchar(50),j.TargetDate),'',''),120), 103) as 'position-endDate' --, j.TargetDate As 'position-endDate'
, cl.ClientId As 'position-companyId' --, j.ClientName As 'External Company ID'
, case when (ltrim(j.ContactID) = '' or j.ContactID is null) then concat(cl.ClientId,'','_default') else ltrim(j.ContactID) end as 'position-ContactID' --, j.ContactID As 'position-ContactID'
, j.AssignedRecruiter_s As 'position-owners'
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
        , ltrim(Stuff(    Coalesce('AccountManager: ' + NULLIF(j.AccountManager, '') + char(10), '')
                        + Coalesce('Is Hot JobOpening: ' + NULLIF(j.IsHotJobOpening, '') + char(10), '')
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
-- select count(*)
from JobOpenings J
left join (select ClientId, ClientName from Clients) cl on cl.ClientName = j.ClientName
--left join (select ContactId, FullName from Contacts) co on co.FullName = c.ContactId
left join attachment a on a.ParentId = j.JobOpeningId
left join job on j.JobOpeningId = job.JobOpeningId

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
left join (select ClientId, ClientName from Clients) cl on cl.ClientName = j.ClientName
)
select distinct [contact-externalId],[companyId],lastname from t
*/

/*
----------

with comment as (
        select
                   j.ParentID, j.CreatedTime
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from ClientsNotes J
UNION ALL
        select
                   j.ParentID, j.CreatedTime
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from JobNotes J --left join JobOpenings jo on jo.JobOpeningId = j.ParentID
UNION ALL
        select
                   j.ParentID, j.CreatedTime
                 , ltrim(Stuff(     Coalesce('Note Owner: ' + NULLIF(j.NoteOwner, '') + char(10), '')
                                + Coalesce('Note Title: ' + NULLIF(j.NoteTitle, '') + char(10), '')
                                + Coalesce('Note Content: ' + NULLIF(j.NoteContent, '') + char(10), '')
                                + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
                                + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
                        , 1, 0, '') ) as 'comment'
        -- select * 
        from CandidatesNotes J
)
--select top 1000 * from comment

select
        c.JobOpeningId as 'externalId'
        , cast('-10' as int) as 'user_account_id'
        , CONVERT(datetime, replace(convert(varchar(50),comment.CreatedTime),'',''),120) 'comment_timestamp|insert_timestamp'
        , comment.comment  as 'comment_body'
from JobOpenings c
left join comment on comment.ParentID = c.JobOpeningId where comment.comment is not null

*/