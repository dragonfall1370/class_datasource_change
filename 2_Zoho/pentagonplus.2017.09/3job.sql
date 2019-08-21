
with
--JOB DUPLICATION REGCONITION
 job (JobOpeningId,ClientId,PostingTitle,DateOpened,rn) as (
	SELECT  a.JobOpeningId as JobOpeningId
		, a.clientID as clientID
		, a.PostingTitle as PostingTitle
		, CONVERT(varchar(10), CONVERT(date, a.DateOpened, 103), 120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.ClientId,a.PostingTitle,CONVERT(varchar(10), CONVERT(date, a.DateOpened, 103), 120) ORDER BY a.JobOpeningId) AS rn 
	from jobopenings a )
--select * from job

select 
        j.JobOpeningId as 'position-externalId',
        --j.AccountManagerId as 'position-owners',
        u.email as 'position-owners',
        case when j.JobType = 'Full Time' then 'FULL_TIME'
                when j.JobType = 'Part time' then 'PART_TIME'
                when j.JobType = 'Contract' then 'PART_TIME'
                else j.Jobtype
                end as 'position-employmentType',
        --j.PostingTitle as 'position-title',
	case when job.rn > 1 then concat(job.PostingTitle,' ',rn) else job.PostingTitle end as 'position-title',
        --convert(varchar(10),j.DateOpened,120) as 'position-startDate',
        CONVERT(varchar(10), CONVERT(date, j.DateOpened, 103), 120) as 'position-startDate',
        --convert(varchar(10),j.TargetDate,120) as 'position-endDate',
        nullif(CONVERT(varchar(10), CONVERT(date, j.TargetDate, 103), 120),'1900-01-01') as 'position-endDate',
        j.ClientId as 'Company(Company links with Contact)',
        j.NumberofPositions as 'position-headcount',
        ltrim(j.JobDescription) as 'position-publicDescription',
        j.ClientContact as 'position-contactId',
        --j.WorkExperience as 'position-internalDescription',
        --j.Skillset as 'position-internalDescription',
  	replace(Stuff(
	         Coalesce('Work Experience: ' + NULLIF(replace(cast(j.WorkExperience as varchar(max)),'year',' year'), '') + char(10), '')
	       + Coalesce(char(10) + 'Skill Set: ' + char(10) + NULLIF(cast(j.Skillset as varchar(max)), '') + char(10), '')
                , 1, 0, ''),char(10),'<br>') as 'position-internalDescription',
  	Stuff(
	         Coalesce('No of Candidates Associated: ' + NULLIF(cast(j.NoofCandidatesAssociated as varchar(max)), '') + char(10), '')
	       + Coalesce('Associated Tags: ' + NULLIF(cast(j.AssociatedTags as varchar(max)), '') + char(10), '')
               + Coalesce('Last Activity Time: ' + NULLIF(cast(j.LastActivityTime as varchar(max)), '') + char(10), '')
               + Coalesce('Job Opening Status: ' + NULLIF(cast(j.JobOpeningStatus as varchar(max)), '') + char(10), '')
               + Coalesce('Assigned Recruiter: ' + NULLIF(j.AssignedRecruiter_s, '') + char(10), '')
               + Coalesce('City: ' + NULLIF(j.City, '') + char(10), '')
               + Coalesce('Created By: ' + NULLIF(j.CreatedBy, '') + char(10), '')
               + Coalesce('Modified By: ' + NULLIF(j.ModifiedBy, '') + char(10), '')
               + Coalesce('Created Time: ' + NULLIF(j.CreatedTime, '') + char(10), '')
               + Coalesce('Modified Time: ' + NULLIF(j.ModifiedTime, '') + char(10), '')
               + Coalesce('Discipline: ' + NULLIF(j.Discipline, '') + char(10), '')
               + Coalesce('Job In-Process Status: ' + NULLIF(j.JobInProgressStatus, '') + char(10), '')
               + Coalesce('Country: ' + NULLIF(j.Country, '') + char(10), '')
               + Coalesce('Billing Amt Details: ' + NULLIF(j.BillingAmtDetails, '') + char(10), '')
               + Coalesce('Upper Salary Range: ' + NULLIF(j.UpperSalaryRange, '') + char(10), '')
               + Coalesce('Estimated Billing (RM): ' + NULLIF(j.EstimatedBilling_RM, '') + char(10), '')
               + Coalesce('Commencing Work: ' + NULLIF(j.CommencingWork, '') + char(10), '')
               + Coalesce('No of Candidates Hired: ' + NULLIF(j.NoofCandidatesHired, '') + char(10), '')
                , 1, 0, '') as note,     
        doc.docs as 'company-document'
from jobopenings j
left join users u on u.userid = j.AccountManagerId
--left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join clients c on a.parentid = c.ClientId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = j.JobOpeningId
left join (SELECT b.parentid, STUFF((SELECT DISTINCT ',' + replace(replace(a.FileName,',',''),'''','') from attachments a left join jobopenings c on a.parentid = c.JobOpeningId WHERE a.parentid = b.parentid FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS docs FROM attachments AS b GROUP BY b.parentid) doc on doc.parentid = j.JobOpeningId
left join job on j.JobOpeningId = job.JobOpeningId
--where j.ClientId <> ('Zrecruit_139304000000412029')

/*
select u.*
from jobopenings j
left join users u on u.userid = j.AccountManagerId



------------
-- NOTE COMMENT - INJECT TO VINCERE
select
        t.ParentId as 'externalId'
        , c.PostingTitle
        , cast('-10' as int) as 'user_account_id'
        , CONVERT(DATETIME, t.CreatedTime, 103) as 'comment_timestamp|insert_timestamp'
	, Stuff(
	                  Coalesce('Title: ' + NULLIF(cast(t.NoteTitle as varchar(max)), '') + char(10), '')
                        + Coalesce('Content: ' + char(10) + NULLIF(cast(t.NoteContent as varchar(max)), '') + char(10) + char(10), '')
                        --+ Coalesce('Created By: ' + NULLIF(cast(t.CreatedBy as varchar(max)), '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(cast(concat(u1.firstname,' ',u1.lastname,' ',u1.email) as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Modified By: ' + NULLIF(t.ModifiedBy, '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(cast(concat(u2.firstname,' ',u2.lastname,' ',u2.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(t.CreatedTime, '') + char(10), '')
                        + Coalesce('Modified Time: ' + NULLIF(t.ModifiedTime, '') + char(10), '')
                , 1, 0, '') as 'comment_body'
--select count(*) --1461
from notes t
left join jobopenings c on c.JobOpeningId = t.parentid
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.JobOpeningId is not null
and t.parentid in ('Zrecruit_139304000000053001','Zrecruit_139304000000055095','Zrecruit_139304000000055095','Zrecruit_139304000000055101','Zrecruit_139304000000069077','Zrecruit_139304000000121689','Zrecruit_139304000000050035','Zrecruit_139304000000479001')




------------
--CALL COMMENT - INJECT TO VINCERE
select
        t.RelatedTo as 'externalId'
        , cast('-10' as int) as userid
        , CONVERT(DATETIME, t.CreatedTime, 103) as 'comment_timestamp|insert_timestamp'
	, Stuff( 
	                  Coalesce('Subject: ' + NULLIF(cast(t.Subject as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Type: ' + NULLIF(cast(t.CallType as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Purpose: ' + NULLIF(cast(t.CallPurpose as varchar(max)), '') + char(10), '')
                        --+ Coalesce('Contact Name: ' + NULLIF(cast(t.ContactName as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Start Time: ' + NULLIF(cast(t.CallStartTime as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Duration: ' + NULLIF(cast(t.CallDuration as varchar(max)), '') + char(10), '')
                        + Coalesce('Description: ' + NULLIF(cast(t.Description as varchar(max)), '') + char(10), '')
                        + Coalesce('Call Result: ' + NULLIF(cast(t.CallResult as varchar(max)), '') + char(10), '')
                        + Coalesce('Created By: ' + NULLIF(cast(concat(u1.firstname,' ',u1.lastname,' ',u1.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Modified By: ' + NULLIF(cast(concat(u2.firstname,' ',u2.lastname,' ',u2.email) as varchar(max)), '') + char(10), '')
                        + Coalesce('Created Time: ' + NULLIF(t.CreatedTime, '') + char(10), '')
                        + Coalesce('Modified Time: ' + NULLIF(t.ModifiedTime, '') + char(10), '')
                        + Coalesce('Status: ' + NULLIF(cast(t.Status as varchar(max)), '') + char(10), '')
                        + Coalesce('Reminder: ' + NULLIF(cast(t.Reminder as varchar(max)), '') + char(10), '')
                , 1, 0, '') as 'comment_content'
-- select top 10 * --count(*)
from calls t
left join jobopenings c on c.JobOpeningId = t.RelatedTo
left join (select userid, email, firstname, lastname from users) u1 on u1.userid = t.CreatedBy
left join (select userid, email, firstname, lastname from users) u2 on u2.userid = t.ModifiedBy
where c.JobOpeningId is not null

*/