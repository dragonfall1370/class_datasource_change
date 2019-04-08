
with 
-- FILES
 doc (id,doc) as (
        SELECT id
                     , STUFF((SELECT DISTINCT ',' + filename from attachments WHERE id = a.id /*and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS doc 
        FROM (select id from attachments) as a GROUP BY a.id )
--select top 100 * from doc

, note as (
	select v.JobNumber
	, Stuff( 
	          Coalesce('ID: ' + NULLIF(cast(v.JobNumber as varchar(max)), '') + char(10), '')
	      + Coalesce('Owner: ' + NULLIF(cast(v.UserName as varchar(max)), '') + char(10), '')
	      + Coalesce('Post Code: ' + NULLIF(cast(v.postcode as varchar(max)), '') + char(10), '')
	      + Coalesce('Progress: ' + NULLIF(cast(v.progressstage as varchar(max)), '') + char(10), '')
              , 1, 0, '') as note
from dbo.vacancies v )

--JOB DUPLICATION REGCONITION
, job (JobNumber,companyid,JobTitle,starDate,rn) as (
	SELECT  a.JobNumber
		, a.companyid
		, iif(a.JobTitle <> '', ltrim(rtrim(a.JobTitle)), 'No JobTitle') as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.companyid,a.JobTitle,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.JobNumber) AS rn 
	from dbo.vacancies a
--	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
--	where b.isPrimaryOwner = 1
	) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select * from job
-- select count(*) from job where title = ''

select
         distinct v.JobNumber as 'position-externalId'
	, iif(v.ContactId in (select replace(contactid,'Z','') as contactid from dbo.contacts where type in ('Client','Contact','Contractor') ), v.contactid, 'default' ) as 'position-contactId'--, v.displayname --, c.firstname, c.lastname
	, v.companyid as 'position-companyId' , v.company
       --, a.clientUserID as '#UserID', cc.name as '#CompanyName', uc.firstname as '#ContactFirstName', uc.lastname as '#ContactLastName'
	, case when job.rn > 1 then concat(job.JobTitle,' ',rn) else job.JobTitle end as 'position-title' --, v.JobTitle as 'position-title'
	, v.VacanciesCount as 'position-headcount'
	, u.email as 'position-owners' --, v.UserName, v.UserId
	--, v.PermTemp, v.FullPart, v.FullTimeJob --as 'position-employmentType#' --[FULL_TIME, PART_TIME, CASUAL]
	, v.sector as 'INDUSTRY'
	--, v.KeySkills, v.HasSkills
	, case
	       when v.PermanentJob = 'Permanent' then 'PERMANENT'
	       when v.PermanentJob = 'Contract' then 'CONTRACT'
	       else '' end as 'position-type' --[PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT]
	--, a.salary as 'position-actualSalary'
	, v.Salary as 'position-salaryFrom'
       , v.Salary1 as 'position-salaryTo'
	--, a.customtext1 as 'position-currency'
	, cast(v.VacancyDetails as varchar(max)) as 'position-publicDescription'
	--, cast(a.description as varchar(max)) as 'position-internalDescription'
	, Stuff( 
	          Coalesce('Source: ' + NULLIF(cast(v.VacancySource as varchar(max)), '') + char(10), '')
	      + Coalesce('Date Reg''d: ' + NULLIF(cast(v.RegDate as varchar(max)), '') + char(10), '')
	      + Coalesce('Fee %: ' + NULLIF(cast(v.Fee as varchar(max)), '') + char(10), '')
	      + Coalesce('Rebate Period: ' + NULLIF(cast(v.RebatePeriod as varchar(max)), '') + char(10), '')
              , 1, 0, '') as 'position-internalDescription'
	, v.StartDate as 'position-startDate' --, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	--, v.EndDate --, v.DateClosed --, convert(varchar(10),iif(a.isOpen in (null,0),getdate()-1,dateClosed),120) as 'position-endDate' --, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, convert(varchar(10),iif(v.jobstatus in ('Closed','Filled'),getdate()-1,getdate()+365),120) as 'position-endDate'
	, n.note as 'position-note'
	, d.doc as 'position-document'
-- select count(*) --599 -- select v.jobnumber, count(*) -- select distinct v.jobstatus, count(*) --PermanentJob -- select top 10 * -- select v.KeySkills, v.HasSkills
from dbo.vacancies v --group by v.jobstatus
left join job on job.JobNumber = v.jobnumber
left join dbo.users u on u.userid = v.userid
left join dbo.contacts c on replace(c.contactid,'Z','') = v.ContactId --group by v.jobnumber
--left join dbo.contacts c on c.displayname = v.displayname
left join doc d on d.id = v.JobNumber
left join note n on n.JobNumber = v.jobnumber
--where v.jobnumber in ('864821-7102-17251','666151-7194-17251')





/*
-- LOG
select 
         v.jobnumber as 'contact-externalId'
       , v.JobTitle as 'position-title'
       , cast('-10' as int) as 'user_account_id'
       , 'comment' as 'category'
       , 'job' as 'type'       
       , l.logdate as 'insert_timestamp'
	, Stuff( 
	          Coalesce('Name: ' + NULLIF(cast(u.username as varchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Subject: ' + NULLIF(cast(l.subject as varchar(max)), '') + char(10), '')
	      + Coalesce(char(10) + 'Log Item Text: ' + char(10) + NULLIF(cast(ld.text as varchar(max)), '') + char(10), '')
              , 1, 0, '')  as 'content'
       --, ld.*
-- select count(*)       
from dbo.vacancies v
left join dbo.logitems l on l.itemid  = v.jobnumber
left join dbo.logdata ld on ld.logdataid = l.logdataid
left join dbo.users u on u.shortuser = l.shortuser
where v.jobnumber = '695868-3484-18130'
where l.subject like '%DM Note - Spoke to Ian%'

*/