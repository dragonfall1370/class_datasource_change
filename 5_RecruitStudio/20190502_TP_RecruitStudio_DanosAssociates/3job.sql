
with
--JOB DUPLICATION REGCONITION
 job (JobNumber,companyid,JobTitle,starDate,rn) as (
	SELECT  a.JobNumber
		, iif(c.companyid is null, a.companyid, c.companyid ) as companyid
		, iif(a.JobTitle <> '', ltrim(rtrim(a.JobTitle)), 'No JobTitle') as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY iif(a.companyid is null or a.companyid = '', a.companyid, 'default' ), a.JobTitle, CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.JobNumber) AS rn 
	from dbo.vacancies a
	left join companies c on c.companyid = a.companyid
	--where a.JobNumber in ('167077-1544-17181','158494-3179-1487')
--	where b.isPrimaryOwner = 1
	) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select * from job
-- select count(*) from job where title = ''
--select v.companyid as 'position-companyId' , v.company from dbo.vacancies v	

-- CREATE DEFAULT CONTACT
/*select
	  distinct iif(c.contactid is not null, v.contactid, iif(job.companyid = 'default', 'default', concat('default',job.companyid)) ) as 'contact-externalId'
	, job.companyid as 'contact-companyId' --, v.company
	, 'Default Contact' as 'contact-lastname'
from dbo.vacancies v
left join job on job.JobNumber = v.jobnumber
left join (select replace(contactid,'Z','') as contactid from dbo.contacts where type in ('Client','Contact','Contractor') ) c on c.contactid = v.ContactId
where c.contactid is null --and job.companyid like '%104864-6273-11210'*/

 
-- FILES
, doc (id,doc) as (
        SELECT id
                     , STUFF((SELECT DISTINCT ',' + filename from attachments WHERE id = a.id /*and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf', '.html', '.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS doc 
        FROM (select id from attachments) as a GROUP BY a.id )
--select top 100 * from doc

, note as (
	select v.JobNumber
	, Stuff( 
	        Coalesce('ID: ' + NULLIF(cast(v.JobNumber as varchar(max)), '') + char(10), '')
	      --+ Coalesce('Owner: ' + NULLIF(cast(v.UserName as varchar(max)), '') + char(10), '')
              + Coalesce('Postcode: ' + NULLIF(convert(nvarchar(max),v.postcode), '') + char(10), '')
              + Coalesce('Contract Value > Margin per Day: ' + NULLIF(convert(nvarchar(max),v.marginperday), '') + char(10), '')
              + Coalesce('Contract Value > Days per Week: ' + NULLIF(convert(nvarchar(max),v.daysperweek), '') + char(10), '')
              + Coalesce('Contract Value > Duration in Weeks: ' + NULLIF(convert(nvarchar(max),v.durationinweeks), '') + char(10), '')
              --+ Coalesce('Contract Value > Predicted Value: ' + NULLIF(convert(nvarchar(max),v.), '') + char(10), '')
              + Coalesce('Signed Off (checkbox): ' + NULLIF(case when v.signedoff = 1 then 'Yes' when v.signedoff = 0 then 'No' else '' end, '') + char(10), '')
              + Coalesce('Progress: ' + NULLIF(convert(nvarchar(max),v.progressstage), '') + char(10), '')
              + Coalesce('Location: ' + NULLIF(convert(nvarchar(max),v.location), '') + char(10), '')
              + Coalesce('Sub Location: ' + NULLIF(convert(nvarchar(max),v.sublocation), '') + char(10), '')
              + Coalesce('Site: ' + NULLIF(convert(nvarchar(max),v.site), '') + char(10), '')
              + Coalesce('Department: ' + NULLIF(convert(nvarchar(max),v.department), '') + char(10), '')   
              , 1, 0, '') as note
from dbo.vacancies v )
--select  * from note




select --top 100
         v.jobnumber as 'position-externalId'
	--, iif(v.ContactId in (select replace(contactid,'Z','') as contactid from dbo.contacts where type in ('Client','Contact','Contractor') ), v.contactid, 'default' ) as 'position-contactId'
	--, iif(c.contactid is not null, v.contactid, 'default' ) as 'position-contactId' --, v.displayname --, c.firstname, c.lastname
	, iif(c.contactid is not null, v.contactid, iif(job.companyid = 'default', 'default', concat('default',job.companyid)) ) as 'position-contactId'
	--, iif(v.companyid in (null,''), 'default', v.companyid) as 'position-companyId' , v.company
       , job.companyid as 'position-companyId'
       --, a.clientUserID as '#UserID', cc.name as '#CompanyName', uc.firstname as '#ContactFirstName', uc.lastname as '#ContactLastName'
	, case when job.rn > 1 then concat(job.JobTitle,' ',rn) else job.JobTitle end as 'position-title' --, v.JobTitle as 'position-title'
	, v.vacanciescount as 'position-headcount'
	, u.email as 'position-owners' --, v.UserName, v.UserId
	--, v.PermTemp, v.FullPart, v.FullTimeJob --as 'position-employmentType#' --[FULL_TIME, PART_TIME, CASUAL]
	--, v.sector as 'INDUSTRY'
	--, v.KeySkills, v.HasSkills
	, case
	       when v.PermanentJob = 'Permanent' then 'PERMANENT'
	       when v.PermanentJob = 'Contract' then 'CONTRACT'
	       else '' end as 'position-type' --[PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT]
       , case when v.fulltimejob = 'Full Time' then 'FULL_TIME' else '' end as 'position-employmentType' --[FULL_TIME, PART_TIME, CASUAL]
	--, a.salary as 'position-actualSalary'
	, v.Salary as 'position-salaryFrom'
       , v.Salary1 as 'position-salaryTo'
	, v.Currency1 as 'position-currency'
	, jobskill.skills as 'position-skills'
	, cast(v.VacancyDetails as nvarchar(max)) as 'position-publicDescription'
	--, cast(a.description as varchar(max)) as 'position-internalDescription'
	, Stuff(
              + Coalesce('Discipline: ' + NULLIF(convert(nvarchar(max),v.discipline), '') + char(10), '')
              + Coalesce('Source: ' + NULLIF(convert(nvarchar(max),v.VacancySource), '') + char(10), '')
              + Coalesce('More > User Specified Data > Item 1: ' + NULLIF(convert(nvarchar(max),v.UserName), '') + char(10), '')
              + Coalesce('More > User Specified Data > Item 2: ' + NULLIF(convert(nvarchar(max),v.UserName2), '') + char(10), '')
              + Coalesce('More > User Specified Data > Item 3: ' + NULLIF(convert(nvarchar(max),v.UserName3), '') + char(10), '')
              + Coalesce('More > Other Data > Report To: ' + NULLIF(convert(nvarchar(max),v.reportto), '') + char(10), '')
              + Coalesce('More > Other Data > Work Hours: ' + NULLIF(convert(nvarchar(max),v.hoursofwork), '') + char(10), '')
              + Coalesce('Date Reg''d: ' + NULLIF(convert(nvarchar(max),v.RegDate), '') + char(10), '')
              + Coalesce('Targets CVs: ' + NULLIF(convert(nvarchar(max),v.targetcvs), '') + char(10), '')
              + Coalesce('Job Status: ' + NULLIF(convert(nvarchar(max),v.jobstatus), '') + char(10), '')
              + Coalesce('Fee %: ' + NULLIF(convert(nvarchar(max),v.Fee), '') + char(10), '')
              + Coalesce('Fee Amount > Currency: ' + NULLIF(convert(nvarchar(max),v.currency1), '') + char(10), '')
              --+ Coalesce('Fee Amount > Value: ' + NULLIF(convert(nvarchar(max),v.), '') + char(10), '')
              + Coalesce('Rebate Period: ' + NULLIF(convert(nvarchar(max),v.RebatePeriod), '') + char(10), '')
              --+ Coalesce('Target Int Date: ' + NULLIF(convert(nvarchar(max),v.), '') + char(10), '')
              + Coalesce('Benefits: ' + NULLIF(convert(nvarchar(max),v.Benefits), '') + char(10), '')
              + Coalesce('RS Ref: ' + NULLIF(convert(nvarchar(max),v.jobnumber), '') + char(10), '')
              + Coalesce('Vac''y No: ' + NULLIF(convert(nvarchar(max),v.vacancyid), '') + char(10), '')
              --+ Coalesce('Command (pulldown): ' + NULLIF(convert(nvarchar(max),v.), '') + char(10), '')
              + Coalesce('Job ID: ' + NULLIF(convert(nvarchar(max),v.jobidonweb), '') + char(10), '')
              + Coalesce('Job URL: ' + NULLIF(convert(nvarchar(max),v.joburl), '') + char(10), '')	      
              , 1, 0, '') as 'position-internalDescription'
	, v.StartDate as 'position-startDate' --, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	--, v.EndDate --, v.DateClosed --, convert(varchar(10),iif(a.isOpen in (null,0),getdate()-1,dateClosed),120) as 'position-endDate' --, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, convert(varchar(10),iif(v.jobstatus in ('Closed','Filled'),getdate()-1,getdate()+365),120) as 'position-endDate'
	, n.note as 'position-note'
	, d.doc as 'position-document'
--select count(*) --709 -- select v.jobnumber, count(*) -- select distinct v.fulltimejob --v.jobstatus, count(*) --PermanentJob -- select top 10 * -- select v.KeySkills, v.HasSkills
from dbo.vacancies v --group by v.jobstatus
left join job on job.JobNumber = v.jobnumber
left join dbo.users u on u.userid = v.userid
left join (select replace(contactid,'Z','') as contactid from dbo.contacts where type in ('Client','Contact','Contractor') ) c on c.contactid = v.ContactId
--left join dbo.contacts c on c.displayname = v.displayname
left join doc d on d.id = v.JobNumber
left join note n on n.JobNumber = v.jobnumber
left join (
       select v.jobnumber
                , string_agg(s.skill, ', ') as skills
                /*, sec.**/
       -- select v.KeySkills, v.HasSkills
       from dbo.vacancies v
       left join SkillInstances sk on sk.objectid = v.jobnumber
       left join dbo.skills s on s.skillid = sk.skillid
       left join dbo.sectors sec on sec.sectorid = s.sectorid
       where v.hasskills = 1 and sk.objectid is not null
       group by v.jobnumber
       ) jobskill on jobskill.jobnumber = v.jobnumber
--where v.jobnumber in ('158494-3179-1487','167077-1544-17181')





/*
-- LOG
select --top 10
         v.jobnumber as 'externalId'
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
left join LogDataIndex ldi on ldi.logitemid = l.logitemid
left join logdata ld on ld.logdataid = ldi.logdataid
left join dbo.users u on u.shortuser = l.shortuser
where l.logdate is not null

v.jobnumber = '695868-3484-18130'
where l.subject like '%DM Note - Spoke to Ian%'

*/