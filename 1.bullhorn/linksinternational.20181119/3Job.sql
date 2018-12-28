
with 
-- MAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail5 where id in (39188,14248,30223)

--JOB DUPLICATION REGCONITION
, job (jobPostingID,clientID,title,starDate,rn) as (
	SELECT  a.jobPostingID as jobPostingID
		, b.clientID as clientID
		, iif(a.title <> '', ltrim(rtrim(a.title)), 'No JobTitle') as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,a.title,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
	where b.isPrimaryOwner = 1) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select * from job where title like '%no%'

-- NOTE
, note as (
        select JP.jobPostingID
	, Stuff(  
                 Coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
              + Coalesce('Client Charge Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
              + Coalesce('Employee Type: ' + NULLIF(cast(JP.customText4 as varchar(max)), '') + char(10), '')
              + Coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
              + Coalesce('Salary Unit: ' + NULLIF(cast(JP.salaryunit as varchar(max)), '') + char(10), '')
              + Coalesce('Perm or T&C Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
              + Coalesce('Pay Rate: ' + NULLIF(cast(JP.payRate as varchar(max)), '') + char(10), '')
              + Coalesce('Source: ' + NULLIF(JP.Source, '') + char(10), '')
	        , 1, 0, '') as note
        -- select count(*) -- select top 50 *
        from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        left join tmp_country on JP.countryID = tmp_country.CODE
        left join ( SELECT jobPostingID, STUFF((SELECT ',' + uc.name from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID where ja.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM ( select ja.jobPostingID,uc.name  from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID ) AS a GROUP BY a.jobPostingID ) ass on ass.jobPostingID = JP.jobPostingID
        )
--select count(*) from note --918 > 1103
--select * from note

-- Add placement information under job posting
, placementnote as (
        select jobPostingID
	, Stuff((select  --Add placement information under job posting
	        --+ Coalesce('Placement Status: ' + NULLIF(PL.status, '') + char(10), '')
	        + Coalesce('Report to: ' + NULLIF(cast(PL.reportTo as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Cost Center: ' + NULLIF(cast(PL.costCenter as varchar(max)), '') + char(10), '')
	        + Coalesce('Billing User: ' + NULLIF(cast(PL.billingUserID as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Start Date: ' + NULLIF(convert(varchar(10),PL.Datebegin,120), '') + char(10), '')
	        + Coalesce('Scheduled End: ' + NULLIF(convert(varchar(10),PL.dateEnd,120), '') + char(10), '')
	        + Coalesce('Employee type: ' + NULLIF(cast(PL.employeeType as varchar(max)), '') + char(10), '')
	        + Coalesce('Placement Fee(%): ' + NULLIF(cast(PL.fee as varchar(max)), '') + char(10), '')
	        /*+ Coalesce('Days Guaranteed: ' + NULLIF(cast(PL.daysGuaranteed as varchar(max)), '') + char(10), '')
	        + Coalesce('Days Pro-Rated: ' + NULLIF(cast(PL.daysProRated as varchar(max)), '') + char(10), '')
	        + Coalesce('Date Effective Date: ' + NULLIF(cast(PL.dateClientEffective as varchar(max)), '') + char(10), '')
	        + Coalesce('Bill Rate: ' + NULLIF(cast(PL.clientBillRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Pay Rate: ' + NULLIF(cast(PL.payRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Salary Unit: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
	        + Coalesce('Overtime Bill Rate: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
	        + Coalesce('Effective Date (pay rate info): ' + NULLIF(cast(PL.dateEffective as varchar(max)), '') + char(10), '')
	        + Coalesce('Overtime Pay Rate: ' + NULLIF(cast(PL.overtimeRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Date Added: ' + NULLIF(cast(PL.dateAdded as varchar(max)), '') + char(10), '')
	        + Coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '') */
	        + Coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10), '')
	        --+ Coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10) + 'Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
	        --+ Coalesce('BE: ' + NULLIF(cast(PL.userid as varchar(max)), '') + char(10), '')
	        -- select top 50 *
	        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
                left join bullhorn1.BH_UserContact UC on PL.billingUserID = UC.userID
                WHERE PL.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') as note
        from bullhorn1.BH_Placement a group by a.jobPostingID )
--select count(*) from placementnote
--select * from placementnote where jobPostingID in (17,28,30,50,92,115)
--select jobPostingID from placementnote group by jobPostingID having count(*) > 1

--DOCUMENT
, doc as (SELECT jobPostingID
                 , STUFF((SELECT DISTINCT ',' + concat(jobPostingFileID,fileExtension) from bullhorn1.View_JobPostingFile WHERE jobPostingID = a.jobPostingID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html','.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS files 
                 FROM (select jobPostingID from bullhorn1.View_JobPostingFile) AS a GROUP BY a.jobPostingID)


select --top 100
         a.jobPostingID as 'position-externalId'
	, b.clientID as 'position-contactId'
       --, a.clientUserID as '#UserID', cc.name as '#CompanyName', uc.firstname as '#ContactFirstName', uc.lastname as '#ContactLastName'
	, case when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
	, a.numOpenings as 'position-headcount'
	, mail5.email as 'position-owners'
	, a.type as 'position-employmentType#' --[FULL_TIME, PART_TIME, CASUAL]
/*	, case when a.employmentType is null then 'PERMANENT'
	       when a.employmentType = 'HRO' then 'INTERIM_PROJECT_CONSULTING'
	       when a.employmentType = 'Permanent' then 'PERMANENT'
	       when a.employmentType = 'Temp/Contract' then 'CONTRACT'
	       when a.employmentType = 'Willing to Temp' then 'TEMPORARY'
	       else '' end as '#position-type#' --[PERMANENT, INTERIM_PROJECT_CONSULTING, TEMPORARY, CONTRACT, TEMPORARY_TO_PERMANENT]*/
	, case
              when a.employmentType in (null,'Permanent') and a.salaryUnit in ('Salary','Yearly') then 'PERMANENT'
	       when a.employmentType = 'HRO' and a.salaryUnit in ('Per Day','Per Hour','Per Month','Per Week','Per Year') then 'INTERIM_PROJECT_CONSULTING'
	       when a.employmentType = 'Temp/Contract' and a.salaryUnit in ('Per Day','Per Hour','Per Month','Per Week','Per Year') then 'CONTRACT'
	       when a.employmentType = 'Willing to Temp' and a.salaryUnit in ('Per Day','Per Hour','Per Month','Per Week','Per Year') then 'TEMPORARY'
	       else '' end as 'position-type'
	--, a.salary as 'position-actualSalary'
	, a.customFloat1 as 'SalaryFrom'
	, a.salary as 'SalaryTo'
	, case
              when a.salaryUnit = 'Per Day' then 'Pay Interval - Daily'
              when a.salaryUnit = 'Per Hour' then 'Pay Interval – Hourly'
              when a.salaryUnit = 'Per Month' then 'Pay Interval – Monthly'
              when a.salaryUnit = 'Per Week' then 'Pay Interval – Weekly'
              when a.salaryUnit = 'Per Year' then 'Pay Interval – Monthly x 12 Months on Contract length.'
              when a.salaryUnit = 'Salary' then 'Permanent Salary Type - Annually'
              when a.salaryUnit = 'Yearly' then 'Permanent Salary Type - Annually'
              end as 'SalaryUnit'
	, case
              when a.customText5 = 'HK$' then 'HKD'
              when a.customText5 = 'MOP' then 'HKD'
              when a.customText5 = 'RMB' then 'CNY'
              when a.customText5 = 'S$' then 'SGD'
              when a.customText5 = 'TWD' then 'TWD'
              end as 'position-currency'
	, cast(a.publicDescription as varchar(max)) as 'position-publicDescription'
	, cast(a.description as varchar(max)) as 'position-internalDescription'
	, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	, convert(varchar(10),iif(a.isOpen in (null,0),getdate()-1,dateClosed),120) as 'position-endDate' --, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, doc.files as 'position-document'
	--, concat(note.note,placementnote.note) as 'position-note' --left(,32000)
	, note.note as 'position-note' --left(,32000)
       --, skills as 'skills'
-- select distinct employmentType -- select distinct Type -- select distinct salaryUnit -- select distinct customText5 -- select count(*) --2574 -- select top 100 startDate  -- select customFloat1, salary, customText5
from bullhorn1.BH_JobPosting a --where a.jobPostingID = 2539
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join job on a.jobPostingID = job.jobPostingID
left join mail5 ON a.userID = mail5.ID
left join note on a.jobPostingID = note.jobPostingID
left join placementnote  on a.jobPostingID = placementnote.jobPostingID
left join doc on a.jobPostingID = doc.jobPostingID
where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--and job.title <> ''
--and a.jobPostingID in (1847,3,2,33,80,27,130)

/*select
	 len(cast(a.publicDescription as varchar(max))) as 'position-publicDescription'
	, len(cast(a.description as varchar(max))) as 'position-internalDescription'
from bullhorn1.bh_jobposting a
*/
/*
select
	distinct case when a.employmentType is null then 'PERMANENT'
	       when a.employmentType = 'HRO' then 'INTERIM_PROJECT_CONSULTING'
	       when a.employmentType = 'Permanent' then 'PERMANENT'
	       when a.employmentType = 'Temp/Contract' then 'CONTRACT'
	       when a.employmentType = 'Willing to Temp' then 'TEMPORARY'
	       else '' end as 'position-type'
	, case
              when a.employmentType in (null,'Permanent') and a.salaryUnit in ('Salary','Yearly') then 'PERMANENT'
	       when a.employmentType = 'HRO' and a.salaryUnit in ('Per Day','Per Hour','Per Month','Per Week','Per Year') then 'INTERIM_PROJECT_CONSULTING'
	       when a.employmentType = 'Temp/Contract' and a.salaryUnit in ('Per Day','Per Hour','Per Month','Per Week','Per Year') then 'CONTRACT'
	       when a.employmentType = 'Willing to Temp' and a.salaryUnit in ('Per Day','Per Hour','Per Month','Per Week','Per Year') then 'TEMPORARY'
	       else '' end as 'position-type'	       
	, case
              when a.salaryUnit = 'Per Day' then 'Pay Interval - Daily'
              when a.salaryUnit = 'Per Hour' then 'Pay Interval – Hourly'
              when a.salaryUnit = 'Per Month' then 'Pay Interval – Monthly'
              when a.salaryUnit = 'Per Week' then 'Pay Interval – Weekly'
              when a.salaryUnit = 'Per Year' then 'Pay Interval – Monthly x 12 Months on Contract length.'
              when a.salaryUnit = 'Salary' then 'Permanent Salary Type - Annually'
              when a.salaryUnit = 'Yearly' then 'Permanent Salary Type - Annually'
              end as 'SalaryUnit'
from bullhorn1.bh_jobposting a
*/