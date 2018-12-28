
with 
-- MAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail5 where id in (39188,14248,30223)

--JOB DUPLICATION REGCONITION
, job (jobPostingID,clientID,title,starDate,rn) as (
	SELECT  a.jobPostingID as jobPostingID
		, b.clientID as clientID
		, a.title as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,a.title,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
	where b.isPrimaryOwner = 1) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select count(*) from job where title = ''


--DOCUMENT
, doc as (SELECT jobPostingID
                 , STUFF((SELECT DISTINCT ',' + concat(jobPostingFileID,fileExtension) from bullhorn1.View_JobPostingFile WHERE jobPostingID = a.jobPostingID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS files 
                 FROM (select jobPostingID from bullhorn1.View_JobPostingFile) AS a GROUP BY a.jobPostingID)



-- NOTE
, note as (
        select JP.jobPostingID
	, Stuff(  Coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
              + Coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
              + Coalesce('Benefits: ' + NULLIF(cast(JP.benefits as varchar(max)), '') + char(10), '')
              + Coalesce('Certification Requirements: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
              + Coalesce('Suburb: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
              + Coalesce('Client Bill Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
              + Coalesce('Primary Timesheet Approver: ' + NULLIF(cast(JP.correlatedCustomText1 as varchar(max)), '') + char(10), '')
              + Coalesce('Secondary Timecard Approver: ' + NULLIF(cast(JP.correlatedCustomText2 as varchar(max)), '') + char(10), '')
              + Coalesce('Purchase Order Number: ' + NULLIF(cast(JP.correlatedCustomText3 as varchar(max)), '') + char(10), '')
              + Coalesce('Country: ' + NULLIF(cast(tmp_country.COUNTRY as varchar(max)), '') + char(10), '') --JP.countryID 
              + Coalesce('Scheduled End: ' + NULLIF(cast(JP.dateEnd as varchar(max)), '') + char(10), '')
              + Coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
              + Coalesce('Job Description: ' + NULLIF(cast([dbo].[fn_ConvertHTMLToText](JP.description) as varchar(max)), '') + char(10), '')
              + Coalesce('Education Requirements: ' + NULLIF(cast(JP.educationDegree as varchar(max)), '') + char(10), '')
              + Coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
              + Coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
              --+ Coalesce('Address: ' + NULLIF(cast(JP.fullAddress as varchar(max)), '') + char(10), '')
              + Coalesce('Hourly Commitment: ' + NULLIF(cast(JP.hoursPerWeek as varchar(max)), '') + char(10), '')
              + Coalesce('Interview Required?: ' + NULLIF(cast(JP.isInterviewRequired as varchar(max)), '') + char(10), '')
              + Coalesce('Pay Rate: ' + NULLIF(cast(JP.payRate as varchar(max)), '') + char(10), '')
              + Coalesce('Reason Closed: ' + NULLIF(cast(JP.reasonClosed as varchar(max)), '') + char(10), '')
              + Coalesce('Pay Rate: ' + NULLIF(cast(JP.salaryUnit as varchar(max)), '') + char(10), '')
              + Coalesce('State: ' + NULLIF(cast(JP.state as varchar(max)), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')
              + Coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
              + Coalesce('Visa Sponsorship Provided: ' + NULLIF(cast(JP.willSponsor as varchar(max)), '') + char(10), '')
              + Coalesce('Minimum Experience: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
              + Coalesce('Post Code: ' + NULLIF(cast(JP.zip as varchar(max)), '') + char(10), '')
	        , 1, 0, '') as note
        -- select count(*) -- select top 50 *
        from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
        --left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        --left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        left join tmp_country on JP.countryID = tmp_country.CODE
        --left join ( SELECT jobPostingID, STUFF((SELECT ',' + uc.name from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID where ja.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM ( select ja.jobPostingID,uc.name  from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID ) AS a GROUP BY a.jobPostingID ) ass on ass.jobPostingID = JP.jobPostingID
        )
--select count(*) from note --918 > 1103
--select * from note

-- Add placement information under job posting
/*, placementnote as (
        select jobPostingID
	, Stuff((select  *//* Add placement information under job posting *//*
	        --+ Coalesce('Placement Status: ' + NULLIF(PL.status, '') + char(10), '')
	        + Coalesce('Report to: ' + NULLIF(cast(PL.reportTo as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Cost Center: ' + NULLIF(cast(PL.costCenter as varchar(max)), '') + char(10), '')
	        + Coalesce('Billing User: ' + NULLIF(cast(PL.billingUserID as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Start Date: ' + NULLIF(convert(varchar(10),PL.Datebegin,120), '') + char(10), '')
	        + Coalesce('Scheduled End: ' + NULLIF(convert(varchar(10),PL.dateEnd,120), '') + char(10), '')
	        + Coalesce('Employee type: ' + NULLIF(cast(PL.employeeType as varchar(max)), '') + char(10), '')
	        + Coalesce('Placement Fee(%): ' + NULLIF(cast(PL.fee as varchar(max)), '') + char(10), '')
	        *//*+ Coalesce('Days Guaranteed: ' + NULLIF(cast(PL.daysGuaranteed as varchar(max)), '') + char(10), '')
	        + Coalesce('Days Pro-Rated: ' + NULLIF(cast(PL.daysProRated as varchar(max)), '') + char(10), '')
	        + Coalesce('Date Effective Date: ' + NULLIF(cast(PL.dateClientEffective as varchar(max)), '') + char(10), '')
	        + Coalesce('Bill Rate: ' + NULLIF(cast(PL.clientBillRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Pay Rate: ' + NULLIF(cast(PL.payRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Salary Unit: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
	        + Coalesce('Overtime Bill Rate: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
	        + Coalesce('Effective Date (pay rate info): ' + NULLIF(cast(PL.dateEffective as varchar(max)), '') + char(10), '')
	        + Coalesce('Overtime Pay Rate: ' + NULLIF(cast(PL.overtimeRate as varchar(max)), '') + char(10), '')
	        + Coalesce('Date Added: ' + NULLIF(cast(PL.dateAdded as varchar(max)), '') + char(10), '')
	        + Coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
	        *//*
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
*/

select --top 100 
          a.jobPostingID as 'position-externalId'
	, b.clientID as 'position-contactId'
	--, uc.firstname as '#ContactFirstName'
	--, uc.lastname as '#ContactLastName'
	--, cc.name as '#CompanyName'
	--, a.clientUserID as '#UserID'
	, case when job.title is null then 'No JobTitle' when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
	, a.numOpenings as 'position-headcount'
	--, mail5.email as 'position-owners'
        /* This field only accepts FULL_TIME, PART_TIME, CASUAL */
	--, a.type as 'position-employmentType#'
        /* This field only accepts PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT */
	--, replace(replace(replace(replace(replace(replace(a.employmentType,'Contingent','TEMPORARY'),'Permanent','PERMANENT'),'ReverseMarket','INTERIM_PROJECT_CONSULTING'),'Exclusive','PERMANENT'),'Fixed Contract','CONTRACT'),'Fixed to Perm','TEMPORARY_TO_PERMANENT') as 'position-type'
	, case when a.employmentType is null then 'PERMANENT'
	       when a.employmentType = 'Permanent' then 'PERMANENT'
	       when a.employmentType = 'Temp' then 'TEMPORARY'
	       else '' end as 'position-type'
	, a.salary as 'position-actualSalary'
	--, a.customtext1 as 'position-currency'
	, cast(a.publicDescription as varchar(max)) as 'position-publicDescription'
         , Stuff( Coalesce('Location Requirements: ' + NULLIF(cast(a.onSite as varchar(max)), '') + char(10), '')
                        + Coalesce('Internal Job Description: ' + char(10) + NULLIF(cast(a.description as varchar(max)), ''), '')
                , 1, 0, '') as 'position-internalDescription'
	--, cast(a.description as varchar(max)) as 'position-internalDescription'
	, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	, a.isOpen, convert(varchar(10),iif(a.isOpen in (null,0),getdate()-1,dateClosed),120) as 'position-endDate'
	--, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, doc.files as 'position-document'
	--, concat(note.note,placementnote.note) as 'position-note' --left(,32000)
	, note.note as 'position-note'
	, skills as 'skills'
-- select distinct employmentType -- select distinct Type -- select distinct status -- select  skills,* -- select count(*) --2574 -- select top 100 startDate  -- select onSite
from bullhorn1.BH_JobPosting a --where a.jobPostingID = 2539
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join job on a.jobPostingID = job.jobPostingID
left join mail5 ON a.userID = mail5.ID
left join note on a.jobPostingID = note.jobPostingID
--left join placementnote  on a.jobPostingID = placementnote.jobPostingID
left join doc on a.jobPostingID = doc.jobPostingID
where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--and job.title <> ''
--and a.jobPostingID in (1847,3,2,33,80,27,130)