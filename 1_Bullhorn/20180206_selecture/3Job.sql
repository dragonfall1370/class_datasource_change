
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
		, a.title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,case a.title when '' then 'No JobTile' else a.title end,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
	where b.isPrimaryOwner = 1) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select count(*) from job where title = ''
--select * from job where jobPostingID in (628,655,688,967,1375,1543,1792,1794,2154,2502,3130,3777,3829,4375,4754,5298,5483,5541,5656,5702)

-- Files
, files(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF((SELECT DISTINCT ',' + concat(candidateFileID, fileExtension) from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID) --where a.type = 'Resume') ==> get all candidates files


-- NOTE
, note as (
        select JP.jobPostingID
	, Stuff(  
                    Coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
                + Coalesce('HOT JOB?: ' + NULLIF(cast(JP.salaryUnit as varchar(max)), '') + char(10), '')
                + Coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')
                + Coalesce('Assigned to: ' + NULLIF(cast(ass.name as varchar(max)), '') + char(10), '')
                + Coalesce('Open/Closed: ' + NULLIF(cast(JP.isOpen as varchar(max)), '') + char(10), '')
                + Coalesce('Scheduled End: ' + NULLIF(cast(JP.dateEnd as varchar(max)), '') + char(10), '')
                + Coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
                + Coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
                + Coalesce('Charge Rate Lower: ' + NULLIF(cast(JP.customFloat3 as varchar(max)), '') + char(10), '')
                + Coalesce('Charge Rate Higher: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
                + Coalesce('Margin %: ' + NULLIF(cast(JP.correlatedCustomFloat2 as varchar(max)), '') + char(10), '')
                + Coalesce('# of Openings: ' + NULLIF(cast(JP.numOpenings as varchar(max)), '') + char(10), '')
                --+ Coalesce('Levels: ' + NULLIF(cast(b.desiredSpecialties as varchar(max)), '') + char(10), '')
                + Coalesce('Category: ' + NULLIF(cast(CL.Name as varchar(max)), '') + char(10), '')
                + Coalesce('Additional Keywords: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
                + Coalesce('Business Sector: ' + NULLIF(cast(CC.businessSectorList as varchar(max)), '') + char(10), '')
                + Coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
                + Coalesce('City: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
                + Coalesce('Country: ' + NULLIF(cast(tmp_country.COUNTRY as varchar(max)) + char(10), ''), '')
	        + Coalesce('Position type: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
	        /*+ Coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
	        + Coalesce('Salary: ' + NULLIF(cast(JP.salary as varchar(max)), '') + char(10), '')
	        + Coalesce('Fee arrangement: ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Publish Category: ' + NULLIF(cast(JP.publishedCategoryID as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Required skills: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
	        + Coalesce('Years required: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
	        + Coalesce('Start Date: ' + NULLIF(convert(varchar(10),JP.startdate,120), '') + char(10), '')
	        + Coalesce('Company address: ' + NULLIF(CC.address1, '') + char(10), '')
	        --+ Coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
	        + Coalesce('isOpen: ' + NULLIF(cast(JP.isOpen as varchar(max)), '') + char(10), '')
	        + Coalesce('SSOC code: ' + NULLIF(cast(JP.customInt1 as varchar(max)), '') + char(10), '')
	        --+ Coalesce('Skills / Experience: ' + NULLIF(cast(JP.skillsInfoHeader as varchar(max)), '') + char(10), '')
	        + Coalesce('Keyword: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
	        + Coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
	        + Coalesce('Certifications: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
	        + Coalesce('Benefits: ' + NULLIF(cast(JP.Benefits as varchar(max)), '') + char(10), '')
	        + Coalesce('Job Location: ' + NULLIF(cast(JP.locationInfoHeader as varchar(max)), '') + char(10), '')
	        + Coalesce('MaximumSalary: ' + NULLIF(cast(JP.customFloat1 as varchar(max)), '') + char(10), '')
	        + Coalesce('Salary Notes: ' + NULLIF(cast(JP.customTextBlock1 as varchar(max)), '') + char(10), '')
	        + Coalesce('Benefits: ' + NULLIF(cast(JP.customTextBlock3 as varchar(max)), '') + char(10), '')
	        + Coalesce('NumOpenings: ' + NULLIF(cast(JP.numOpenings as varchar(max)), '') + char(10), '')
	        + Coalesce('Kick-Off Date: ' + NULLIF(cast(JP.customDate3 as varchar(max)), '') + char(10), '')
	        + Coalesce('RC Comment: ' + NULLIF(cast(JP.customTextBlock2 as varchar(max)), '') + char(10), '')
	        + Coalesce('Social Media Snippet: ' + NULLIF(cast(JP.customTextBlock4 as varchar(max)), '') + char(10), '')
	        + Coalesce('Exclusive?: ' + NULLIF(cast(JP.customText16 as varchar(max)), '') + char(10), '') 
	        + Coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
	        + Coalesce('City: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
	        + Coalesce('Zip: ' + NULLIF(cast(JP.zip as varchar(max)), '') + char(10), '') */
	        , 1, 0, '') as note
        -- select * -- select externalCategoryID,billRateCategoryID,publishedCategoryID -- select count(*)
        from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        --left join ( select userID, desiredSpecialties from bullhorn1.BH_Client where convert(varchar(max),desiredSpecialties) is not null and convert(varchar(max),desiredSpecialties) <> '' ) b on b.userID = JP.UserID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        --left join ( select ja.jobPostingID,uc.name  from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID ) ass on ass.jobPostingID = JP.jobPostingID
        left join ( SELECT jobPostingID, STUFF((SELECT ',' + uc.name from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID where ja.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name FROM ( select ja.jobPostingID,uc.name  from bullhorn1.BH_JobAssignment ja left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID ) AS a GROUP BY a.jobPostingID ) ass on ass.jobPostingID = JP.jobPostingID
        left join tmp_country on JP.countryID = tmp_country.CODE
        --where b.desiredSpecialties is not null
        )
--select * from note
 --select count(*) from note --918 > 1103


-- Add placement information under job posting
, placementnote as (
        select jobPostingID
	, Stuff((select  /* Add placement information under job posting */
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
	        + Coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
	        */
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
                 , STUFF((SELECT DISTINCT ',' + concat(jobPostingFileID,fileExtension) from bullhorn1.View_JobPostingFile WHERE jobPostingID = a.jobPostingID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS files 
                 FROM (select jobPostingID from bullhorn1.View_JobPostingFile) AS a GROUP BY a.jobPostingID)


select --top 100 
          a.jobPostingID as 'position-externalId'
	, b.clientID as 'position-contactId'
	--, uc.firstname as '#ContactFirstName'
	--, uc.lastname as '#ContactLastName'
	--, cc.name as '#CompanyName'
	--, a.clientUserID as '#UserID'
	, case when job.title is null then 'No JobTitle' when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
	, a.numOpenings as 'position-headcount'
	, mail5.email as 'position-owners'
	, a.type as 'position-employmentType#' /* This field only accepts FULL_TIME, PART_TIME, CASUAL */
	, case when a.employmentType is null then 'PERMANENT'
	       when a.employmentType = 'Opportunity' then 'PERMANENT'
	       when a.employmentType = 'Direct Hire' then 'PERMANENT'
	       when a.employmentType = 'Permanent' then 'PERMANENT'
	       when a.employmentType = 'Contract' then 'CONTRACT'
	       when a.employmentType = 'Contract To Hire' then 'CONTRACT'
	       else '' end as 'position-type' /* This field only accepts PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT */
	--, a.salary as 'position-actualSalary'
	, a.customFloat1 as 'Salary From'
	, a.salary as 'Salary To'
	, substring(a.correlatedCustomText2,0,4) as 'position-currency'
	, cast(a.description as varchar(max)) as 'position-publicDescription'
	--, cast(a. as varchar(max)) as 'position-internalDescription'
	, Stuff(  Coalesce('Professional Qualification Requirements: ' + NULLIF(cast(a.degreeList as varchar(max)), '') + char(10), '')
	         + Coalesce('Benefits: ' + NULLIF(cast(a.benefits as varchar(max)), '') + char(10), '')
	         + Coalesce('Bonus: ' + NULLIF(cast(a.bonusPackage as varchar(max)), '') + char(10), '')
	          , 1, 0, '') as 'position-internalDescription'	
	, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	, a.isOpen, convert(varchar(10),iif(a.isOpen in (null,0),getdate()-1,dateClosed),120) as 'position-endDate', dateEnd as 'Scheduled End'
	--, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, doc.files as 'position-document'
	, note.note as 'position-note' --, concat(note.note,placementnote.note) as 'position-note' --left(,32000)
-- select distinct employmentType -- select distinct(substring(correlatedCustomText2,0,4) ) --Type -- select distinct status -- select distinct responseUserID -- select count(*) --2574 -- select top 100 startDate  -- select *
from bullhorn1.BH_JobPosting a --where a.jobPostingID  in (10582,3,2,33,80,27,10452)
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
and a.jobPostingID in (628,655,688,967,1375,1543,1792,1794,2154,2502,3130,3777,3829,4375,4754,5298,5483,5541,5656,5702)


/*
select a.jobPostingID as 'position-externalId'
	, a.customFloat1 as 'Salary From'
	, a.salary as 'Salary To'
-- select distinct employmentType -- select distinct(substring(correlatedCustomText2,0,4) ) --Type -- select distinct status -- select distinct responseUserID -- select count(*) --2574 -- select top 100 startDate  -- select *
from bullhorn1.BH_JobPosting a --where a.jobPostingID  in (10582,3,2,33,80,27,10452)
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
where b.isPrimaryOwner = 1 
and a.salary <> '' 

*/