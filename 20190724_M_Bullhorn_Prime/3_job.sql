with
--JOB DUPLICATION REGCONITION
job (jobPostingID, clientID, CompanyId, CompanyName, ContactUserID, Contact_firstname, Contact_lastname, title, starDate, rn) as (
	SELECT  a.jobPostingID as jobPostingID
		, iif(b.clientID is null, concat('999999999', iif(cc.clientcorporationid is null, '', convert(nvarchar(1000),cc.clientcorporationid)) ), convert(nvarchar(1000),b.clientID) ) as clientID
		, iif(cc.clientcorporationid is null, '999999999', convert(nvarchar(1000),cc.clientcorporationid)) as '#CompanyId' , cc.name as '#CompanyName'--, cc.status as '#Companystatus'
		, a.clientUserID as '#ContactUserID', uc.firstname as '#Contact_firstname', uc.lastname as '#Contact_lastname'--, uc.isdeleted as '#Contact_isdeleted', uc.status as '#Contact_status'
		, iif(a.title is not null and a.title <> '', ltrim(rtrim(a.title)), 'No JobTitle') as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,a.title,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join ( select userID, clientcorporationid, isdeleted, /*status,*/ max(clientID) as clientID from bullhorn1.BH_Client where (isdeleted <> 1 and status <> 'Archive') group by userID, clientcorporationid, isdeleted/*, status*/ ) b on a.clientUserID = b.userID
	left JOIN ( select clientCorporationID, name, status from bullhorn1.BH_ClientCorporation CC where CC.status <> 'Archive') cc ON cc.clientCorporationID = a.clientcorporationid
	left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
	where (a.isdeleted <> 1 and a.status <> 'Archive') --and b.clientID is null
	--and a.jobpostingid in (30, 71, 77, 104, 105, 112)
	) --where b.isPrimaryOwner = 1) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select * from job where CompanyId like '%default%' and clientID like '%default%'
--select count(*) from job where title = ''

-- >>> CREATE DEFAULT CONTACT LIST FOR JOB <<< ---
/*
	select distinct concat('PR', CompanyId) as 'contact-companyId'
	, iif(CompanyId <> 'default', concat('PR9999999',CompanyId),CompanyId ) as 'contact-externalId'
	, concat('Default Contact - ', CompanyName) as 'contact-lastname'
	from job where clientID like 'default%'
	order by 'contact-companyId' desc --companyID 83, 12
*/
/* GET MAX CONTACT ID except DELETED CONTACTS
, maxContact (select clientCorporationID, max(clientID) as maxContactID
	from bullhorn1.BH_Client
	where clientCorporationID in (12, 83)
	and (isdeleted <> 1 and status <> 'Archive')
	group by clientCorporationID)
*/

--JOB OWNERS
, mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
			concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email 
			from bullhorn1.BH_UserContact)
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
--, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
, mail5 (ID,email) as ( SELECT ID, STRING_AGG(email,',' ) WITHIN GROUP (ORDER BY email) att from mail3 GROUP BY ID )

--NOTE
, note as (
        select JP.jobPostingID
	, Stuff(  
				  coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
				+ coalesce('Assigned to: ' + NULLIF(cast(ass.name as varchar(max)), '') + char(10), '')
				+ coalesce('Benefits: ' + NULLIF(cast(JP.Benefits as varchar(max)), '') + char(10), '')
				--+ coalesce('Benefits: ' + NULLIF(cast(JP.customTextBlock3 as varchar(max)), '') + char(10), '')
				--+ coalesce('Certifications: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
				+ coalesce('Client Charge Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
				--+ coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
				--+ coalesce('Company address: ' + NULLIF(CC.address1, '') + char(10), '')
				--+ coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
				+ coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
				--+ coalesce('Exclusive?: ' + NULLIF(cast(JP.customText16 as varchar(max)), '') + char(10), '')
				--+ coalesce('Fee arrangement: ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
				--+ coalesce('Weekly Hourly Commitment: ' + NULLIF(cast(JP.hoursPerWeek as varchar(max)), '') + char(10), '')
				+ coalesce('Interview Required? ' + NULLIF(case when JP.isInterviewRequired = 1 then 'YES' else 'NO' end, '') + char(10), '')
				--+ coalesce('Job Location: ' + NULLIF(cast(JP.locationInfoHeader as varchar(max)), '') + char(10), '')
				--+ coalesce('Keyword: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
				--+ coalesce('Kick-Off Date: ' + NULLIF(cast(JP.customDate3 as varchar(max)), '') + char(10), '')
				--+ coalesce('Maximum Salary: ' + NULLIF(cast(JP.customFloat1 as varchar(max)), '') + char(10), '')
				--+ coalesce('NumOpenings: ' + NULLIF(cast(JP.numOpenings as varchar(max)), '') + char(10), '')
				--+ coalesce('Open / Closed: ' + NULLIF( cast( iif(JP.isOpen in (null,0),'Closed','Open') as varchar(max)), '') + char(10), '') 
				--+ coalesce('Pay Rate: ' + NULLIF(cast(JP.payRate as varchar(max)), '') + char(10), '')
				--+ coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
				--+ coalesce('Position type: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
				+ coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
				--+ coalesce('Publish Category: ' + NULLIF(cast(JP.publishedCategoryID as varchar(max)), '') + char(10), '')
				--+ coalesce('RC Comment: ' + NULLIF(cast(JP.customTextBlock2 as varchar(max)), '') + char(10), '')
				+ coalesce('Reason Closed: ' + NULLIF(cast(JP.reasonClosed as varchar(max)), '') + char(10), '')
				+ coalesce('Reports to: ' + NULLIF(cast(UC.name as varchar(max)), '') + char(10), '') --JP.reportToUserID
				--+ coalesce('Required skills: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
				--+ coalesce('Salary Notes: ' + NULLIF(cast(JP.customTextBlock1 as varchar(max)), '') + char(10), '')
				--+ coalesce('Salary: ' + NULLIF(cast(JP.salary as varchar(max)), '') + char(10), '')
				--+ coalesce('Skills / Experience: ' + NULLIF(cast(JP.skillsInfoHeader as varchar(max)), '') + char(10), '')
				--+ coalesce('Social Media Snippet: ' + NULLIF(cast(JP.customTextBlock4 as varchar(max)), '') + char(10), '')
				--+ coalesce('SSOC code: ' + NULLIF(cast(JP.customInt1 as varchar(max)), '') + char(10), '')
				--+ coalesce('Start Date: ' + NULLIF(convert(varchar(10),JP.startdate,120), '') + char(10), '')
				+ coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')
				--+ coalesce('Status: ' + NULLIF(JP.Status, '') + char(10), '')
				--+ coalesce('Years required: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
				+ coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
				+ coalesce('City: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
				+ coalesce('County: ' + NULLIF(cast(JP.state as varchar(max)), '') + char(10), '')
				+ coalesce('Post code: ' + NULLIF(cast(JP.zip as varchar(max)), '') + char(10), '')
				+ coalesce('Country: ' + NULLIF(cast(tmp_country.COUNTRY as varchar(max)) + char(10), ''), '')
	        , 1, 0, '') as note
        from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        left join (select userID,name from bullhorn1.BH_UserContact) UC on UC.userID = JP.reportToUserID
        left join tmp_country on JP.countryID = tmp_country.CODE
        left join ( SELECT jobPostingID, STUFF((SELECT ',' + uc.name from bullhorn1.BH_JobAssignment ja 
					left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID 
					where ja.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS name 
							from ( select ja.jobPostingID,uc.name  
										from bullhorn1.BH_JobAssignment ja 
										left join bullhorn1.BH_UserContact UC on UC.userID = ja.userID ) AS a 
										GROUP BY a.jobPostingID ) ass on ass.jobPostingID = JP.jobPostingID
        )

--INTERNAL JOB DESCRIPTION
, internaljob as (
        select JP.jobPostingID
			, Stuff(  
              + coalesce('Certifications: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
              + coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
              + coalesce('Years required: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
	        , 1, 0, '') as note
        from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
		)

/*
--PLACEMENTS INFO IN JOB
, placementnote as (
        select jobPostingID
		, Stuff((select  --Add placement information under job posting
	        --+ coalesce('Placement Status: ' + NULLIF(PL.status, '') + char(10), '')
	        + coalesce('Report to: ' + NULLIF(cast(PL.reportTo as varchar(max)), '') + char(10), '')
	        --+ coalesce('Cost Center: ' + NULLIF(cast(PL.costCenter as varchar(max)), '') + char(10), '')
	        + coalesce('Billing User: ' + NULLIF(cast(PL.billingUserID as varchar(max)), '') + char(10), '')
	        --+ coalesce('Start Date: ' + NULLIF(convert(varchar(10),PL.Datebegin,120), '') + char(10), '')
	        + coalesce('Scheduled End: ' + NULLIF(convert(varchar(10),PL.dateEnd,120), '') + char(10), '')
	        + coalesce('Employee type: ' + NULLIF(cast(PL.employeeType as varchar(max)), '') + char(10), '')
	        + coalesce('Placement Fee(%): ' + NULLIF(cast(PL.fee as varchar(max)), '') + char(10), '')
	        /*+ coalesce('Days Guaranteed: ' + NULLIF(cast(PL.daysGuaranteed as varchar(max)), '') + char(10), '')
	        + coalesce('Days Pro-Rated: ' + NULLIF(cast(PL.daysProRated as varchar(max)), '') + char(10), '')
	        + coalesce('Date Effective Date: ' + NULLIF(cast(PL.dateClientEffective as varchar(max)), '') + char(10), '')
	        + coalesce('Bill Rate: ' + NULLIF(cast(PL.clientBillRate as varchar(max)), '') + char(10), '')
	        + coalesce('Pay Rate: ' + NULLIF(cast(PL.payRate as varchar(max)), '') + char(10), '')
	        + coalesce('Salary Unit: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
	        + coalesce('Overtime Bill Rate: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
	        + coalesce('Effective Date (pay rate info): ' + NULLIF(cast(PL.dateEffective as varchar(max)), '') + char(10), '')
	        + coalesce('Overtime Pay Rate: ' + NULLIF(cast(PL.overtimeRate as varchar(max)), '') + char(10), '')
	        + coalesce('Date Added: ' + NULLIF(cast(PL.dateAdded as varchar(max)), '') + char(10), '')
	        + coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '') */
	        + coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10), '')
	        --+ coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10) + 'Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
	        --+ coalesce('BE: ' + NULLIF(cast(PL.userid as varchar(max)), '') + char(10), '')
	        -- select top 50 *
	        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
                left join bullhorn1.BH_UserContact UC on PL.billingUserID = UC.userID
                WHERE PL.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') as note
        from bullhorn1.BH_Placement a group by a.jobPostingID )
--select count(*) from placementnote
--select * from placementnote where jobPostingID in (17,28,30,50,92,115)
--select jobPostingID from placementnote group by jobPostingID having count(*) > 1
*/
--JOB OWNERS
, jobowners as (select j.jobPostingID
		, case when j.userID <> j.reportToUserID then concat_ws(',', m.email, m2.email)
		else m.email end as jobowners
		from bullhorn1.BH_JobPosting j
		left join mail5 m on m.ID = j.userID
		left join mail5 m2 on m2.ID = j.reportToUserID)

--DOCUMENT
/*, doc as (SELECT jobPostingID
                 , STUFF((SELECT DISTINCT ',' + concat(jobPostingFileID,fileExtension) from bullhorn1.View_JobPostingFile WHERE jobPostingID = a.jobPostingID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html','.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS files 
                 FROM (select jobPostingID from bullhorn1.View_JobPostingFile) AS a GROUP BY a.jobPostingID)*/
, doc as (SELECT jobPostingID
			, STRING_AGG(cast(concat(jobPostingFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY jobPostingFileID) files 
			from bullhorn1.View_JobPostingFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ 
			GROUP BY jobPostingID )

select
     concat('PR',a.jobPostingID) as 'position-externalId'
	, iif(job.clientID is null, 'PR999999999', concat('PR',job.clientID)) as 'position-contactId'
    , job.CompanyId as '#CompanyId'
	, job.CompanyName as '#CompanyName'
	, job.ContactUserID as '#ContactUserID'
	, job.Contact_firstname as '#Contact_firstname'
	, job.Contact_lastname as '#Contact_lastname'
	--, job.Contact_isdeleted as '#Contact_isdeleted', job.Contact_status as '#Contact_status'
	, case when job.rn > 1 then concat(job.title, ' - ', rn) else job.title end as 'position-title'
	, a.numOpenings as 'position-headcount'
	, jo.jobowners as 'position-owners'
	, case when a.employmentType is null then 'PERMANENT'
	       when a.employmentType = 'Contract' then 'CONTRACT'
	       when a.employmentType = 'Fixed Contract' then 'CONTRACT'
	       when a.employmentType = 'Permanent' then 'PERMANENT'
	       when a.employmentType = 'Temp to Perm' then 'CONTRACT' --all except 'PERMANENT' to 'CONTRACT'|'TEMPORARY_TO_PERMANENT'
	       when a.employmentType = 'Temporary' then 'CONTRACT'
	       else '' end as 'position-type'
	, a.salary as 'position-actualSalary'
	, 'GBP' as 'position-currency' --UK currency
	--, cast(a.publicDescription as nvarchar(max)) as 'position-publicDescription' --CUSTOM SCRIPT
	, stuff(  
              + coalesce('Certifications: ' + NULLIF(cast(a.certifications as varchar(max)), '') + char(10), '')
              + coalesce('Degree Requirements: ' + NULLIF(cast(a.degreeList as varchar(max)), '') + char(10), '')
              --+ coalesce('Minimum Experience: ' + NULLIF(cast(a.yearsRequired as varchar(max)), '') + char(10), '')
	        , 1, 0, '') as 'position-internalDescription'
	, convert(varchar(10),a.startDate,120) as 'position-startDate'
	--, convert(varchar(10),iif(a.isOpen in (null,0),getdate()-1, dateClosed),120) as 'position-endDate' 
	, a.feeArrangement as quickFee --CUSTOM SCRIPT
	, a.payRate as payRate --CUSTOM SCRIPT
	, doc.files as 'position-document'
	, note.note as 'position-note' --left(,32000) concat(note.note,placementnote.note) 
    , ltrim(stuff( 
                coalesce(' ' + NULLIF(convert(nvarchar(max),jds.skills), ''), '') 
              + coalesce(', ' + NULLIF(convert(nvarchar(max),a.skills), ''), '') 
              , 1, 1, '') ) as 'key_words' --CUSTOM SCRIPT
from bullhorn1.BH_JobPosting a --where a.jobPostingID = 2539
left join job on a.jobPostingID = job.jobPostingID
left join bullhorn1.View_JobDelimitedSkills jds on jds.jobpostingid = a.jobpostingid
left join jobowners jo ON jo.jobPostingID = a.jobPostingID
left join note on a.jobPostingID = note.jobPostingID
--left join placementnote  on a.jobPostingID = placementnote.jobPostingID
left join doc on a.jobPostingID = doc.jobPostingID
where (a.isdeleted <> 1 and a.status <> 'Archive')
order by a.jobPostingID

/*
select --top 100
         a.jobPostingID as 'position-externalId'
	, case 
	      when salaryUnit = 'Per Hour' then 1
	      when salaryUnit = 'Per Day' then 2
	      --when salaryUnit = 'Per Week' then 3
	      --when salaryUnit = 'Per Month' then 4
	      else '' end as 'contract_rate_type'
-- select distinct salaryUnit
from bullhorn1.BH_JobPosting a
where salaryUnit is not null
*/