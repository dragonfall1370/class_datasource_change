drop table if exists VCJobs;

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
	, Stuff(
		Coalesce('ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')
		
		--+ Coalesce('City: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
		--+ Coalesce('County: ' + NULLIF(cast(JP.state as varchar(max)), '') + char(10), '')
		--+ Coalesce('Post Code: ' + NULLIF(cast(JP.zip as varchar(max)), '') + char(10), '')
		--+ Coalesce('Country: ' + NULLIF(cast(tmp_country.COUNTRY as varchar(max)), '') + char(10), '') --JP.countryID 
		+ Coalesce('Client Charge Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
		+ Coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
		+ Coalesce('Value: ' + NULLIF(cast(JP.CustomText2 as varchar(max)), '') + char(10), '')
		+ Coalesce('Brand: ' + NULLIF(cast(JP.CustomText20 as varchar(max)), '') + char(10), '')
		+ Coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
		+ Coalesce('Reason Closed: ' + NULLIF(cast(JP.reasonClosed as varchar(max)), '') + char(10), '')
		+ Coalesce('Pay Unit: ' + NULLIF(cast(JP.salaryUnit as varchar(max)), '') + char(10), '')
		+ Coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')

		--+ Coalesce('Probability Rating: ' + NULLIF(cast(JP.CustomText1 as varchar(max)), '') + char(10), '')
		--+ concat('Open/Closed: ', iif(JP.isOpen in (null,0), 'Closed', 'Open'), char(10))
		--+ Coalesce('Mark-up %: ' + NULLIF(cast(JP.markUpPercentage as varchar(max)), '') + char(10), '')
		--+ Coalesce('Pay Rate: ' + NULLIF(cast(JP.payRate as varchar(max)), '') + char(10), '') -->>
		--+ concat('Published Contact Info: ', isnull(JP.responseUserID, ''), char(10))
		--+ Coalesce('Amount of Recruitment Agencies: ' + NULLIF(cast(JP.source as varchar(max)), '') + char(10), '')              
		
    --          + Coalesce('Benefits: ' + NULLIF(cast(JP.benefits as varchar(max)), '') + char(10), '')
    --          + Coalesce('Certification Requirements: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
              
              
    --          + Coalesce('Purchase Order Number: ' + NULLIF(cast(JP.correlatedCustomText3 as varchar(max)), '') + char(10), '')
    --          + Coalesce('Scheduled End: ' + NULLIF(cast(JP.dateEnd as varchar(max)), '') + char(10), '')
    --          + Coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
    --          + Coalesce('Job Description: ' + NULLIF(cast([dbo].[fn_ConvertHTMLToText](JP.description) as varchar(max)), '') + char(10), '')
    --          + Coalesce('Education Requirements: ' + NULLIF(cast(JP.educationDegree as varchar(max)), '') + char(10), '')
    --          + Coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
    --          --+ Coalesce('Address: ' + NULLIF(cast(JP.fullAddress as varchar(max)), '') + char(10), '')
    --          + Coalesce('Hourly Commitment: ' + NULLIF(cast(JP.hoursPerWeek as varchar(max)), '') + char(10), '')
    --          + Coalesce('Interview Required?: ' + NULLIF(cast(JP.isInterviewRequired as varchar(max)), '') + char(10), '')
    
    --          + Coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
    --          + Coalesce('Visa Sponsorship Provided: ' + NULLIF(cast(JP.willSponsor as varchar(max)), '') + char(10), '')
    --          + Coalesce('Minimum Experience: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
		, 1, 0, ''
	) as note
    -- select count(*) -- select top 50 * -- select payRate
    from bullhorn1.BH_JobPosting JP --where cast(skills as varchar(max)) <> ''
    --left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
    --left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
    left join VC_Countries vcc on JP.countryID = vcc.CODE
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
	, lower(dbo.ufn_TrimSpecialCharacters_V2(mail5.email, '')) as 'position-owners'
	--, a.type as 'position-employmentType#' /* This field only accepts FULL_TIME, PART_TIME, CASUAL */
	, case lower(trim(isnull(a.employmentType, '')))
		when lower('Contract') then 'CONTRACT'
		when lower('Fixed Contract') then 'CONTRACT'
		when lower('Temporary') then 'CONTRACT'
		when lower('Permanent') then 'PERMANENT'
		when lower('Project') then 'INTERIM_PROJECT_CONSULTING'
		when lower('Temp to Perm') then 'TEMPORARY_TO_PERMANENT'
		when lower('Perm & Contract') then 'PERMANENT'

		else 'PERMANENT'
	
	end as [position-type]
	--, case when a.employmentType is null then 'PERMANENT'
	--       when lower(trim(isnull(a.employmentType, ''))) = lower('Permanent') then 'PERMANENT'
	--       when lower(trim(isnull(a.employmentType, ''))) = lower('Opportunity') then 'PERMANENT'
	--	   when lower(trim(isnull(a.employmentType, ''))) = lower('Contract') then 'CONTRACT'
	--	   when lower(trim(isnull(a.employmentType, ''))) = lower('Fixed Term') then 'CONTRACT'
	--	   when lower(trim(isnull(a.employmentType, ''))) = lower('Temporary') then 'TEMPORARY'
	--       else '' end as 'position-type' /* This field only accepts PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT */
	, a.salary as 'position-actualSalary'
	--, a.customtext1 as 'position-currency'
	, cast(a.publicDescription as varchar(max)) as 'position-publicDescription'
/*       , Stuff( Coalesce('Location Requirements: ' + NULLIF(cast(a.onSite as varchar(max)), '') + char(10), '')
                        + Coalesce('Internal Job Description: ' + char(10) + NULLIF(cast(a.description as varchar(max)), ''), '')
                , 1, 0, '') as 'position-internalDescription'*/
	, cast(a.description as varchar(max)) as 'position-internalDescription'
	, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	, a.isOpen
	, convert(
		varchar(10)
		, iif(a.isOpen in (null, 0), iif(dateClosed is null, dateadd(day, -7 , getdate()), dateClosed), dateadd(year, 1 , getdate()))
		, 120
	) as 'position-endDate'
	--, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, doc.files as 'position-document'
	--, concat(note.note,placementnote.note) as 'position-note' --left(,32000)
	, note.note as 'position-note'
	--, skills as 'skills'
-- select distinct employmentType -- select distinct Type -- select distinct status -- select  skills,* -- select count(*) --2574 -- select top 100 startDate  -- select onSite

into VCJobs

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



select * from VCJobs
--where [position-contactId] not in (
--	select [contact-externalId] from VCCons
--)
--where [position-externalId] is null
--where len(trim(isnull([position-title], ''))) = 0
--where [position-endDate] is null
--where [position-startDate] is null
--where [position-type] like '%TEMP_TO_PERMANENT%' or [position-type] like '%PROJECT_CONSULTING%'
--where [position-externalId] in (
--166,194,196,197,216,259,300,315,317,258,195,251,331,373,322,325,389,370,385,453,559,585,692,666,668,594,618,675,708,679,704,747,677,770,752,1010,758,885,853,850,884,893,1100,1029,1328,1459,1460,1336,1525,1530,1463,1527,1545,1559,1577,1596,1451,1517,1519,1425,1461,1462,1591,1657,1664,1666,1689,1691,1541,1571,1582,1513,1563,1574,1599,1606,1624,1458,1497,1610,1627,1628,1642,1645,1646,1659,1660,1677,1678,1695,1551,1568,1570,1604,1621,1634,1651,1653,1668,1670,1685,1687,1702,1704,1719,1721,1520,1535,1618,1652,1654,1669,1671,1684,1686,1701,1703,1718,1698,1723,1725,1476,1511,1526,1579,1605,1607,1614,1641,1648,1650,1675,1682,1649,1656,1663,1667,1681,1688,1699,1706,1717,1724,1583,1608,1640,1647,1658,1665,1672,1683,1690,1697,1708,1709,1710,1713,1728,1720,1612,1626,1644,1661,1676,1679,1693,1694,1711,1712,1726,1705,1707,1714,1716,1715,1722,1729
--)