with mail1 (ID,email) as (select userID, concat_WS(dbo.fn_RemoveNonASCIIChars(email),dbo.fn_RemoveNonASCIIChars(email2),dbo.fn_RemoveNonASCIIChars(email3)) as email 
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
--				+ coalesce('Assigned to: ' + NULLIF(cast(ass.name as varchar(max)), '') + char(10), '')
--				+ coalesce('Benefits: ' + NULLIF(cast(JP.Benefits as varchar(max)), '') + char(10), '')
--				+ coalesce('Benefits: ' + NULLIF(cast(JP.customTextBlock3 as varchar(max)), '') + char(10), '')
--				+ coalesce('Certifications: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
--				+ coalesce('Client Charge Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
--				+ coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
--				+ coalesce('Company address: ' + NULLIF(CC.address1, '') + char(10), '')
--				+ coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
--				+ coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
--				+ coalesce('Exclusive?: ' + NULLIF(cast(JP.customText16 as varchar(max)), '') + char(10), '')
--				+ coalesce('Fee arrangement: ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
--				+ coalesce('Weekly Hourly Commitment: ' + NULLIF(cast(JP.hoursPerWeek as varchar(max)), '') + char(10), '')
--				+ coalesce('Interview Required? ' + NULLIF(case when JP.isInterviewRequired = 1 then 'YES' else 'NO' end, '') + char(10), '')
--				+ coalesce('Job Location: ' + NULLIF(cast(JP.locationInfoHeader as varchar(max)), '') + char(10), '')
--				+ coalesce('Keyword: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
--				+ coalesce('Kick-Off Date: ' + NULLIF(cast(JP.customDate3 as varchar(max)), '') + char(10), '')
--				+ coalesce('Maximum Salary: ' + NULLIF(cast(JP.customFloat1 as varchar(max)), '') + char(10), '')
--				+ coalesce('NumOpenings: ' + NULLIF(cast(JP.numOpenings as varchar(max)), '') + char(10), '')
--				+ coalesce('Open / Closed: ' + NULLIF( cast( iif(JP.isOpen in (null,0),'Closed','Open') as varchar(max)), '') + char(10), '') 
--				+ coalesce('Pay Rate: ' + NULLIF(cast(JP.payRate as varchar(max)), '') + char(10), '')
--				+ coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
--				+ coalesce('Position type: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
--				+ coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
--				+ coalesce('Publish Category: ' + NULLIF(cast(JP.publishedCategoryID as varchar(max)), '') + char(10), '')
--				+ coalesce('RC Comment: ' + NULLIF(cast(JP.customTextBlock2 as varchar(max)), '') + char(10), '')
--				+ coalesce('Reason Closed: ' + NULLIF(cast(JP.reasonClosed as varchar(max)), '') + char(10), '')
--				+ coalesce('Reports to: ' + NULLIF(cast(UC.name as varchar(max)), '') + char(10), '') --JP.reportToUserID
--				+ coalesce('Required skills: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
--				+ coalesce('Salary Notes: ' + NULLIF(cast(JP.customTextBlock1 as varchar(max)), '') + char(10), '')
--				+ coalesce('Salary: ' + NULLIF(cast(JP.salary as varchar(max)), '') + char(10), '')
--				+ coalesce('Skills / Experience: ' + NULLIF(cast(JP.skillsInfoHeader as varchar(max)), '') + char(10), '')
--				+ coalesce('Social Media Snippet: ' + NULLIF(cast(JP.customTextBlock4 as varchar(max)), '') + char(10), '')
--				+ coalesce('SSOC code: ' + NULLIF(cast(JP.customInt1 as varchar(max)), '') + char(10), '')
--				+ coalesce('Start Date: ' + NULLIF(convert(varchar(10),JP.startdate,120), '') + char(10), '')
--				+ coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')
--				+ coalesce('Status: ' + NULLIF(JP.Status, '') + char(10), '')
--				+ coalesce('Years required: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
				+ coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
				+ coalesce('City: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
				+ coalesce('Bill Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
				+ coalesce('Country: ' + NULLIF(cast(tmp_country.COUNTRY as varchar(max)) + char(10), ''), '')
				+ coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
				+ coalesce('Full Address: ' +
				            trim(stuff( coalesce(' ' + nullif(JP.address, ''), '') + 
                                             + coalesce(', ' + nullif(jp.city, ''), '') + coalesce(', ' + nullif(jp.state, ''), '') 
			                     + coalesce(', ' + nullif(jp.zip, ''), '') + coalesce(', ' + nullif(tmp_country.COUNTRY, ''), '') , 1, 1, '') )+ char(10), '')
                                + coalesce('isOpen: ' + NULLIF(cast(JP.isOpen as varchar(max)), '') + char(10), '')
--				+ coalesce('County: ' + NULLIF(cast(JP.state as varchar(max)), '') + char(10), '')
                                + coalesce('Estimated Value: ' + NULLIF(cast(JP.correlatedCustomText10 as varchar(max)), '') + char(10), '')
                                + coalesce('Start Date: ' + NULLIF(cast(JP.startDate as varchar(max)), '') + char(10), '')
				+ coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
				+ coalesce('Post code: ' + NULLIF(cast(JP.zip as varchar(max)), '') + char(10), '')			
--				+ coalesce('Month: ' + NULLIF(cast(JP.customText1 as varchar(max)), '') + char(10), '')
--				+ coalesce('Status: ' + NULLIF(cast(JP.customText10 as varchar(max)), '') + char(10), '')			
				--CONCAT_WS(JP.address+jp.city+jp.countryID+jp.zip+jp.state+ char(10), '')	
--				+ coalesce('Source: ' + NULLIF(cast(JP.source as varchar(max)), '') + char(10), '')				
--				+ coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')
				
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
----PLACEMENTS INFO IN JOB
--, placementnote as (
--        select jobPostingID
--		, Stuff((select  --Add placement information under job posting
--	        --+ coalesce('Placement Status: ' + NULLIF(PL.status, '') + char(10), '')
--	        + coalesce('Report to: ' + NULLIF(cast(PL.reportTo as varchar(max)), '') + char(10), '')
--	        --+ coalesce('Cost Center: ' + NULLIF(cast(PL.costCenter as varchar(max)), '') + char(10), '')
--	        + coalesce('Billing User: ' + NULLIF(cast(PL.billingUserID as varchar(max)), '') + char(10), '')
--	        --+ coalesce('Start Date: ' + NULLIF(convert(varchar(10),PL.Datebegin,120), '') + char(10), '')
--	        + coalesce('Scheduled End: ' + NULLIF(convert(varchar(10),PL.dateEnd,120), '') + char(10), '')
--	        + coalesce('Employee type: ' + NULLIF(cast(PL.employeeType as varchar(max)), '') + char(10), '')
--	        + coalesce('Placement Fee(%): ' + NULLIF(cast(PL.fee as varchar(max)), '') + char(10), '')
--	        /*+ coalesce('Days Guaranteed: ' + NULLIF(cast(PL.daysGuaranteed as varchar(max)), '') + char(10), '')
--	        + coalesce('Days Pro-Rated: ' + NULLIF(cast(PL.daysProRated as varchar(max)), '') + char(10), '')
--	        + coalesce('Date Effective Date: ' + NULLIF(cast(PL.dateClientEffective as varchar(max)), '') + char(10), '')
--	        + coalesce('Bill Rate: ' + NULLIF(cast(PL.clientBillRate as varchar(max)), '') + char(10), '')
--	        + coalesce('Pay Rate: ' + NULLIF(cast(PL.payRate as varchar(max)), '') + char(10), '')
--	        + coalesce('Salary Unit: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
--	        + coalesce('Overtime Bill Rate: ' + NULLIF(cast(PL.salaryUnit as varchar(max)), '') + char(10), '')
--	        + coalesce('Effective Date (pay rate info): ' + NULLIF(cast(PL.dateEffective as varchar(max)), '') + char(10), '')
--	        + coalesce('Overtime Pay Rate: ' + NULLIF(cast(PL.overtimeRate as varchar(max)), '') + char(10), '')
--	        + coalesce('Date Added: ' + NULLIF(cast(PL.dateAdded as varchar(max)), '') + char(10), '')
--	        + coalesce('Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '') */
--	        + coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10), '')
--	        --+ coalesce('Date Added: ' + cast(PL.dateAdded as varchar(max)) + char(10) + 'Comments: ' + NULLIF(cast(PL.comments as varchar(max)), '') + char(10), '')
--	        --+ coalesce('BE: ' + NULLIF(cast(PL.userid as varchar(max)), '') + char(10), '')
--	        -- select top 50 *
--	        from bullhorn1.BH_Placement PL --where PL.reportTo <> ''
--                left join bullhorn1.BH_UserContact UC on PL.billingUserID = UC.userID
--                WHERE PL.jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 0, '') as note
--        from bullhorn1.BH_Placement a group by a.jobPostingID )
----select count(*) from placementnote
----select * from placementnote where jobPostingID in (17,28,30,50,92,115)
----select jobPostingID from placementnote group by jobPostingID having count(*) > 1
--JOB OWNERS
, jobowners as (select j.jobPostingID
		, case when j.userID <> j.reportToUserID then concat_ws(',', m.email, m2.email)
		else m.email end as jobowners
		from bullhorn1.BH_JobPosting j
		left join mail5 m on m.ID = j.userID
		left join mail5 m2 on m2.ID = j.reportToUserID
)

--DOCUMENT
/*, doc as (SELECT jobPostingID
                 , STUFF((SELECT DISTINCT ',' + concat(jobPostingFileID,fileExtension) from bullhorn1.View_JobPostingFile WHERE jobPostingID = a.jobPostingID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html','.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS files 
                 FROM (select jobPostingID from bullhorn1.View_JobPostingFile) AS a GROUP BY a.jobPostingID)*/
, doc as (SELECT jobPostingID
			, STRING_AGG(cast(concat(jobPostingFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY jobPostingFileID) files 
			from bullhorn1.View_JobPostingFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ 
			GROUP BY jobPostingID 
), company as (
        select clientCorporationID, name 
        from bullhorn1.BH_ClientCorporation CC 
        where CC.status <> 'Archive'
), contact as (
        select clientID,userID,clientCorporationID
        from bullhorn1.BH_Client 
        where (isdeleted <> 1 and status <> 'Archive')
        and isPrimaryOwner=1
), companywithoutcontact as (
        select clientCorporationID, name 
        from company
        where clientCorporationID not in (select DISTINCT clientCorporationID from contact)
), titledup as (
        select j.jobPostingID,j.clientUserID,j.title,j.startDate,ROW_NUMBER() OVER(PARTITION BY j.clientUserID,j.title,CONVERT(VARCHAR(10),j.startDate,120) ORDER BY j.jobPostingID) AS rn
        from bullhorn1.BH_JobPosting j
)
select  --count(*)/*
        concat('TRJ',a.jobPostingID) as 'position-externalId',
        case when rn > 1 then concat(a.title, ' - ', rn) else a.title end as 'position-title',
        case 
                when a.userID <> a.reportToUserID then concat_ws(',', m.email, m2.email)
		else m.email 
        end as 'position-owners',
        case when a.employmentType is null then 'PERMANENT'
	       when a.employmentType in ('Temporary','Fixed Term','Temp','Contract') then 'CONTRACT'
	       when a.employmentType in ('Opportunity','Permanent','FT,Perm') then 'PERMANENT'
	       else '' 
        end as 'position-type',
	a.salary as 'position-actualSalary',
	a.payRate as 'position-contractPayrate',
	'GBP' as 'position-currency',
	TRIM('.?/-;' from [bullhorn1].[fn_ConvertHTMLToText](a.description)) as 'position-internalDescription',
        TRIM('.?/-;' from [bullhorn1].[fn_ConvertHTMLToText](a.publicDescription)) as 'position-publicDescription',
	convert(varchar(10),a.startDate,120) as 'position-startDate',
        convert(varchar(10),a.dateEnd,120) as 'position-endDate',
        a.numOpenings as 'position-Headcount',
        a.salaryUnit,
        a.markupPercentage,
	a.payRate as payRate, --CUSTOM SCRIPT
	doc.files as 'position-document',
	note.note as 'position-note', --left(,32000) concat(note.note,placementnote.note) 
        ltrim(stuff( 
                coalesce(' ' + NULLIF(convert(nvarchar(max),jds.skills), ''), '') 
              + coalesce(', ' + NULLIF(convert(nvarchar(max),a.skills), ''), '') 
              , 1, 1, '') ) as 'key_words',
        case 
                when a.clientCorporationID in (select DISTINCT clientCorporationID from company) then CONCAT('TRCP',a.clientCorporationID)
                else CONCAT('DEFAULT_TRCP',a.clientCorporationID)
        end ProjectCompany,
        CC.Name,   
        case 
                when a.clientCorporationID in (select DISTINCT clientCorporationID from company) 
                        then
                        (
                                case 
                                        when a.clientCorporationID = c.clientCorporationID then CONCAT('TRCT',c.clientID)
                                        else CONCAT('DEFAULT_TRCT',a.clientCorporationID)
                                end
                        )
                else 
                        (
                                case 
                                        when a.clientCorporationID = c.clientCorporationID then CONCAT('TRCT',c.clientID)
                                        else CONCAT('DEFAULT_TRCT',a.clientCorporationID)
                                end
                        )
        end 'position-contactId',
        a.clientUserID,c.clientID,
        uc2.firstName,uc2.lastName,uc2.Name,
        a.numOpenings  as 'position-headcount'
from bullhorn1.BH_JobPosting a
left join bullhorn1.BH_ClientCorporation CC on a.clientCorporationID=CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.userID = UC.userID
left join mail5 m on m.ID = a.userID
left join mail5 m2 on m2.ID = a.reportToUserID
--left join bullhorn1.BH_Client cl on cl.userID=a.clientUserID
left join contact c on c.userID=a.clientUserID
left join bullhorn1.BH_UserContact UC2 on a.clientUserID = UC2.userID
left join bullhorn1.View_JobDelimitedSkills jds on jds.jobpostingid = a.jobpostingid
left join note on a.jobPostingID = note.jobPostingID
left join doc on a.jobPostingID = doc.jobPostingID
left join titledup on titledup.jobPostingID=a.jobPostingID
where (a.isdeleted <> 1 and a.status <> 'Archive')
;


select * from bullhorn1.BH_JobPosting where jobPostingID=1253 and (isdeleted <> 1 and status <> 'Archive');
select * from bullhorn1.BH_UserContact where BH_UserContact.userID=1708;