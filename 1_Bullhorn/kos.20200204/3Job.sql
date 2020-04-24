
with
--JOB DUPLICATION REGCONITION
job0 as (
       select
                a.jobPostingID
              , a.dateadded
              , iif(a.title is not null and a.title <> '', trim(a.title), 'No JobTitle') as title
              , convert(varchar(10),a.startDate,120) as startDate
              , iif(a.clientcorporationid is null or a.clientcorporationid = '', 'default', a.clientcorporationid) as 'Company_externalID', cc.name as 'CompanyName' --COMPANY
              , iif(company.contact_externalId is null or convert(varchar(500),company.contact_externalId) = '', 'default', convert(varchar(500),company.contact_externalId)) as 'contact_externalId', a.clientUserID as 'ContactUserID', uc.firstname as 'Contact_firstname', uc.lastname as 'Contact_lastname'--, uc.isdeleted as '#Contact_isdeleted', uc.status as '#Contact_status'              

              , company.[contact_companyId] as 'Company_externalID_of_ContactUserID'
              , company.company_name as 'CompanyName_of_ContactUserID'
       -- select count(*)                            
       from bullhorn1.BH_JobPosting a
       left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
       left JOIN ( select clientCorporationID, name, status from bullhorn1.BH_ClientCorporation CC /*where CC.status <> 'Archive'*/) cc ON cc.clientCorporationID = a.clientcorporationid
       left join (
              select * 
              from (
                     select
                              Cl.clientID as 'contact_externalId', Cl.userID as 'UserID'
                            , UC.clientCorporationID as 'contact_companyId'
                            , com.name as 'company_name'
                            , ROW_NUMBER() OVER(PARTITION BY Cl.UserID, UC.clientCorporationID, com.name ORDER BY Cl.clientID desc) AS rn 
                     -- select *
                     from bullhorn1.BH_Client Cl --where Cl.userID = 41471
                     left join (select distinct userID,clientCorporationID from bullhorn1.BH_UserContact) UC ON Cl.userID = UC.userID
                     left join (select clientCorporationID, name from bullhorn1.BH_ClientCorporation /*where status <> 'Archive'*/) com on com.clientCorporationID = UC.clientCorporationID
                     where (Cl.isdeleted <> 1 /*and Cl.status <> 'Archive'*/) --and Cl.userID in (579,605)
              ) n where rn = 1
              ) company on company.userID = a.clientUserID
       where (a.isdeleted <> 1 /*and a.status <> 'Archive'*/) --and company.rn = 1
       --and a.clientcorporationid in (51)       
       --and a.jobPostingID in (18,19,20)
)
, job (jobPostingID, dateadded, title, starDate, Company_externalID, CompanyName, contact_externalId, final_contact_externalId, ContactUserID, Contact_firstname, Contact_lastname, Company_externalID_of_ContactUserID, CompanyName_of_ContactUserID, rn) as (
	SELECT  jobPostingID
	       , dateadded
		, title
		, startDate
       --COMPANY
		, Company_externalID, CompanyName
       --CONTACT
		, contact_externalId
		, case
		     when Company_externalID = Company_externalID_of_ContactUserID  then iif(contact_externalId is null or contact_externalId = '', concat('default',convert(varchar(5000),Company_externalID)), convert(varchar(5000),contact_externalId) )
		     --when Company_externalID <> Company_externalID_of_ContactUserID then iif(contact_externalId is null or contact_externalId = '', concat('default',convert(varchar(5000),Company_externalID)), concat('default',convert(varchar(5000),contact_externalId)) )
		     when Company_externalID <> Company_externalID_of_ContactUserID then iif(contact_externalId is null or contact_externalId = '', concat('default',convert(varchar(5000),Company_externalID)), concat('default',convert(varchar(5000),Company_externalID),'.',convert(varchar(5000),contact_externalId)) )
		     end as 'final_contact'
		, ContactUserID, Contact_firstname, Contact_lastname
              , Company_externalID_of_ContactUserID
              , CompanyName_of_ContactUserID
		, ROW_NUMBER() OVER(PARTITION BY contact_externalId,title,startDate ORDER BY jobPostingID) AS rn 
	from job0 
	)
--select * from job where jobPostingID in (952)


-- >>> CREATE DEFAULT CONTACT LIST FOR JOB <<< ---
--select distinct Company_externalID as 'contact-companyId', final_contact_externalId as 'contact-externalId', Contact_firstname as 'contact-firstname', Contact_lastname as 'contact-lastname' from job where final_contact_externalId like 'default%' order by Company_externalID desc


-- MAIL
, mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
--, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
, mail5 (ID,email) as ( SELECT ID, STRING_AGG(email,',' ) WITHIN GROUP (ORDER BY email) att from mail3 GROUP BY ID )
--select * from mail5 where id in (39188,14248,30223)



-- NOTE
, note as (
        select JP.jobPostingID
	, Stuff(  
                Coalesce('BH Job ID: ' + NULLIF(cast(JP.jobPostingID as varchar(max)), '') + char(10), '')

+ Coalesce('Benefits: ' + NULLIF(convert(nvarchar(max),JP.benefits), '') + char(10), '')
+ Coalesce('Certification Requirements: ' + NULLIF(convert(nvarchar(max),JP.certifications), '') + char(10), '')
+ Coalesce('Retainer Fee Date: ' + NULLIF(convert(nvarchar(max),JP.customDate1), '') + char(10), '')
+ Coalesce('Shortlist Fee Date: ' + NULLIF(convert(nvarchar(max),JP.customDate2), '') + char(10), '')
+ Coalesce('Completion Fee Date: ' + NULLIF(convert(nvarchar(max),JP.customDate3), '') + char(10), '')
+ Coalesce('Projected Fee: ' + NULLIF(convert(nvarchar(max),JP.customFloat2), '') + char(10), '')
+ Coalesce('Completion Fee: ' + NULLIF(convert(nvarchar(max),JP.customFloat3), '') + char(10), '')
+ Coalesce('Confidence Level (%): ' + NULLIF(convert(nvarchar(max),JP.customText2), '') + char(10), '')
+ Coalesce('Discipline: ' + NULLIF(convert(nvarchar(max),JP.customText3), '') + char(10), '')
+ Coalesce('Job Location: ' + NULLIF(convert(nvarchar(max),JP.customText4), '') + char(10), '')
+ Coalesce('Interview Required?: ' + NULLIF(convert(nvarchar(max),case JP.isInterviewRequired when 1 then 'Yes' else 'No' end), '') + char(10), '')
+ Coalesce('Reason Closed: ' + NULLIF(convert(nvarchar(max),JP.reasonClosed), '') + char(10), '')
+ Coalesce('Reports to: ' + NULLIF(cast(UC.name as varchar(max)), '') + char(10), '') --JP.reportToUserID
+ Coalesce('Source: ' + NULLIF(convert(nvarchar(max),JP.source), '') + char(10), '')
+ Coalesce('Status: ' + NULLIF(convert(nvarchar(max),JP.status), '') + char(10), '')
+ Coalesce('Priority: ' + NULLIF(convert(nvarchar(max),JP.type), '') + char(10), '')
+ Coalesce('Minimum Experience: ' + NULLIF(convert(nvarchar(max),JP.yearsRequired), '') + char(10), '')

--              + Coalesce('Assigned to: ' + NULLIF(cast(ass.name as varchar(max)), '') + char(10), '')
--              + Coalesce('Benefits: ' + NULLIF(cast(JP.Benefits as varchar(max)), '') + char(10), '')
--              + Coalesce('Certifications: ' + NULLIF(cast(JP.certifications as varchar(max)), '') + char(10), '')
--              + Coalesce('Client Charge Rate: ' + NULLIF(cast(JP.clientBillRate as varchar(max)), '') + char(10), '')
--              + Coalesce('Client Corporation ID: ' + NULLIF(cast(CC.clientCorporationID as varchar(max)), '') + char(10), '')
--              + Coalesce('Company address: ' + NULLIF(CC.address1, '') + char(10), '')
--              + Coalesce('Degree Requirements: ' + NULLIF(cast(JP.degreeList as varchar(max)), '') + char(10), '')
--              + Coalesce('Employment Type: ' + NULLIF(cast(JP.employmentType as varchar(max)), '') + char(10), '')
--              + Coalesce('Fee arrangement: ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
--              + Coalesce('Job Location: ' + NULLIF(cast(JP.locationInfoHeader as varchar(max)), '') + char(10), '')
--              + Coalesce('Keyword: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
--              + Coalesce('Linked Job Posting ID: ' + NULLIF(cast(JP.linkedJobPostingID as varchar(max)), '') + char(10), '')
--              + Coalesce('NumOpenings: ' + NULLIF(cast(JP.numOpenings as varchar(max)), '') + char(10), '')
--              + Coalesce('Open / Closed: ' + NULLIF( cast( iif(JP.isOpen in (null,0),'Closed','Open') as varchar(max)), '') + char(10), '') 
--              + Coalesce('Pay Rate: ' + NULLIF(cast(JP.payRate as varchar(max)), '') + char(10), '')
--              + Coalesce('Perm Fee (%): ' + NULLIF(cast(JP.feeArrangement as varchar(max)), '') + char(10), '')
--              + Coalesce('Position type: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
--              + Coalesce('Priority: ' + NULLIF(cast(JP.type as varchar(max)), '') + char(10), '')
--              + Coalesce('Publish Category: ' + NULLIF(cast(JP.publishedCategoryID as varchar(max)), '') + char(10), '')
--              + Coalesce('Reason Closed: ' + NULLIF(cast(JP.reasonClosed as varchar(max)), '') + char(10), '')
--              
--              + Coalesce('Required skills: ' + NULLIF(cast(JP.skills as varchar(max)), '') + char(10), '')
--              + Coalesce('Salary: ' + NULLIF(cast(JP.salary as varchar(max)), '') + char(10), '')
--              + Coalesce('Skills / Experience: ' + NULLIF(cast(JP.skillsInfoHeader as varchar(max)), '') + char(10), '')
--              + Coalesce('Start Date: ' + NULLIF(convert(varchar(10),JP.startdate,120), '') + char(10), '')
--              + Coalesce('Status: ' + NULLIF(cast(JP.status as varchar(max)), '') + char(10), '')
--              + Coalesce('Years required: ' + NULLIF(cast(JP.yearsRequired as varchar(max)), '') + char(10), '')
--
--              + Coalesce('Address: ' + NULLIF(cast(JP.address as varchar(max)), '') + char(10), '')
--              + Coalesce('Suburb: ' + NULLIF(cast(JP.city as varchar(max)), '') + char(10), '')
--              + Coalesce('State: ' + NULLIF(cast(JP.state as varchar(max)), '') + char(10), '')
--              + Coalesce('Zip: ' + NULLIF(cast(JP.zip as varchar(max)), '') + char(10), '')
--              + Coalesce('Country: ' + NULLIF(cast(tmp_country.COUNTRY as varchar(max)) + char(10), ''), '')
	        , 1, 0, '') as note
        -- select count(*) -- select top 50 * -- select distinct isInterviewRequired, count(*)
        from bullhorn1.BH_JobPosting JP --group by isInterviewRequired --where cast(skills as varchar(max)) <> ''
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID
        left join (select userID,name from bullhorn1.BH_UserContact) UC on UC.userID = JP.reportToUserID
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
/*, doc as (SELECT jobPostingID
                 , STUFF((SELECT DISTINCT ',' + concat(jobPostingFileID,fileExtension) from bullhorn1.View_JobPostingFile WHERE jobPostingID = a.jobPostingID and fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html','.txt') FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS files 
                 FROM (select jobPostingID from bullhorn1.View_JobPostingFile) AS a GROUP BY a.jobPostingID)*/
, doc (jobPostingID,files) as ( SELECT jobPostingID, STRING_AGG(cast(concat(jobPostingFileID,fileExtension) as nvarchar(max)),',' ) WITHIN GROUP (ORDER BY jobPostingFileID) files from bullhorn1.View_JobPostingFile where isdeleted <> 1 /*and fileExtension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')*/ GROUP BY jobPostingID )


select --top 3
         a.jobPostingID as 'position-externalId'
	, iif(job.final_contact_externalId is null, 'default',job.final_contact_externalId) as 'position-contactId'
       , job.Company_externalID as '#CompanyId', job.CompanyName as '#CompanyName', job.ContactUserID as '#ContactUserID', job.Contact_firstname as '#Contact_firstname', job.Contact_lastname as '#Contact_lastname'
       , a.dateadded
	, case 
	      when a.status = 'Archive' then concat( (case when job.rn > 1 then concat(job.title,' ',rn) else job.title end), ' (Archive)')
	      else (case when job.rn > 1 then concat(job.title,' ',rn) else job.title end)
	      end as 'position-title'
	, a.numOpenings as 'position-headcount'
	, mail5.email as 'position-owners'

	--, a.type as 'position-employmentType#' --[FULL_TIME, PART_TIME, CASUAL]
	, case when a.employmentType is null or a.employmentType in ('Permanent','Perm','Retained','Opportunity','Full-time','General Posting','INTERN FEE','Internal Recruitment','Strategic Opportunity') then 'PERMANENT'
	       when a.employmentType in ('Contract','Contract Flat Fee', 'Fixed Term', 'Fixed Contract','Temp','Temporary','Temp to Perm','Temporary - Contractor','Temporary - Employee','Transactional Opportunity') then 'CONTRACT'
	       else 'CONTRACT' end as 'position-type' --[PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT]
--	, a.salary as 'position-actualSalary'
--	, a.customtext1 as 'position-currency'

	, concat_ws (''
--	      , coalesce('Certification Requirements: ' + nullif(convert(nvarchar(max),a.certifications), '') + '<br/>', '')
	      , coalesce('Degree Requirements: ' + nullif(convert(nvarchar(max),a.degreeList), '') + '<br/>', '')
--	      , coalesce('Minimum Experience: ' + nullif(convert(nvarchar(max),a.yearsRequired), '') + '<br/>', '')
             , coalesce('Job Description: ' + nullif(convert(nvarchar(max),a.description), '') + '<br/>', '')
	      ) as 'position-internalDescription'

       , trim(convert(nvarchar(max),a.publicDescription)) as 'position-publicDescription'
--	, concat_ws (''
	      --, coalesce('General Job Description: ' + nullif(convert(nvarchar(max),a.correlatedCustomTextBlock2), '') + '<br/>', '')
--             , coalesce('Published Description: ' + nullif(convert(nvarchar(max),a.publicDescription), '') + '<br/>', '') as 'position-publicDescription'
--	      ) as 'position-publicDescription'
	      
	, convert(varchar(10),a.startDate,120) as 'position-startDate'
	, convert(varchar(10),iif(a.isOpen in (null,0),getdate()-1,dateend),120) as 'position-endDate' --, convert(varchar(10),iif(a.status in ('Archive','Cancelled','Filled by Client','Lost to Competitor'),getdate()-2,dateClosed),120) as 'position-endDate'
	, doc.files as 'position-document'
	, note.note as 'position-note' --left(,32000) concat(note.note,placementnote.note) 

       , trim(Stuff(
                Coalesce(' ' + NULLIF(convert(nvarchar(max),jds.skills), ''), '') 
              + Coalesce(', ' + NULLIF(convert(nvarchar(max),a.skills), ''), '') 
              , 1, 1, '') ) as 'key_words'
       , a.clientBillRate as 'Charge Rate' --**
       , a.feeArrangement as 'Quick Fee %' --**
       , a.payRate as 'Pay Rate' --**
       , a.salaryUnit as 'Payment Type' --**
-- select distinct employmentType -- select distinct Type -- select distinct status -- select distinct customtext1 -- select count(*) --2574 -- select top 100 startDate, dateend, dateClosed -- select top *
-- select clientBillRate, feeArrangement, payRate,salaryUnit
from bullhorn1.BH_JobPosting a --where a.jobPostingID = 2539
/*left join ( select userID, clientcorporationid, isdeleted, status, max(clientID) as clientID from bullhorn1.BH_Client where (isdeleted <> 1 and status <> 'Archive') group by userID, clientcorporationid, isdeleted, status ) b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID*/
--left join (select * from job where rn = 1) job on a.jobPostingID = job.jobPostingID
left join job on a.jobPostingID = job.jobPostingID
left join bullhorn1.View_JobDelimitedSkills jds on jds.jobpostingid = a.jobpostingid
left join mail5 ON a.userID = mail5.ID
left join note on a.jobPostingID = note.jobPostingID
--left join placementnote  on a.jobPostingID = placementnote.jobPostingID
left join doc on a.jobPostingID = doc.jobPostingID
where (a.isdeleted <> 1 /*and a.status <> 'Archive'*/)
--and a.employmentType in ('Contract','Fixed Contract','Temp','Temporary','Temp to Perm')
--and job.Company_externalID in (939)
--and a.jobpostingid in (939)
--where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--and job.clientID like '%default%'
--and a.jobPostingID in (1847,3,2,33,80,27,130)


select distinct reasonClosed from bullhorn1.BH_JobPosting a where reasonClosed is not null and reasonClosed <> ''  order by reasonClosed asc
select jobPostingID as 'additional_id'
       , 'add_job_info' as 'additional_type'
       , 1006 as 'form_id'
       , 11267 as 'field_id'
       , convert(nvarchar(max),reasonClosed) as 'field_value'
       , 11267 as 'constraint_id'
from bullhorn1.BH_JobPosting a
where reasonClosed is not null and reasonClosed <> ''


select distinct source from bullhorn1.BH_JobPosting a where source is not null and source <> '' order by source asc
select jobPostingID as 'additional_id'
       , 'add_job_info' as 'additional_type'
       , 1006 as 'form_id'
       , 11268 as 'field_id'
       , case 
when source = 'Business Development' then '1'
when source = 'CFA91' then '2'
when source = 'CPA 1.0' then '3'
when source = 'CPA 2.0' then '4'
when source = 'CPA 3.0' then '5'
when source = 'Dummy' then '6'
when source = 'General Enquiry' then '7'
when source = 'General Management' then '8'
when source = 'Referral' then '9'
when source = 'Repeat Business' then '10'
end       as 'field_date_value'
       , 11268 as 'constraint_id'
from bullhorn1.BH_JobPosting a
where source is not null and source <> ''

