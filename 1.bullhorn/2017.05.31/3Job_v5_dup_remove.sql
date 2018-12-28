with 
-- MAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail5

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

, Note as (
        select jobPostingID
        , concat('BH Job ID:',JP.jobPostingID,char(10)
        , 'Position type: ',JP.type,char(10)
        , 'Employment Type: ',JP.employmentType,char(10)
        , 'Priority: ',JP.type,char(10)
        , iif(JP.salary = '' or JP.salary is NULL,'',concat('Salary: ',JP.salary,char(10)))
        , iif(JP.feeArrangement = '' or JP.feeArrangement is NULL,'',concat('Fee arrangement: ',JP.feeArrangement,char(10)))
        , iif(JP.publishedCategoryID = '' or JP.publishedCategoryID is NULL,'',concat('Publish Category: ',JP.publishedCategoryID,' - ',CL.occupation,char(10)))
        , iif(cast(JP.skills as varchar(max))= '' or JP.skills is NULL,'',concat('Required skills: ',JP.skills,char(10)))
        , iif(JP.yearsRequired = '' or JP.yearsRequired is NULL,'',concat('Years required: ',JP.yearsRequired,char(10)))
        , iif(CC.address1 = '' or CC.address1 is NULL,'',concat('Company address: ',CC.address1)) 
        ) as AdditionalNote
        from bullhorn1.BH_JobPosting JP
        left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
        left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID )
--DOCUMENT
, doc as (select a.jobPostingID, concat(a.jobPostingFileID,a.fileExtension) as jobFile
	from bullhorn1.View_JobPostingFile a
	where fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html'))

, jobdoc as (SELECT jobPostingID, STUFF((SELECT DISTINCT ',' + jobFile 
from doc 
WHERE jobPostingID = a.jobPostingID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS jobFile 
FROM doc AS a GROUP BY a.jobPostingID)


/* Add placement information under job posting */
, tmp_4 as (select jobPostingID
        , concat('Status: ',PL.status,char(10)
        , iif(PL.reportTo = '' or PL.reportTo is NULL,'',concat('Report to: ',PL.reportTo,char(10)))
        , iif(PL.costCenter = '' or PL.costCenter is NULL,'',concat('Cost Center: ',PL.costCenter,char(10)))
        , iif(PL.billingUserID = '' or PL.billingUserID is NULL,'',concat('Billing User: ',PL.billingUserID,' - ',UC.firstName,' ',UC.lastName,char(10)))
        , iif(cast(PL.dateBegin as varchar(10)) = '' or PL.dateBegin is NULL,'',concat('Start Date: ',convert(varchar(10),PL.dateBegin,120),char(10)))
        , iif(cast(PL.dateEnd as varchar(10)) = '' or PL.dateEnd is NULL,'',concat('Scheduled End: ',convert(varchar(10),PL.dateBegin,120),char(10)))
        , iif(PL.employeeType = '' or PL.employeeType is NULL,'',concat('Employee type: ',PL.employeeType,char(10)))
        , iif(PL.fee = '' or PL.fee is NULL,'',concat('Placement Fee(%): ',PL.fee,char(10)))
        , iif(PL.daysGuaranteed = '' or PL.daysGuaranteed is NULL,'',concat('Days Guaranteed: ',PL.daysGuaranteed,char(10)))
        , iif(PL.daysProRated = '' or PL.daysProRated is NULL,'',concat('Days Pro-Rated: ',PL.daysProRated,char(10)))
        , iif(cast(PL.dateClientEffective as varchar(10)) = '' or PL.dateClientEffective is NULL,'',concat('Effective Date: ',convert(varchar(10),PL.dateClientEffective,120),char(10)))
        , iif(PL.clientBillRate = '' or PL.clientBillRate is NULL,'',concat('Bill Rate: ',PL.clientBillRate,char(10)))
        , iif(PL.payRate = '' or PL.payRate is NULL,'',concat('Pay Rate: ',PL.payRate,char(10)))
        , iif(PL.salaryUnit = '' or PL.salaryUnit is NULL,'',concat('Pay Unit: ',PL.salaryUnit,char(10)))
        , iif(PL.clientOvertimeRate = '' or PL.clientOvertimeRate is NULL,'',concat('Overtime Bill Rate: ',PL.clientOvertimeRate,char(10)))
        , iif(cast(PL.dateEffective as varchar(10)) = '' or PL.dateEffective is NULL,'',concat('Effective Date (pay rate info): ',convert(varchar(10),PL.dateEffective,120),char(10)))
        , iif(PL.overtimeRate = '' or PL.overtimeRate is NULL,'',concat('Overtime Pay Rate: ',PL.overtimeRate,char(10)))
        , iif(cast(PL.dateAdded as varchar(10)) = '' or PL.dateAdded is NULL,'',concat('Date Added: ',convert(varchar(10),PL.dateAdded,120)
			,char(10),iif(CONVERT(NVARCHAR(MAX), PL.comments) = '' or PL.comments is null,'',concat('Comments: ',CONVERT(NVARCHAR(MAX), PL.comments))) ))
			) as PlacementNote
from bullhorn1.BH_Placement PL
left join bullhorn1.BH_UserContact UC on PL.billingUserID = UC.userID)
--select * from tmp_4

, PlacementNote (jobPostingID, PlacementNote) as (SELECT
     jobPostingID,
     STUFF(
         (SELECT DISTINCT ' || ' + PlacementNote
          from  tmp_4
          WHERE jobPostingID = a.jobPostingID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 4, '')  AS URLList
FROM tmp_4 as a
GROUP BY a.jobPostingID)

select a.jobPostingID as 'position-externalId' 
	, b.clientID as 'position-contactId'
	, uc.firstname as '(ContactFirstName)'
	, uc.lastname as '(ContactLastName)'
	, cc.name as '(CompanyName)'
	, a.clientUserID as '(UserID)'
	, case when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
	, a.numOpenings as 'position-headcount'
	, mail5.email as 'position-owners'
/* This field only accepts FULL_TIME, PART_TIME, CASUAL */
	, a.type as '(position-employmentType)'	
/* This field only accepts PERMANENT, INTERIM_PROJECT_CONSULTING,TEMPORARY,CONTRACT, TEMPORARY_TO_PERMANENT */
	--, replace(replace(replace(replace(replace(replace(a.employmentType,'Contingent','TEMPORARY'),'Permanent','PERMANENT'),'ReverseMarket','INTERIM_PROJECT_CONSULTING'),'Exclusive','PERMANENT'),'Fixed Contract','CONTRACT'),'Fixed to Perm','TEMPORARY_TO_PERMANENT') as 'position-type'
	, case when a.employmentType = 'Direct Hire' then 'PERMANENT'
	when a.employmentType = 'Contract' then 'CONTRACT'
	when a.employmentType = 'Contract To Hire' then 'CONTRACT'
	else '' end as 'position-type'
	, a.salary as 'position-actualSalary'
	, cast(a.publicDescription as varchar(max)) as 'position-publicDescription'
	, cast(a.description as varchar(max)) as 'position-internalDescription'
	, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	, convert(varchar(10),iif(a.status = 'Lead' or a.status = 'Closed',getdate()-2,dateClosed),120) as 'position-endDate'
	, jobdoc.jobFile as 'position-document'
	, left(concat(note.AdditionalNote,char(10),PN.PlacementNote),32000) as 'position-note'
-- select count(*) --13.455
from bullhorn1.BH_JobPosting a
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientCorporationID = CC.clientCorporationID
left join mail5 ON a.userID = mail5.ID
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join PlacementNote PN on a.jobPostingID = PN.jobPostingID
left join Note on a.jobPostingID = note.jobPostingID
left join job on a.jobPostingID = job.jobPostingID
left join jobdoc on a.jobPostingID = jobdoc.jobPostingID
where b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID