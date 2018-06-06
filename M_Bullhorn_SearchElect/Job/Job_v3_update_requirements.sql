with tmp_1(userID, email) as 
(select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email
from bullhorn1.BH_UserContact
 )
 --select * from tmp_1
 --select userID, email, CHARINDEX(email,',',0) from tmp_1
 , tmp_2(userID, email) as (
select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)
	ELSE email END as email
from tmp_1
)
 , tmp_3(userID, email) as (
select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) 
	THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END)
	ELSE email END as email
from tmp_2
)

, Note as (
select jobPostingID
, concat('BH Job ID:',JP.jobPostingID,char(10)
, 'Position type: ',JP.type,char(10)
, 'Employment Type: ',JP.employmentType,char(10)
, 'Priority: ',JP.type,char(10)
, iif(JP.salary = '' or JP.salary is NULL,'',concat('Salary: ',JP.salary))
, iif(JP.feeArrangement = '' or JP.feeArrangement is NULL,'',concat('Fee arrangement: ',JP.feeArrangement))
, iif(JP.publishedCategoryID = '' or JP.publishedCategoryID is NULL,'',concat('Publish Category: ',JP.publishedCategoryID,' - ',CL.occupation))
, iif(cast(JP.skills as varchar(max))= '' or JP.skills is NULL,'',concat('Required skills: ',JP.skills))
, iif(JP.yearsRequired = '' or JP.yearsRequired is NULL,'',concat('Years required: ',JP.yearsRequired))
, iif(CC.address1 = '' or CC.address1 is NULL,'',concat('Company address: ',CC.address1)) 
) as AdditionalNote
from bullhorn1.BH_JobPosting JP
left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID)


/* Add placement information under job posting */
, tmp_4 as (select jobPostingID
, concat('Status: ',PL.status,char(10)
, iif(PL.reportTo = '' or PL.reportTo is NULL,'',concat('Report to: ',PL.reportTo,char(10)))
, iif(PL.costCenter = '' or PL.costCenter is NULL,'',concat('Cost Center: ',PL.costCenter,char(10)))
, iif(PL.billingUserID = '' or PL.billingUserID is NULL,'',concat('Billing User: ',PL.billingUserID,' - ',UC.firstName,' ',UC.lastName))
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
, iif(cast(PL.dateAdded as varchar(10)) = '' or PL.dateAdded is NULL,'',concat('Date added: ',convert(varchar(10),PL.dateAdded,120),' | ',PL.comments))
) as PlacementNote
from bullhorn1.BH_Placement PL
left join bullhorn1.BH_UserContact UC on PL.billingUserID = UC.userID)

, PlacementNote (jobPostingID, PlacementNote) as (SELECT
     jobPostingID,
     STUFF(
         (SELECT DISTINCT ' || ' + PlacementNote
          from  tmp_4
          WHERE jobPostingID = a.jobPostingID
          FOR XML PATH (''))
          , 1, 4, '')  AS URLList
FROM tmp_4 as a
GROUP BY a.jobPostingID)

select a.jobPostingID as 'position-externalId' 
	, b.clientID as 'position-contactId'
	, a.clientUserID as 'UserID'
	, a.title as 'position-title'
	, a.numOpenings as 'position-headcount'
	, c.email as 'position-owners'
	--, UC.email as 'position-owners'
	, a.type as 'position-type'
	, a.employmentType as 'position-employmentType'	
	, a.salary as 'position-actualSalary'
	, left(cast(a.publicDescription as varchar(max)),32000) as 'position-publicDescription'
	, left(cast(a.description as varchar(max)),32000) as 'position-internalDescription'
	, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	, convert(varchar(10),iif(a.status = 'Lead' or a.status = 'Closed',getdate()-2,dateClosed),120) as 'position-endDate'
	, left(concat(note.AdditionalNote,char(10),PN.PlacementNote),30000) as 'position-note'
from bullhorn1.BH_JobPosting a
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left join tmp_3 c ON a.userID = c.userID
--left join bullhorn1.BH_UserContact UC on a.userID = UC.userID
left join PlacementNote PN on a.jobPostingID = PN.jobPostingID
left join Note on a.jobPostingID = note.jobPostingID
where b.isPrimaryOwner = 1;

/*
select * from bullhorn1.BH_JobPosting;

select * from bullhorn1.BH_CategoryList;

select * from bullhorn1.BH_ClientCorporation;

select jobPostingID, count(jobPostingID)
from bullhorn1.BH_Placement PL
group by jobPostingID having count(jobPostingID) >1

*/