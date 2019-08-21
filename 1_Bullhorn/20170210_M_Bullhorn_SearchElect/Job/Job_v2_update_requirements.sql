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
, concat('BH Job ID:',JP.jobPostingID
,'Employment Type: ',JP.employmentType,char(10)
,'Priority: ',JP.type,char(10)
, iif(JP.salary = '' or JP.salary is NULL,'',concat('Salary: ',JP.salary))
, iif(JP.feeArrangement = '' or JP.feeArrangement is NULL,'',concat('Fee arrangement: ',JP.feeArrangement))
, iif(JP.publishedCategoryID = '' or JP.publishedCategoryID is NULL,'',concat('Publish Category: ',JP.publishedCategoryID,' - ',CL.occupation))
, iif(JP.skills = '' or JP.skills is NULL,'',concat('Required skills: ',JP.skills))
, iif(JP.yearsRequired = '' or JP.yearsRequired is NULL,'',concat('Years required: ',JP.yearsRequired))
, iif(CC.address1 = '' or CC.address1 is NULL,'',concat('Company address: ',CC.address1)) 
) as AdditionalNote
from bullhorn1.BH_JobPosting JP
left join bullhorn1.BH_CategoryList CL on JP.publishedCategoryID = CL.categoryID
left join bullhorn1.BH_ClientCorporation CC on JP.clientCorporationID = CC.clientCorporationID)

select a.jobPostingID as 'position-externalId' 
	, b.clientID as 'position-contactId'
	, a.clientUserID as 'UserID'
	, a.title as 'position-title'
	, a.numOpenings as 'position-headcount'
	, c.email as 'position-owners' 
	, a.type as 'position-type'
	, a.employmentType as 'position-employmentType'	
	, a.salary as 'position-actualSalary'
	, a.publicDescription as 'position-publicDescription'
	, a.description as 'position-internalDescription'
	, CONVERT(VARCHAR(10),a.startDate,120) as 'position-startDate'
	, convert(varchar(10),iif(a.status = 'Lead',getdate()-2,dateClosed),120) as 'position-endDate'
from bullhorn1.BH_JobPosting a
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left join tmp_3 c ON a.userID = c.userID;


/*
select * from bullhorn1.BH_JobPosting;

select * from bullhorn1.BH_CategoryList;

select * from bullhorn1.BH_ClientCorporation;
*/