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
, concat('BH Job ID:',jobPostingID
,'Employment Type: ',employmentType,char(10)
,'Priority: ',type,char(10)
, feeArrangement
, externalCategoryID
, publishedCategoryID
, skills
, yearsRequired
) as note
from bullhorn1.BH_JobPosting
)

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
	, convert(varchar(10),iif(a.status = 'Lead',getdate()-1,dateClosed),120) as 'position-endDate'
 
from bullhorn1.BH_JobPosting a
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left join tmp_3 c ON a.userID = c.userID
where 1=1
and b.isPrimaryOwner = 1

--select * from bullhorn1.BH_JobPosting