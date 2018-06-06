with 
tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email from bullhorn1.BH_UserContact )
 --select * from tmp_1
 --select userID, email, CHARINDEX(email,',',0) from tmp_1
 , tmp_2(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)	ELSE email END as email from tmp_1)
 , tmp_3(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) 	THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END) ELSE email END as email from tmp_2)

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
	, CONVERT(VARCHAR(10),a.startDate,110) as 'position-startDate'
	, a.dateClosed as 'position-endDate'
from bullhorn1.BH_JobPosting a
left join bullhorn1.BH_Client b on a.clientUserID = b.userID
left join tmp_3 c ON a.userID = c.userID
where a.status not like '%Archive%'
--where a.jobPostingID = 1096
--order by a.jobPostingID
