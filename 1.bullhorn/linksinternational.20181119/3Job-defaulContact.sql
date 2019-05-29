
with
--JOB DUPLICATION REGCONITION
job (jobPostingID,clientID,title,starDate,rn) as (
	SELECT  a.jobPostingID as jobPostingID
		, b.clientID as clientID
		, iif(a.title <> '', ltrim(rtrim(a.title)), 'No JobTitle') as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,a.title,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
	/*where b.isPrimaryOwner = 1*/) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
-- select * from job where title like '%receptionist%'


select --top 100
         a.jobPostingID as 'position-externalId'
	, iif(b.clientID is null, 'default', convert(varchar(max),b.clientID)) as 'position-contactId'
       , a.clientUserID as '#UserID', uc.firstname as 'ContactFirstName', uc.lastname as 'ContactLastName', b.isdeleted, b.status, concat('default',cc.clientcorporationid) as 'contact-externalId'
       , CC.clientcorporationid as 'contact-companyId', cc.name as 'CompanyName', cc.status
	, case when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
-- select distinct cc.clientcorporationid as 'contact-companyId', concat('default',cc.clientcorporationid) as 'contact-externalId', 'Default Contact' as 'lastname' -- select count(*)
from bullhorn1.BH_JobPosting a
left join ( select userID, clientcorporationid, isdeleted, status, max(clientID) as clientID from bullhorn1.BH_Client where (isdeleted = 1 or status = 'Archive') group by userID, clientcorporationid, isdeleted, status ) b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON a.clientcorporationid = CC.clientcorporationid
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join job on a.jobPostingID = job.jobPostingID
where a.isdeleted <> 1 and a.status <> 'Archive'
and b.clientID is not null
--and a.jobpostingid in (20272)
and cc.status not in ('Archive')

--select * from  bullhorn1.BH_JobPosting a where a.jobpostingid in (20272)
--select * from bullhorn1.BH_ClientCorporation CC where CC. clientCorporationID in (2071)
