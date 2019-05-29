
with 
-- MAIL
  mail1 (ID,email) as (select userID, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2)),',',ltrim(rtrim(email3))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from bullhorn1.BH_UserContact )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail5 (ID,email) as (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail3 WHERE ID = a.ID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 2, '')  AS email FROM mail3 as a GROUP BY a.ID)
--select * from mail5 where id in (39188,14248,30223)

--JOB DUPLICATION REGCONITION
, job (jobPostingID,clientID,title,starDate,rn) as (
	SELECT  a.jobPostingID as jobPostingID
		, b.clientID as clientID
		, iif(a.title <> '', ltrim(rtrim(a.title)), 'No JobTitle') as title
		, CONVERT(VARCHAR(10),a.startDate,120) as starDate
		, ROW_NUMBER() OVER(PARTITION BY a.clientUserID,a.title,CONVERT(VARCHAR(10),a.startDate,120) ORDER BY a.jobPostingID) AS rn 
	from bullhorn1.BH_JobPosting a
	left join bullhorn1.BH_Client b on a.clientUserID = b.userID
	/*where b.isPrimaryOwner = 1*/) --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--select * from job where title like '%Sales & Marketing Manager TPU%'

--DOCUMENT
, doc as (SELECT jobPostingID
                 , STUFF((SELECT DISTINCT ',' + concat(jobPostingFileID,fileExtension) from bullhorn1.View_JobPostingFile WHERE jobPostingID = a.jobPostingID and fileExtension in ('.png') /*('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html','.txt')*/ FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') AS files 
                 FROM (select jobPostingID from bullhorn1.View_JobPostingFile) AS a GROUP BY a.jobPostingID)


select --top 100
         a.jobPostingID as 'position-externalId'
	, iif(b.clientID is null, 'default', convert(varchar(max),b.clientID)) as 'position-contactId'
       , a.clientUserID as '#UserID', cc.name as '#CompanyName', uc.firstname as '#ContactFirstName', uc.lastname as '#ContactLastName'
	, case when job.rn > 1 then concat(job.title,' ',rn) else job.title end as 'position-title'
	, doc.files as 'position-document'
-- select distinct employmentType -- select distinct Type -- select distinct salaryUnit -- select distinct customText5 -- select count(*) --2574 -- select top 100 startDate  -- select customFloat1, salary, customText5
from bullhorn1.BH_JobPosting a --where a.jobPostingID = 2539
left join ( select userID, clientcorporationid, max(clientID) as clientID from bullhorn1.BH_Client where isdeleted <> 1 and status <> 'Archive' group by userID, clientcorporationid ) b on a.clientUserID = b.userID
left JOIN bullhorn1.BH_ClientCorporation CC ON b.clientcorporationid = CC.clientcorporationid
left join bullhorn1.BH_UserContact UC on a.clientUserID = UC.userID
left join job on a.jobPostingID = job.jobPostingID

left join doc on a.jobPostingID = doc.jobPostingID
where a.isdeleted <> 1 and a.status <> 'Archive' --b.isPrimaryOwner = 1 --> add isPrimaryOwner = 1 to remove 1 userID having more than 1 clientID
--and job.title <> ''
--and a.jobPostingID in (25672)
