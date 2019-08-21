select count(*) from Companies -- 17221 rows
----------------------
select count(*) from dbo.Contacts CL where CL.type = 'candidate'
select * from Contacts where DESCRIPTOR = 0 -- 305 rows
select * from Contacts where DESCRIPTOR is null -- 3 rows
select count(*) from Contacts where DESCRIPTOR = 1 -- 43520 rows
select count(*) from Contacts where DESCRIPTOR = 2 -- 99647 rows
select count(*) from Contacts where DESCRIPTOR > 2 -- 142 rows
select distinct DESCRIPTOR from Contacts -- Found Values: (null)0,1,2,4,5,6,8,10
select distinct type from Contacts where DESCRIPTOR < 3 -- Found values: (null),(empty),Active,Candidate,Client,Competitor,Contact,Friend, Placed Candidate, Prospective Client, Supplier,Works for Client)
select count(*) from Contacts where DESCRIPTOR < 3 and type = 'Contact'

select top 100
contactid
,CL.Username
, concat(
		  case when (CL.Email like '%@%') THEN CL.Email ELSE '' END
		, case when (CL.Email like '%@%' and CL.Email2 like '%@%') THEN concat (', ',CL.Email2) else '' END	
	) as 'contact-email'
-- select *
 from Contacts CL where CL.displayname like '%Janie Emmerson%'
 
 select CL.username, case when (CL.Email like '%@%') THEN CL.Email ELSE '' END as 'email' from Contacts CL where CL.displayname = CL.username and CL.Email != ''
  
Janie Emmerson
Julien Wondja Dooh
Amandeep Gill
Amir Hedayat


-----------------------
select count(*) from dbo.Contacts CL where CL.type != 'candidate'
select count(*) from Candidates CA left join Contacts CL on CL.contactid = CA.contactid 

select count(*) from Contacts
--select top 100 *
from Candidates CA --113957
left join Contacts CL on CA.contactid = CL.contactid --113957

select count(*) from Contacts CL where CL.type like '%candid%' --89699
select count(*) from Contacts CL where CL.type = 'candidate' --89595

select count(*) from dbo.Candidates -- 113957 rows
select * from dbo.Candidates
select distinct DESCRIPTOR from dbo.Candidates -- Found Values: 0,2
select distinct CL.Title from dbo.Contacts CL where CL.type = 'candidate'
select distinct CL.country from dbo.Contacts CL where CL.type = 'candidate'
select distinct CA.Nationality from candidates CA
select distinct  from candidates CA

select CL.email from dbo.Contacts CL where CL.type = 'candidate' and CL.email not like '%@%' and CL.email != '' and CL.email is not null --replace( ,'?',''),'&',''),'gmail','@gmail'),'yahoo','@yahoo')

select top 50
	case when ( cast(CL.DirectTel as varchar(max)) != '' and cast(CL.DirectTel as varchar(max)) is not null) then cast(CL.DirectTel as varchar(max)) else
	 (case when ( cast(CL.MobileTel as varchar(max)) != '' and cast(CL.MobileTel as varchar(max)) is not null) then cast(CL.MobileTel as varchar(max)) else CL.WorkTel end)
	 end as 'candidate-phone' --primary phone
	--, CL.DirectTel as 'candidate-phone'
	, CL.DirectTel as 'candidate-DirectTel'
	, CL.MobileTel as 'candidate-mobile'
	, CL.WorkTel as 'candidate-workPhone'
	, CL.HomeTel as 'candidate-homePhone'
from dbo.Contacts CL
----------------------
select count(*) from vacancies where status = 0 -- 470
select count(*) from vacancies where status = 1 -- 7400
select top 100 * from vacancies
select count(*) from vacancies -- 7870 rows
select distinct status from vacancies -- Found Values: 0,1
select distinct jobstatus from vacancies -- Found Values: Withdrawn,Closed,Filled,Permanent ,Acitve ,(null),,speculative,live,Active
select distinct vacancies.FullTimeJob from vacancies
select distinct vacancies.PermanentJob from vacancies

SELECT
	  j.JobNumber as 'position-externalId'
	, j.ContactId as 'position-contactId'
	--, j.clientUserID as 'UserID'
	, j.JobTitle as 'position-title'
	, j.DisplayName as 'Contact Name'
	, j.UserName as '(Job Owner)'
	--, case when (CL.Email like '%@%') THEN CL.Email ELSE (case when (CL.Email2 like '%@%') THEN CL.Email2 else '' END) END as 'position-owners'
	, j.Company as 'Company Name'
from vacancies j inner join Contacts CL on j.ContactId = CL.ContactId

select
	j.JobStatus as 'Job Status (Start-End date)'
	--,CONVERT(VARCHAR(10),j.RegDate,120) as 'position-regDate'
	, case when j.startdate is null then CONVERT(VARCHAR(10),j.regdate,120) else CONVERT(VARCHAR(10),j.startdate,120) end as 'position-startdate'
	--, case when cast(j.JobStatus as nvarchar(max)) in ('Closed','Filled') then (cast(getdate() -1 as nvarchar(max))) else cast(j.JobStatus as nvarchar(max)) end as 'position-enddate'
	--, case when j.JobStatus in ('Closed','Filled','Withdrawn') then (cast(getdate() -1 as nvarchar(max))) else '' end as 'position-enddate'
	, case when j.JobStatus in ('Closed','Filled','Withdrawn') then CONVERT(VARCHAR(10),getdate() - 1,120) else '' end as 'position-enddate'
	
from vacancies j

---------------------

SELECT contactid, phone = STUFF((SELECT ', ' + DirectTel + ', ' + WorkTel + ', ' + MobileTel + ', ' + HomeTel FROM Contacts b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 1, '') FROM Contacts a 
SELECT contactid, phone = STUFF((SELECT ', ' + DirectTel + '| ' + WorkTel + '|| ' + MobileTel + '||| ' + HomeTel FROM Contacts b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 1, '') FROM Contacts a 
where contactid = '592215-5257-976'
GROUP BY contactid

select  case when (CL.DirectTel = '' OR CL.DirectTel is NULL) THEN 
	(case when (CL.MobileTel != '' OR CL.MobileTel is not NULL) THEN CONCAT(CL.MobileTel,' ,',CL.WorkTel,' ,',CL.HomeTel) else concat(CL.WorkTel,',',CL.HomeTel) end)
	 else concat(CL.DirectTel
	,case when (CL.MobileTel = '' OR CL.MobileTel is NULL) THEN '' ELSE CONCAT(', ',CL.MobileTel) END
	,case when (CL.WorkTel = '' OR CL.WorkTel is NULL) THEN '' ELSE CONCAT(' ,',CL.WorkTel) END
	,case when (CL.HomeTel = '' OR CL.HomeTel is NULL) THEN '' ELSE CONCAT(' ,',CL.HomeTel) END
	) END as phone
,concat(CL.DirectTel,',',CL.MobileTel,',',CL.WorkTel,',',CL.HomeTel) as 'contact-phone'
from Contacts CL
where --contactid = '592215-5257-976' and
CL.DirectTel is null or CL.DirectTel = ''
CL.WorkTel is not null and CL.WorkTel != ''
and CL.MobileTel is not null and CL.MobileTel != ''
and CL.HomeTel is not null and CL.HomeTel != ''

SELECT  ISNULL(CAST(CL.DirectTel AS VARCHAR(50)),'') + COALESCE(CONVERT(VARCHAR(50),CL.MobileTel),'')
from Contacts CL
----------------
select count(*) from Attachments -- 76177
where attachmenttype = 4 -- 25635
select top 10 *  from Attachments



----------
select top 10 * from Candidates
select top 10 * from Contacts

select count(*) from Candidates CA--113.957
select CA.contactid from Candidates CA group by CA.contactid having count(*) > 1

select count(*) from Contacts CL --143.617
select CL.contactid from Contacts CL group by CL.contactid having count(*) > 1


select count(*) from Contacts CL inner join Candidates CA on CL.contactid = CA.contactid where CL.Descriptor = 2 --99633
select count(*) from Contacts CL LEFT JOIN Candidates CA on CL.contactid = CA.contactid WHERE CL.Descriptor = 2 and CA.contactid IS NULL --14
select count(*) from Contacts CL LEFT JOIN Candidates CA on CL.contactid = CA.contactid WHERE CA.contactid IS NULL --43489


select distinct DESCRIPTOR from Candidates
select distinct DESCRIPTOR from Contacts


------------------------
