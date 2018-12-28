
with 
tmp_1(userID, email) as (select userID, REPLACE(ISNULL(email,'') + ',' + ISNULL(email2,'') + ',' + ISNULL(email3,''), ',,', ',') as email from bullhorn1.BH_UserContact )
--select * from tmp_1
--select userID, email, CHARINDEX(email,',',0) from tmp_1 

, tmp_2(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,1) = 1 THEN RIGHT(email, len(email)-1)	ELSE email END as email from tmp_1)

, tmp_3(userID, email) as (select userID, CASE WHEN CHARINDEX(',',email,len(email)) = len(email) THEN LEFT(email, CASE WHEN len(email) < 1 THEN 0 ELSE len(email) - 1 END)	ELSE email END as email from tmp_2)

, tmp_4(userID, Notes) as (SELECT userID, STUFF(
    (SELECT ' || ' + 'Action: ' + action + ' || ' + convert(varchar(10), dateAdded, 120) + ': ' + cast(comments as varchar(max))
     from [bullhorn1].[BH_UserComment] WHERE userID = a.userID FOR XML PATH ('')), 1, 4, '')  AS URLList
FROM  [bullhorn1].[BH_UserComment] AS a GROUP BY a.userID)
--select * from tmp_4

, tmp_5 as (select userID 
, case when (Cl.division = '' OR Cl.division is NULL) THEN '' ELSE concat('Department: ',Cl.division) END as Department
, case when (Cl.status = '' OR Cl.status is NULL) THEN '' ELSE concat('Status: ',Cl.status) END as Status1
, case when (cast(Cl.desiredCategories as varchar(max)) = '' OR Cl.desiredCategories is NULL) THEN '' ELSE concat('Designed Categories: ',Cl.desiredCategories) END as DesignedCategories
, case when (cast(Cl.desiredSpecialties as varchar(max)) = '' OR Cl.desiredSpecialties is NULL) THEN '' ELSE concat('Desired Specialties: ',Cl.desiredSpecialties) END as DesiredSpecialties
, case when (cast(Cl.desiredSkills as varchar(max)) = '' OR Cl.desiredSkills is NULL) THEN '' ELSE concat('Desired Skills: ',Cl.desiredSkills) END as DesiredSkills
  from bullhorn1.BH_Client Cl 
  --where Cl.status not like '%Archive%'
  )
--select * from tmp_5

, tmp_6 as (select userID, concat(Status1,char(10),Department,char(10),DesignedCategories,char(10),DesiredSpecialties,char(10),DesiredSkills) as CombinedNote from tmp_5)
--select * from tmp_6

select UC.clientCorporationID as 'contact-companyId'
	, Cl.clientID as 'contact-externalId'
	, Cl.userID
	, UC.firstName as 'contact-firstName'
	, UC.lastName as 'contact-lastName'
	, UC.middleName as 'contact-middleName'
	, UC2.email as 'contact-owners'
	, UC2.name as 'Owners-name'
	, UC.occupation as 'contact-jobTitle'
  --, ISNULL(REPLACE(cast(UC.lastNote_denormalized as nvarchar(max)),CHAR(13),''), '') as 'contact-Note'
	, UC.lastNote_denormalized as 'contact-Note'
	, concat(f.CombinedNote, d.Notes) as 'contact-comment'
  --, UC.phone as 'contact-phone'
	, case when UC.phone not LIKE '%[0-9]%' THEN '' ELSE UC.phone END as 'contact-phone'
    , case when c.email not LIKE '%@%' THEN '' ELSE c.email END as 'contact-email'
  --, case when c.email not LIKE '%_@_%_.__%' THEN '' ELSE c.email END as 'contact-email'
  --, c.email as 'contact-email'
from bullhorn1.BH_Client Cl 
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
--left join tmp_3 b on ltrim(rtrim(cast(UC.ownerUserIDList as nvarchar(max)))) = cast(b.userID as nvarchar(max))
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
left join tmp_3 c ON Cl.userID = c.userID
left join tmp_4 d on Cl.userID = d.userID
left join tmp_6 f on Cl.userID = f.userID
--where UC2.email LIKE '%_@_%_.__%' and c.email LIKE '%_@_%_.__%'
--left JOIN bullhorn1.BH_ClientCorporation CC ON Cl.clientCorporationID = CC.clientCorporationID
where Cl.status not like '%Archive%' -- and Cl.status <> ''
