
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

, tmp_4(userID, Notes) as (SELECT
     userID,
     STUFF(
         (SELECT ' || ' + 'Action: ' + action + ' || ' + convert(varchar(10), dateAdded, 120) + ': ' + cast(comments as varchar(max))
          from  [bullhorn1].[BH_UserComment]
          WHERE userID = a.userID
          FOR XML PATH (''))
          , 1, 4, '')  AS URLList
FROM  [bullhorn1].[BH_UserComment] AS a
GROUP BY a.userID)
--select * from tmp_4
, 
tmp_5 as (select userID 
, case when (Cl.division = '' OR Cl.division is NULL) THEN '' ELSE concat('Department: ',Cl.division) END as Department
, case when (Cl.status = '' OR Cl.status is NULL) THEN '' ELSE concat('Status: ',Cl.status) END as Status1
, case when (cast(Cl.desiredCategories as varchar(max)) = '' OR Cl.desiredCategories is NULL) THEN '' ELSE concat('Designed Categories: ',Cl.desiredCategories) END as DesignedCategories
, case when (cast(Cl.desiredSpecialties as varchar(max)) = '' OR Cl.desiredSpecialties is NULL) THEN '' ELSE concat('Desired Specialties: ',Cl.desiredSpecialties) END as DesiredSpecialties
, case when (cast(Cl.desiredSkills as varchar(max)) = '' OR Cl.desiredSkills is NULL) THEN '' ELSE concat('Desired Skills: ',Cl.desiredSkills) END as DesiredSkills
from bullhorn1.BH_Client Cl where isPrimaryOwner = 1 and isDeleted = 0)
--select * from tmp_5

, tmp_6 as (select userID
, concat(Status1,char(10),Department,char(10),DesignedCategories,char(10)
,DesiredSpecialties,char(10),DesiredSkills) 
as CombinedNote from tmp_5)
--select * from tmp_6

/* this is written for all custom required fields from Search Elect */
, tmp_6_1 as (select userID
, concat(iif(address1 = '' or address1 is NULL,'',concat('Address 1: ',address1,' | '))
,iif(city = '' or city is NULL,'',concat('City: ',city,' | '))
,iif(state = '' or state is NULL,'',concat('State: ',state,' | '))
,iif(cast(countryID as varchar(max)) = '' or countryID is NULL,'',concat('Country: ',tmp_country.COUNTRY,' | '))
,iif(employmentPreference = '' or employmentPreference is NULL,'',concat('Employment Preference: ',employmentPreference))) 
as AdditionalNote from bullhorn1.BH_UserContact UC
left join tmp_country on UC.countryID = tmp_country.CODE)

--select * from tmp_6_1

, tmp_7(userID, phone) as (SELECT
     userID,
     STUFF(
         (SELECT iif(phone = '' or phone is NULL,'',concat(phone,',')) + 
		 iif(phone2 = '' or phone2 is NULL,'',concat(phone2,',')) + 
		 iif(phone3 = '' or phone3 is NULL,'',concat(phone3,',')) +
		 iif(workPhone = '' or workPhone is NULL or workPhone = '0','', workPhone)
          from  bullhorn1.BH_UserContact
          WHERE userID = a.userID
          FOR XML PATH (''))
          , 1, 0, '')  AS URLList
FROM  bullhorn1.BH_UserContact AS a
GROUP BY a.userID)


, tmp_7_1 (userID, phone) as (SELECT
     userID,
	 iif(right(phone,1)=',',left(phone,len(phone)-1),phone)
	 from tmp_7)

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
	, concat('BH Contact owners: ',UC2.name,char(10),e.AdditionalNote,char(10),f.CombinedNote,' || ',UC.lastNote_denormalized) as 'contact-Note'
	, left(replace(d.Notes,'&#x0D;',''),32000) as 'contact-comments'
	, g.phone as 'contact-phone'
	, c.email as 'contact-email'
from bullhorn1.BH_Client Cl 
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
--left join tmp_3 b on ltrim(rtrim(cast(UC.ownerUserIDList as nvarchar(max)))) = cast(b.userID as nvarchar(max))
left join bullhorn1.BH_UserContact UC2 on Cl.recruiterUserID = UC2.userID
left join tmp_3 c ON Cl.userID = c.userID
left join tmp_4 d on Cl.userID = d.userID
left join tmp_6 f on Cl.userID = f.userID
left join tmp_6_1 e on Cl.userID = e.userID
left join tmp_7_1 g on Cl.userID = g.userID
where isPrimaryOwner = 1 and CL.isDeleted = 0

--left JOIN bullhorn1.BH_ClientCorporation CC ON Cl.clientCorporationID = CC.clientCorporationID

/* --> check data validity
select * from bullhorn1.BH_Client Cl 
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where Cl.userID = 112 

select phone, phone2, phone3, workphone
from bullhorn1.BH_UserContact UC
where userID = 5429 or userID = 5430 or userID = 5431

*/