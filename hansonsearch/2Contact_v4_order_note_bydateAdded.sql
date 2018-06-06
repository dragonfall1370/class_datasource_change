/*
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
         (SELECT char(10) + 'Date added: ' + convert(varchar(10), dateAdded, 120) + ' || ' + 'Action: ' + action + ' || ' + cast(comments as varchar(max))
          from  [bullhorn1].[BH_UserComment]
          WHERE userID = a.userID
		  order by dateAdded desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
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

, tmp_6 as (select userID, concat(Status1,char(10),Department,char(10),DesignedCategories,char(10),DesiredSpecialties,char(10),DesiredSkills) as CombinedNote from tmp_5)
--select * from tmp_6
*/

-- this is written for all custom required fields from Search Elect
/*, tmp_6_1 as (select userID
	, concat(iif(address1 = '' or address1 is NULL,'',concat('Address 1: ',address1,' | '))
	,iif(city = '' or city is NULL,'',concat('City: ',city,' | '))
	,iif(state = '' or state is NULL,'',concat('State: ',state,' | '))
	,iif(cast(countryID as varchar(max)) = '' or countryID is NULL,'',concat('Country: ',tmp_country.COUNTRY,' | '))
	,iif(employmentPreference = '' or employmentPreference is NULL,'',concat('Employment Preference: ',employmentPreference))
	) as AdditionalNote 
	from bullhorn1.BH_UserContact UC 
	left join tmp_country on UC.countryID = tmp_country.CODE)
*/

/*
, tmp_6_1 as (select userID
	, concat(iif(address1 = '' or address1 is NULL,'',concat('Address 1: ',address1,char(10)))
		,iif(city = '' or city is NULL,'',concat('City: ',city,char(10)))
		,iif(state = '' or state is NULL,'',concat('State: ',state,char(10)))
		,iif(cast(countryID as varchar(max)) = '' or countryID is NULL,'',concat('Country: ',tmp_country.COUNTRY,char(10)))
		,iif(employmentPreference = '' or employmentPreference is NULL,'',concat('Employment Preference: ',employmentPreference))
	) as AdditionalNote 
	from bullhorn1.BH_UserContact UC 
	left join tmp_country on UC.countryID = tmp_country.CODE)
--select * from tmp_6_1

, tmp_7(userID, phone) as (SELECT
     userID,
     STUFF(
         (SELECT iif(phone = '' or phone is NULL,'',concat(phone,',')) + 
		 iif(phone2 = '' or phone2 is NULL,'',concat(phone2,',')) + 
		 iif(phone3 = '' or phone3 is NULL,'',concat(phone3,',')) +
		 iif(mobile = '' or mobile is NULL,'',concat(mobile,',')) +
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

, tmp_doc0 (userid, name) as (select a.USERID, concat(a.clientCorporationFileID,'-co-doc',a.fileExtension) from bullhorn1.BH_ClientCorporationFile a)
--select * from tmp_5
--where a.type = 'Resume') ==> get all candidates files
, tmp_doc1 (userid, ResumeId) as (SELECT userid, STUFF((SELECT DISTINCT ',' + name from tmp_doc0 WHERE userid = a.USERID FOR XML PATH ('')), 1, 1, '')  AS URLList FROM tmp_doc0 AS a GROUP BY a.USERID)
--select * from tmp_6 --order by clientCorporationID
*/

with 
skill as (select c.contactid,ss.skill from Contacts c left join SkillInstances si on c.contactid = si.objectid  left join skills ss on si.skillid = ss.skillid)

, mail1 (candidateID,email) as (select ContactId, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(concat(ltrim(rtrim(email)),',',ltrim(rtrim(email2))),'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' ') as email from Contacts WHERE type in ('Client','Competitor','Contact','Friend','futurist','Prospective Client','Supplier') )
, mail2 (candidateID,email) as (SELECT candidateID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT candidateID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (candidateID,email) as (SELECT candidateID, email from mail2 WHERE email like '%_@_%.__%')
--, mail4 (candidateID,email,rn) as ( SELECT candidateID, email = CONVERT(NVARCHAR(MAX), email), r1 = ROW_NUMBER() OVER (PARTITION BY candidateID ORDER BY candidateID desc) FROM mail3 )
, mail4a (candidateID,email,rn) as ( SELECT candidateID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY candidateID ORDER BY candidateID desc) FROM mail3 )
, mail4 (candidateID,email,rn) as ( select candidateID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email, rn from mail4a ) --where RIGHT(email, 1) = '.'
--, mailpe as (select candidateID,email as email1 from mail4 where rn = 1)
--, mailwe as (select candidateID,email as email1 from mail4 where rn = 2)
--, mailoe as (SELECT candidateID,email = STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and candidateID = a.candidateID FOR XML PATH ('')), 1, 1, '') FROM mail4 AS a where rn >2 GROUP BY a.candidateID)
, mail5 (candidateid, email1, email2, email3) as (
		select pe.candidateID, email as email1, we.email2, oe.email3 from mail4 pe
		left join (select candidateID, email as email2 from mail4 where rn = 2) we on we.candidateID = pe.candidateID
		left join (SELECT candidateID, STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and candidateID = a.candidateID FOR XML PATH ('')), 1, 1, '')  AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.candidateID ) oe on oe.candidateid = pe.candidateid
		where pe.rn = 1 )
-- select  * from mail5 where email1 = 'phoughton@man.co.uk'
-- select top 100 contactid, email,email2 from contacts where ContactId like '%43341%' or ContactId like '%43342%' or ContactId like '%43344%' or ContactId like '%43345%'


-- select top 100
 select
          Coalesce(NULLIF(CC.CompanyId, ''), '12345') as 'contact-companyId'
	, CC.CompanyName as '(contact-companyname)'
	, CL.ContactId as 'contact-externalId'
	--, case when ( CL.FirstName = '' or CL.FirstName is null) then 'No Firstname' else replace(CL.FirstName,'?','') end as 'contact-firstName'
	--, case when ( CL.LastName = '' or  CL.LastName is null) then 'No Lastname' else replace(CL.LastName,'?','') end as 'contact-lastName'
	, Coalesce(NULLIF(replace(CL.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
	, Coalesce(NULLIF(replace(CL.LastName,'?',''), ''), 'No Lastname') as 'contact-lastName'
	--, UC.middleName as 'contact-middleName'
        /*, concat(
		  case when (CL.Email like '%@%') THEN CL.Email ELSE '' END
		, case when (CL.Email like '%@%' and CL.Email2 like '%@%') THEN concat (', ',CL.Email2) else '' END	
	) as 'contact-email' */
	, Coalesce(NULLIF(mail5.email1, ''), 'contact-' + CL.ContactId + '@noemail.com') as 'contact-email'
	--, CL.DirectTel as 'contact-phone1'
	--, CL.WorkTel as 'contact-phone2'
	--, CL.MobileTel as 'contact-phone3'
	--, CL.HomeTel as 'contact-phone4'
	/*,case when (CL.DirectTel = '' OR CL.DirectTel is NULL) THEN 
		(case when (CL.MobileTel != '' OR CL.MobileTel is not NULL) THEN CONCAT(CL.MobileTel, ifnull('',concat(' ,',CL.WorkTel)), ifnull('',concat(' ,',CL.HomeTel)) else concat(CL.WorkTel,',',CL.HomeTel) end)
	 	else concat(CL.DirectTel
			,case when (CL.MobileTel = '' OR CL.MobileTel is NULL) THEN '' ELSE CONCAT(', ',CL.MobileTel) END
			,case when (CL.WorkTel = '' OR CL.WorkTel is NULL) THEN '' ELSE CONCAT(' ,',CL.WorkTel) END
			,case when (CL.HomeTel = '' OR CL.HomeTel is NULL) THEN '' ELSE CONCAT(' ,',CL.HomeTel) END
	) END as 'contact-phone' */
	, ltrim(Stuff( Coalesce(' ' + NULLIF(DirectTel, ''), '')
                        + Coalesce(', ' + NULLIF(MobileTel, ''), '')
                        + Coalesce(', ' + NULLIF(WorkTel, ''), '')
                        + Coalesce(', ' + NULLIF(HomeTel, ''), '')
                , 1, 1, '') 
        ) as 'contact-phone'
	
	--, as 'contact-owners'
	, CL.UserId as '(contact-owners-id)'
	, CL.UserName as '(contact-owners-name)'
	, owner.email as 'contact-owners'
	, CL.JobTitle as 'contact-jobTitle'
	--, CL.userId as 'Contact External ID'
	, at.Filename as 'contact-document'
	--, concat('BH Contact owners: ',UC2.name,char(10),e.AdditionalNote,char(10),f.CombinedNote,char(10),UC.lastNote_denormalized) as 'contact-Note'

	, concat(
		  case when (CL.Comments = '' OR CL.Comments is NULL) THEN '' ELSE concat ('Comments: ',CL.Comments,char(10)) END
		, case when (n1.NotesID = '' OR n1.NotesId is NULL) THEN '' ELSE concat ('NotesId: ',n1.NotesId,char(10)) END
		, case when (n.Text = '' OR n.Text is NULL) THEN '' ELSE replace(concat (n.Text,char(10)),'&amp; ','') END
	) as 'contact-comment'

	, concat(
		--  case when (CL.RegDate = '' OR CL.RegDate is NULL) THEN '' ELSE concat ('RegDate: ',CL.RegDate,char(10)) END
		--  case when (CL.Title = '' OR CL.Title is NULL) THEN '' ELSE concat ('Title: ',CL.Title,char(10)) END
		 case when (mail5.email2 is NULL) THEN '' ELSE concat ('Other Email: ',mail5.email2,char(10)) END
		, case when (CL.Type = '' OR CL.Type is NULL) THEN '' ELSE concat('Type: ',CL.Type,char(10)) end
		, case when (CL.ContactStatus = '' OR CL.ContactStatus is NULL) THEN '' ELSE concat ('Status: ',CL.ContactStatus,char(10)) END
		, case when (CL.ContactSource = '' OR CL.ContactSource is NULL) THEN '' ELSE concat ('Source: ',CL.ContactSource,char(10)) END
		--, case when (CL.Department = '' OR CL.Department is NULL) THEN '' ELSE concat ('Department: ',CL.Department,char(10)) END --DIRECT INJECTION
		--, case when (CL.Sector = '' OR CL.Sector is NULL) THEN '' ELSE concat ('Sector: ',CL.Sector,char(10)) END
		--, case when (CL.Segment = '' OR CL.Segment is NULL) THEN '' ELSE concat ('Segment: ',CL.Segment,char(10)) END
		--, case when (CL.Discipline = '' OR CL.Discipline is NULL) THEN '' ELSE concat ('Discipline: ',CL.Discipline,char(10)) END
		--, replace(skills.skill,'&amp; ','') as 'contact-skill'
		--, case when (skills.skill = '' OR skills.skill is NULL) THEN '' ELSE concat ('Skills: ',replace(skills.skill,'&amp; ','')) END --DIRECT INJECTION
		, case when (CL.WebSite = '' OR CL.WebSite is NULL) THEN '' ELSE concat ('WebSite: ',CL.WebSite,char(10)) END
		--, case when (CL.Address1 = '' OR CL.Address1 is NULL) THEN '' ELSE concat ('Address1: ',CL.Address1,char(10)) END --DIRECT INJECTION
		--, case when (CL.Address2 = '' OR CL.Address2 is NULL) THEN '' ELSE concat ('Address2: ',CL.Address2,char(10)) END --DIRECT INJECTION
		--, case when (CL.Address3 = '' OR CL.Address3 is NULL) THEN '' ELSE concat ('Address3: ',CL.Address3,char(10)) END --DIRECT INJECTION
		--, case when (CL.City = '' OR CL.City is NULL) THEN '' ELSE concat ('City: ',CL.City,char(10)) END --DIRECT INJECTION
		--, case when (CL.County = '' OR CL.County is NULL) THEN '' ELSE concat ('County: ',CL.County,char(10)) END --DIRECT INJECTION (district)
		--, case when (CL.Country = '' OR CL.Country is NULL) THEN '' ELSE concat ('Country: ',CL.Country,char(10)) END --DIRECT INJECTION (UK and France only)
		--, case when (CL.PostCode = '' OR CL.PostCode is NULL) THEN '' ELSE concat ('PostCode: ',CL.PostCode,char(10)) END  --DIRECT INJECTION
		--, case when (CL.Location = '' OR CL.Location is NULL) THEN '' ELSE concat ('Location: ',CL.Location,char(10)) END  --DIRECT INJECTION (if Country empty)
		--, case when (CL.SubLocation = '' OR CL.SubLocation is NULL) THEN '' ELSE concat ('SubLocation: ',CL.SubLocation,char(10)) END  --DIRECT INJECTION (If City empty)
		--, case when (CC.companystatus = '' or CC.companystatus is null) then '' else concat('Company Status: ',CC.companystatus,char(10)) end
		--, case when (CL.LastUpdate = '' OR CL.LastUpdate is NULL) THEN '' ELSE concat ('LastUpdate: ',CL.LastUpdate,char(10)) END
		--, case when CL.Restricted = 'false' then 'Restricted: No' + char(10) when CL.Restricted = 'true' then 'Restricted: Yes' + char(10) end
                --, case when AgreedTerms = 'false' then 'Terms: No' + char(10) when AgreedTerms = 'true' then 'Terms: Yes' + char(10) end
                --,HotList
                --, case when AgreedToEmail = 'false' then 'Can email: No' + char(10) when AgreedToEmail = 'true' then 'Can Email: Yes' + char(10) end
                --, case when Newsletter = 'false' then 'Newsletter: No' + char(10) when Newsletter = 'true' then 'Newsletter: Yes' + char(10) end
	) as 'contact-Note'

-- select top 10 *
-- select count(*) --143617-
from Contacts CL --where cl.descriptor = 1
left join Companies CC ON CL.CompanyId = CC.CompanyId

left join (SELECT contactid, text = STUFF((SELECT char(10) + 'Note: ' + text + char(10) FROM notes b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 0, '') FROM notes a GROUP BY contactid) n on CL.contactid = n.contactid
left join (SELECT contactid, notesid = STUFF((SELECT ',' + notesid + char(10) FROM notes b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 1, '') FROM notes a GROUP BY contactid) n1 on CL.contactid = n1.contactid

left join (SELECT id, filename = STUFF((SELECT DISTINCT ',' + 'con_' + replace(filename,',','') from Attachments WHERE id = a.id FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') FROM Attachments a GROUP BY id) at on cl.contactid = at.Id
left join (SELECT contactid, skill = STUFF((SELECT skill + char(10) FROM skill b WHERE b.contactid = a.contactid FOR XML PATH('')), 1, 0, '') FROM skill a GROUP BY contactid) skills on CL.contactid = skills.contactid
left join mail5 on CL.contactid = mail5.candidateid
left join (select CL.username as name, Coalesce( NULLIF(cl.Email, ''), '') as email from Contacts CL where CL.Email like '%_@_%.__%' and CL.displayname = CL.username) owner on CL.username = owner.name
--WHERE CL.type in ('Client','Competitor','Contact','Friend','futurist','Prospective Client','Supplier')
where cl.descriptor = 1
--and CL.DESCRIPTOR < 3
--CL.Email != '' and CL.Email is not null and CL.Email2 != '' and CL.Email2 is not NULL
--and CL.contactid = '339984-6023-6318'
-- n.text is not null
--and CL.FirstName like '%John%' and CL.lastname like '%Dawson%'
--and CL.FirstName like '%Guy%' and CL.lastname like '%Youngman%'
--and CL.FirstName like '%Jamie%' and CL.lastname like '%Holyer%'

--skills.skill is not null
--and CL.contactid = '965800-2111-8231'

/*
--left join tmp_3 b on ltrim(rtrim(cast(UC.ownerUserIDList as nvarchar(max)))) = cast(b.userID as nvarchar(max))

select * from bullhorn1.BH_Client Cl 
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where Cl.userID = 112 

select phone, phone2, phone3, workphone
from bullhorn1.BH_UserContact UC
where userID = 5429 or userID = 5430 or userID = 5431
select distinct type from Contacts
*/


/*
select at.*
from Contacts CL
left join ( SELECT id, filename, ref, replace(ref,',','') as newref from Attachments ) at on cl.contactid = at.Id
where cl.descriptor = 1 and at.filename is not null and at.filename <> '' 
*/
