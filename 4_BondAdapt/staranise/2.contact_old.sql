
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
from bullhorn1.BH_Client Cl
where Cl.isPrimaryOwner = 1)
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

*/

with
 tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ', ' + TEL_NUMBER from  PROP_TELEPHONE WHERE REFERENCE = a.REFERENCE FOR XML PATH ('')), 1, 1, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
--select * from tel
, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ',' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY =6532841 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--select * from doc 

select top 50 
--select
	cc.CONTACT as 'contact-externalId'
	, cc.CLIENT as 'contact-companyId'
	, pg.FIRST_NAME as 'contact-firstName'
	, pg.LAST_NAME as 'contact-lastName'
	, pg.MIDDLE_NAME as 'contact-middleName'
	, mail.EMAIL_LINK as 'contact-email'
	, tel.TEL_NUMBER as 'contact-phone'
	, pg.JOB_TITLE as 'contact-jobTitle'
	, pg.LINKED_IN as 'contact-linkedin'
	--, ISNULL(REPLACE(cast(UC.lastNote_denormalized as nvarchar(max)),CHAR(13),''), '') as 'contact-Note'
	, replace(doc.DOC_ID,'.txt','.rtf') as 'contact-Note'
	, own.EMPLOYEE_NAME as 'contact-owners'
	--, left(replace(d.Notes,'&#x0D;',''),32000) as 'contact-comments'
--select count(*) --21158 rows
--select top 10 *
--select * 
from dbo.PROP_PERSON_GEN pg 
--inner JOIN PROP_X_CLIENT_CON cc ON pg.REFERENCE = cc.CONTACT
left join (select CONTACT, max(client) as CLIENT from PROP_X_CLIENT_CON group by CONTACT) cc ON pg.REFERENCE = cc.CONTACT
--INNER JOIN PROP_EMAIL email on pg.REFERENCE = email.REFERENCE
left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON pg.REFERENCE = mail.REFERENCE
left join tel ON pg.REFERENCE = tel.REFERENCE
left join (select CONS.REFERENCE, EMPLOYEE.NAME as EMPLOYEE_NAME, CLIENT_GEN.NAME as CLIENT_GEN_NAME from PROP_CLIENT_GEN CLIENT_GEN INNER JOIN PROP_OWN_CONS CONS ON CLIENT_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent') own ON pg.REFERENCE = own.REFERENCE
left join doc on pg.REFERENCE = doc.OWNER_ID
--where own.EMPLOYEE_NAME is not null

--left join PROP_TELEPHONE landline on pg.REFERENCE = landline.TEL_NUMBER
--left join tmp_3 b on ltrim(rtrim(cast(UC.ownerUserIDList as nvarchar(max)))) = cast(b.userID as nvarchar(max))

