with
--DOCUMENT / ACTIVITY ATTACHMENTS
 ContactAttachment as (select distinct ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName
, max(act.ModifiedOn) as ModifiedOn --> same contact may have same Subject file, so to remove duplicated files by max ModifiedOn
from ActivityContacts ac --> Remove comma in the file name
left join ActivitiesTable act on act.ActivityID = ac.ActivityID
where ac.ContactType = 1 and act.ActivityType = 2 --> ContactType 2 for Company, 1 for Contact, 3 for Job | ActivityType 2 is for File
and (act.Subject like '%.pdf' or act.Subject like '%.doc%' or act.Subject like '%.xls%' or act.Subject like '%.rtf' or act.Subject like '%.html' or act.Subject like '%.txt')
group by ac.ContactID, act.Subject)

, ContactFiles as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  ContactAttachment
          WHERE ContactID = a.ContactID
		  order by ModifiedOn desc
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS ContactFiles
FROM ContactAttachment as a
GROUP BY a.ContactID)

--CONTACT PHOTO
, ContactPhoto as (select ca.ContactServiceID, ca.AttachmentID, concat(a.ID,'_',a.AttachLongFileName) as ContactPhoto
from ContactAttachments ca
left join Attachments a on a.ID = ca.AttachmentID
where a.AttachmentContactPhoto = 1
and a.ID in (select max(AttachmentID) as maxID from ContactAttachments group by ContactServiceID)) --One contact may have multiple photos

/* Check if 1 contact may have multiple photos
select ContactServiceID, count(*)
from ContactPhoto
group by ContactServiceID
having count(*) > 1 
*/

--COMPANY REFERENCE
, CompanyRef as (select distinct a.ParentContactServiceID, b.FullName from ContactMainTable a
left join ContactMainTable b on a.ParentContactServiceID = b.ContactServiceID
where a.ParentType = 2)

--CONTACT PHONE
, ContactPhone as (select ContactServiceID
, stuff((coalesce(',' + WorkPhoneNum, '') + coalesce(',' + MobilePhoneNum, '') 
	+ coalesce(',' + HomePhoneNum, '') + coalesce(',' + OtherPhoneNum, '') + coalesce(',' + PhoneNum2, '') + coalesce(',' + PhoneNum3, '')
	), 1, 1, '' ) as ContactPhone
from ContactMainTable where Type = 1)

--CONTACT EMAIL
, CombinedEmail as (
select ContactServiceID, AddressBookEmailAddress1 as ContactEmail from ContactDetailsTable
UNION ALL
select ContactServiceID, AddressBookEmailAddress2 from ContactDetailsTable
UNION ALL
select ContactServiceID, AddressBookEmailAddress3 from ContactDetailsTable) 

, DistinctEmail as (SELECT distinct ContactServiceID, ContactEmail from CombinedEmail where ContactEmail is not NULL)

, EmailDupRegconition as (SELECT distinct ContactServiceID, ContactEmail, ROW_NUMBER() OVER(PARTITION BY ContactEmail ORDER BY ContactServiceID ASC) AS rn 
from DistinctEmail)

, ContactEmail as (select ContactServiceID
, case	when rn = 1 then ContactEmail
		else concat(ContactServiceID,'-',ContactEmail) end as ContactEmail
, rn
from EmailDupRegconition)

, ContactEmailFinal as (SELECT
     ContactServiceID,
     STUFF(
         (SELECT ',' + ContactEmail
          from  ContactEmail
          WHERE ContactServiceID = a.ContactServiceID
          FOR XML PATH (''))
          , 1, 1, '')  AS ContactEmailFinal
FROM ContactEmail as a
GROUP BY a.ContactServiceID)

--MAIN SCRIPT
select
iif(cmt.ParentContactServiceID = '' or cmt.ParentContactServiceID is NULL,'CA9999999',concat('CA',cmt.ParentContactServiceID)) as 'contact-companyId'
, concat('CA',cmt.ContactServiceID) as 'contact-externalId'
, iif(cdt.FirstName = '' or cdt.FirstName is NULL,'Firstname',cdt.FirstName) as 'contact-firstName'
, iif(cdt.LastName = '' or cdt.LastName is NULL,concat('LastName-',cdt.ContactServiceID),cdt.LastName) as 'contact-lastName'
, coalesce(cdt.MiddleName,'') as 'contact-middleName'
, coalesce(cmt.JobTitle,'') as 'contact-jobTitle'
, case when right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)) = 'Thomas' then 'thomasvandevyvere@catalize.be'
	when right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)) = 'katrien' then 'katriendebeil@catalize.be'
	when right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)) = 'helene' then 'helenevandeputte@catalize.be'
	else 'thomasvandevyvere@catalize.be' end as 'contact-owner'
, cef.ContactEmailFinal as 'contact-email'
, cp.ContactPhone as 'contact-phone'
, cfi.ContactFiles as 'contact-document'
, cpt.ContactPhoto as 'contact-photo'
, concat('Catalize Contact External ID: ',cmt.ContactServiceID,char(10)
	, coalesce('Contact title: ' + cdt.Prefix + char(10),'')
	, concat('Company name: ',iif(cmt.ParentContactServiceID = '' or cmt.ParentContactServiceID is NULL,'NO company information',cf.FullName),char(10))
	, coalesce('Source: ' + cdt.LeadSource + char(10),'')
	, concat('Created by: ',right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),char(10))
	, concat('Created on: ',convert(varchar(10),cmt.CreatedOn,120),char(10))
	, iif(convert(varchar(10),cdt.Birthday,120) = '' or cdt.Birthday is NULL,'',concat('Birthday: ', convert(varchar(10),cdt.Birthday,120), char(10)))
	, iif(cmt.WebAddress = '' or cmt.WebAddress is NULL,'',concat('Web Address: ', cmt.WebAddress, char(10)))
	, iif(cmt.BusinessAddress = '' or cmt.BusinessAddress is NULL,'',concat('Business Address: ', cmt.BusinessAddress, char(10)))
	, concat('Home address: ',coalesce(cdt.HomeAddressStreet, ''),coalesce(cdt.HomeAddressCity, ''),coalesce(cdt.HomeAddressState, ''),coalesce(cdt.HomeAddressZip, ''),coalesce(cdt.HomeAddressCountry, ''),char(10))
	, iif(uf.UserField2 = '' or uf.UserField2 is NULL,'',concat('Moedertaal: ', cast(uf.UserField2 as nvarchar(max)), char(10)))
	, iif(uf.UserField3 = '' or uf.UserField3 is NULL,'',concat('Ervaring: ', cast(uf.UserField3 as nvarchar(max)), char(10)))
	, iif(uf.UserField4 = '' or uf.UserField4 is NULL,'',concat('IT Kennis: ', cast(uf.UserField4 as nvarchar(max)), char(10)))
	, iif(uf.UserField1 = '' or uf.UserField1 is NULL,'',concat('Beschikbaar vanaf: ', cast(uf.UserField1 as datetime), char(10)))
	, iif(uf.UserField5 = '' or uf.UserField5 is NULL,'',concat('Algemene opmerkingen: ', cast(uf.UserField5 as nvarchar(max)), char(10)))
	, iif(cdt.ManagerName = '' or cdt.ManagerName is NULL,'',concat('Manager''s name: ', cdt.ManagerName, char(10)))
	, iif(cdt.AssistantName = '' or cdt.AssistantName is NULL,'',concat('Assistant''s name: ', cdt.AssistantName, char(10)))
	, iif(cast(cdt.ContactNotes as nvarchar(max)) = '' or cdt.ContactNotes is NULL,'',concat('Todstempel Toevoegen: ',cdt.ContactNotes))
	) as 'contact-note'
from ContactMainTable cmt
left join (select ContactServiceID, max(CategoryName) as CategoryName from ContactCategoriesTable
	where CategoryName in ('Non-finance')
	group by ContactServiceID) cct on cct.ContactServiceID = cmt.ContactServiceID 
	--> conditions to filter Contact/Candidate based on mapping | 1 contact may have multiple statuses
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID --> contact additional info
left join CompanyRef cf on cf.ParentContactServiceID = cmt.ParentContactServiceID --> Company full name for contact
left join UserFields uf on uf.ContactServiceID = cmt.ContactServiceID --> contact addition info from UserField
left join ContactPhone cp on cp.ContactServiceID = cmt.ContactServiceID --> contact combined phones
left join ContactFiles cfi on cfi.ContactID = cmt.ContactServiceID --> contact files
left join ContactEmailFinal cef on cef.ContactServiceID = cmt.ContactServiceID --> contact mails with duplication recognition
left join ContactPhoto cpt on cpt.ContactServiceID = cmt.ContactServiceID --> contact photos
where cmt.Type = 1 and cmt.IsDeletedLocally = 0 --> Type 1 for Contact | 2 for Company | 3 for Job
and cct.ContactServiceID is not NULL --> condition if contactId is available from Contact Category
and cct.CategoryName in ('Non-finance') --> conditional mapping for contact type

UNION ALL

select 'CA9999999','CA9999999','Default','Contact','','','','','','','','This is default contact from data import'
