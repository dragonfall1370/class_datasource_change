with
--DOCUMENT / ACTIVITY ATTACHMENTS
 ContactAttachment as (select distinct ac.ContactID, replace(replace(act.Subject,'.txt','.doc'),',','') as FileName, max(act.ModifiedOn) as ModifiedOn
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

--COMPANY REFERENCE
, CompanyRef as (select distinct a.ParentContactServiceID, b.FullName from ContactMainTable a
left join ContactMainTable b on a.ParentContactServiceID = b.ContactServiceID
where a.ParentType = 2)

--CANDIDATE DUPLICATE MAIL RECOGNITION
, dup as (SELECT cmt.ContactServiceID, cdt.AddressBookEmailAddress1, ROW_NUMBER() OVER(PARTITION BY cdt.AddressBookEmailAddress1 ORDER BY cmt.ContactServiceID ASC) AS rn 
FROM ContactMainTable cmt
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID
where cmt.Type = 1 and cmt.IsDeletedLocally = 0 and cdt.AddressBookEmailAddress1 is not NULL)

--MAIN SCRIPT
select
concat('CA',cmt.ContactServiceID) as 'candidate-externalId'
, iif(cdt.FirstName = '' or cdt.FirstName is NULL,'Firstname',cdt.FirstName) as 'candidate-firstName'
, iif(cdt.LastName = '' or cdt.LastName is NULL,concat('LastName-',cdt.ContactServiceID),cdt.LastName) as 'candidate-lastName'
, coalesce(cdt.MiddleName,'') as 'candidate-middleName'
, 'thomasvandevyvere@catalize.be' as 'contact-owner'
, cdt.AddressBookEmailAddress1
, coalesce(cdt.AddressBookEmailAddress1,concat('candidate-',cmt.ContactServiceID,'@noemail.com')) as 'candidate-email-withduplication'
, iif(cmt.ContactServiceID in (select ContactServiceID from dup where dup.rn > 1)
	, iif(dup.AddressBookEmailAddress1 = '' or dup.AddressBookEmailAddress1 is NULL,concat('candidate-',cmt.ContactServiceID,'@noemail.com'),concat(dup.rn,'_',dup.AddressBookEmailAddress1))
	, iif(cdt.AddressBookEmailAddress1 = '' or cdt.AddressBookEmailAddress1 is null,concat('candidate-',cmt.ContactServiceID,'@noemail.com'),cdt.AddressBookEmailAddress1)) as 'candidate-email' --> check duplicated email
, cdt.AddressBookEmailAddress2 as 'candidate-workEmail'
, cmt.MobilePhoneNum as 'candidate-phone'
, cmt.WorkPhoneNum as 'candidate-workPhone'
, stuff((coalesce(',' + cmt.HomePhoneNum, '') + coalesce(',' + cmt.PhoneNum2, '') 
	+ coalesce(',' + cmt.PhoneNum3, '') + coalesce(',' + cmt.OtherPhoneNum, '')), 1, 1, '') as 'candidate-homePhone'
, stuff((coalesce(',' + cmt.BusinessAddress, '') + coalesce(', ' + cdt.WorkAddressCountry,'')), 1, 1, '') as 'candidate-address'
, cmt.BusinessAddress as '(OriginalAddress)'
, cdt.WorkAddressCity as 'candidate-city'
, cdt.WorkAddressZip as 'candidate-zipCode'
, cdt.WorkAddressState as 'candidate-State'
, case when cdt.WorkAddressCountry = 'Duitsland' then 'DE'
	when cdt.WorkAddressCountry = 'Andorra' then 'AD'
	when cdt.WorkAddressCountry = 'Griekenland' then 'GR'
	when cdt.WorkAddressCountry = 'Groot-Brittannië' then 'GB'
	when cdt.WorkAddressCountry = 'Turkije' then 'TR'
	when cdt.WorkAddressCountry = 'België' then 'BE'
	when cdt.WorkAddressCountry = 'Verenigd Koningrijk' then 'GB'
	when cdt.WorkAddressCountry = 'Ierland' then 'IE'
	when cdt.WorkAddressCountry = 'China' then 'CN'
	when cdt.WorkAddressCountry = 'Italië' then 'IT'
	when cdt.WorkAddressCountry = 'UK' then 'GB'
	when cdt.WorkAddressCountry = 'Verenigd Koninkrijk' then 'GB'
	when cdt.WorkAddressCountry = 'Frankrijk' then 'FR'
	when cdt.WorkAddressCountry = 'Singapore' then 'SG'
	when cdt.WorkAddressCountry = 'Nederland' then 'NL'
	when cdt.WorkAddressCountry = 'Zwitserland' then 'CH'
	else '' end as 'candidate-Country'
, convert(varchar(10),cdt.Birthday,120) as 'candidate-dob'
, coalesce(cmt.JobTitle,'') as 'candidate-jobTitle1'
, cfi.ContactFiles as 'candidate-resume'
, cpt.ContactPhoto as 'candidate-photo'
--NOTES
, concat(concat('Catalize Candidate External ID: ',cmt.ContactServiceID,char(10))
	, coalesce('Candidate title: ' + cdt.Prefix + char(10),'')
	, coalesce('Candidate suffix: ' + cdt.Suffix + char(10),'')
	, concat('Category Name: ',cct.CategoryName,char(10))
	, coalesce('Source: ' + cdt.LeadSource + char(10),'')
	, concat('Created by: ',right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),char(10))
	, iif(cdt.ManagerName = '' or cdt.ManagerName is NULL,'',concat('Manager''s name: ', cdt.ManagerName, char(10)))
	, iif(cdt.AssistantName = '' or cdt.AssistantName is NULL,'',concat('Assistant''s name: ', cdt.AssistantName, char(10)))
	, concat('Home address: ',coalesce(cdt.HomeAddressStreet, ''),coalesce(cdt.HomeAddressCity, ''), coalesce(cdt.HomeAddressState, ''),coalesce(cdt.HomeAddressZip, ''), coalesce(cdt.HomeAddressCountry, ''),char(10))
	, concat('Other address: ',coalesce(cdt.OtherAddressStreet, ''),coalesce(cdt.OtherAddressCity, ''), coalesce(cdt.OtherAddressState, ''),coalesce(cdt.OtherAddressZip, ''), coalesce(cdt.OtherAddressCountry, ''),char(10))
	, iif(uf.UserField2 = '' or uf.UserField2 is NULL,'',concat('Moedertaal: ', cast(uf.UserField2 as nvarchar(max)), char(10)))
	, iif(uf.UserField3 = '' or uf.UserField3 is NULL,'',concat('Ervaring: ', cast(uf.UserField3 as nvarchar(max)), char(10)))
	, iif(uf.UserField4 = '' or uf.UserField4 is NULL,'',concat('IT Kennis: ', cast(uf.UserField4 as nvarchar(max)), char(10)))
	, iif(uf.UserField1 = '' or uf.UserField1 is NULL,'',concat('Beschikbaar vanaf: ', cast(uf.UserField1 as datetime), char(10)))
	, iif(uf.UserField5 = '' or uf.UserField5 is NULL,'',concat('Algemene opmerkingen: ', cast(uf.UserField5 as nvarchar(max)), char(10)))
--	, iif(cast(cdt.ContactNotes as nvarchar(max)) = '' or cdt.ContactNotes is NULL,'',concat('Candidate Notes: ',cdt.ContactNotes))
	) as 'candidate-note'
from ContactMainTable cmt
left join (select ContactServiceID, max(CategoryName) as CategoryName from ContactCategoriesTable
	where CategoryName in ('Non-finance')
	group by ContactServiceID) cct on cct.ContactServiceID = cmt.ContactServiceID -- conditions to filter Contact/Candidate based on mapping | 1 contact may have multiple statuses
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID --> candidate additional info
left join ContactFiles cfi on cfi.ContactID = cmt.ContactServiceID --> candidate files
left join CompanyRef cf on cf.ParentContactServiceID = cmt.ParentContactServiceID --> Company full name for candidate
left join dup on dup.ContactServiceID = cmt.ContactServiceID
left join UserFields uf on uf.ContactServiceID = cmt.ContactServiceID
left join ContactPhoto cpt on cpt.ContactServiceID = cmt.ContactServiceID --> candidate photos
where cmt.Type = 1 and cmt.IsDeletedLocally = 0
and cmt.ContactServiceID not in (select ContactServiceID
	from ContactCategoriesTable
	where CategoryName in ('Non-finance')
	group by ContactServiceID) --> conditional mapping for contact/candidate type | to exclude contacts