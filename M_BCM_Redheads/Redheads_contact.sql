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

--COMPANY REFERENCE
, CompanyRef as (select distinct a.ParentContactServiceID, b.FullName from ContactMainTable a
left join ContactMainTable b on a.ParentContactServiceID = b.ContactServiceID
where a.ParentType = 2)

--CONTACT MAIL - WITHOUT DUPLICATION RECOGNITION || CURRENTLY NOT USED IN MAIN SCRIPT
, ContactEmail_WO_Dup as (select ContactServiceID
, stuff((coalesce(',' + AddressBookEmailAddress1, '') + coalesce(',' + AddressBookEmailAddress2, '') 
	+ (coalesce(',' + AddressBookEmailAddress3, ''))), 1, 1, '') as Email_WO_dup
from ContactDetailsTable
where AddressBookEmailAddress1 is not NULL or AddressBookEmailAddress2 is not NULL or AddressBookEmailAddress3 is not NULL)

--CONTACT PHONE
, ContactPhone as (select ContactServiceID
, stuff((coalesce(',' + WorkPhoneNum, '') + coalesce(',' + MobilePhoneNum, '') 
	+ coalesce(',' + HomePhoneNum, '') + coalesce(',' + OtherPhoneNum, '') + coalesce(',' + PhoneNum2, '') + coalesce(',' + PhoneNum3, '')
	), 1, 1, '' ) as ContactPhone
from ContactMainTable where Type = 1)

--CONTACT COMMENT
--, ContactComment as (select * 
--from ContactMainTable cmt
--left join UserFields uf on cmt.ContactServiceID = uf.ContactServiceID
--where cmt.Type = 1)

--MAIN SCRIPT
select
iif(cmt.ParentContactServiceID = '' or cmt.ParentContactServiceID is NULL,'RH9999999',concat('RH',cmt.ParentContactServiceID)) as 'contact-companyId'
, concat('RH',cmt.ContactServiceID) as 'contact-externalId'
, iif(cdt.FirstName = '' or cdt.FirstName is NULL,'Firstname',cdt.FirstName) as 'contact-firstName'
, iif(cdt.LastName = '' or cdt.LastName is NULL,concat('LastName-',cdt.ContactServiceID),cdt.LastName) as 'contact-lastName'
, coalesce(cdt.MiddleName,'') as 'contact-middleName'
, coalesce(cmt.JobTitle,'') as 'contact-jobTitle'
, iif(cdt.AssignedTo = '' or cdt.AssignedTo is NULL
	,concat(right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),'@redheads.co.za')
	,concat(right(cdt.AssignedTo,len(cdt.AssignedTo)-charindex('\',cdt.AssignedTo)),'@redheads.co.za')) as 'contact-owner'
, cef.ContactEmailFinal as 'contact-email'
, cp.ContactPhone as 'contact-phone'
, cfi.ContactFiles as 'contact-document'
, concat('BCM Contact ID: ',cmt.ContactServiceID,char(10)
	, coalesce('Contact title: ' + cdt.Prefix + char(10),'')
	, coalesce('Contact suffix: ' + cdt.Suffix + char(10),'')
	, concat('Company name: ',iif(cmt.ParentContactServiceID = '' or cmt.ParentContactServiceID is NULL,'NO company information',cf.FullName))
	, coalesce('Source: ' + cdt.LeadSource + char(10),'')
	, concat('Created by: ',right(cmt.CreatedBy,len(cmt.CreatedBy)-charindex('\',cmt.CreatedBy)),char(10))
	, iif(cmt.Department = '' or cmt.Department is NULL,'',concat('Department: ', cmt.Department, char(10)))
	, iif(cdt.ManagerName = '' or cdt.ManagerName is NULL,'',concat('Manager''s name: ', cdt.ManagerName, char(10)))
	, iif(cdt.AssistantName = '' or cdt.AssistantName is NULL,'',concat('Assistant''s name: ', cdt.AssistantName, char(10)))
	, iif(uf.UserField124 = '' or uf.UserField124 is NULL,'',concat('Qualifications: ', cast(uf.UserField124 as nvarchar(max)), char(10)))
	, iif(cmt.LeadScore = '' or cmt.LeadScore is NULL,'',concat('Lead score: ', cmt.LeadScore, char(10)))
	, iif(cdt.AreaOfInterest = '' or cdt.AreaOfInterest is NULL,'',concat('Area of interest: ', cdt.AreaOfInterest, char(10)))
	, iif(cmt.Rating = '' or cmt.Rating is NULL,'',concat('Contact rating: ', cmt.Rating, char(10)))
	, iif(uf.UserField43 = '' or uf.UserField43 is NULL,'',concat('Employment Status: ', cast(uf.UserField43 as nvarchar(max)), char(10)))
	, iif(uf.UserField44 is NULL,'',iif(cast(uf.UserField44 as bit) = 1,concat('Director of Own Company: YES', char(10)),concat('Director of Own Company: NO',char(10))))
	, iif(uf.UserField45 = '' or uf.UserField45 is NULL,'',concat('Nationality: ', cast(uf.UserField45 as nvarchar(max)), char(10)))
	, iif(uf.UserField58 = '' or uf.UserField58 is NULL,'',concat('Work Permit Type Expiry Date: ', cast(uf.UserField58 as nvarchar(max)), char(10)))
	, iif(uf.UserField46 = '' or uf.UserField46 is NULL,'',concat('EE Status: ', cast(uf.UserField46 as nvarchar(max)), char(10)))
	, iif(uf.UserField123 is NULL,'Disabled: n/a',iif(cast(uf.UserField44 as bit) = 1,concat('Disabled: YES',char(10)),concat('Disabled: NO',char(10))))
	, iif(cmt.BusinessAddress = '' or cmt.BusinessAddress is NULL,'',concat('Business Addresses: ', cmt.BusinessAddress, char(10)))
	, iif(uf.UserField32 = '' or uf.UserField32 is NULL,'',concat('Known As: ', cast(uf.UserField32 as nvarchar(max)), char(10)))
	, iif(uf.UserField41 = '' or uf.UserField41 is NULL,'',concat('Pref Language: ', cast(uf.UserField41 as nvarchar(max)), char(10)))
	, iif(cdt.PrefContactMethod = '' or cdt.PrefContactMethod is NULL,'',concat('Preferred Method: ', cdt.PrefContactMethod, char(10)))
	, iif(uf.UserField34 = '' or uf.UserField34 is NULL,'',concat('Employed Since: ', cast(uf.UserField34 as nvarchar(max)), char(10)))
	, iif(convert(varchar(10),cdt.Birthday,120) = '' or cdt.Birthday is NULL,'',concat('Birthday: ', convert(varchar(10),cdt.Birthday,120), char(10)))
	, iif(cdt.Spouse = '' or cdt.Spouse is NULL,'',concat('Spouse / Partner: ', cdt.Spouse, char(10)))
	, iif(cdt.Children = '' or cdt.Children is NULL,'',concat('Children: ', cdt.Children, char(10)))
	, iif(uf.UserField4 = '' or uf.UserField4 is NULL,'',concat('Trademanship 1: ', cast(uf.UserField4 as nvarchar(max)), char(10)))
	, iif(uf.UserField51 = '' or uf.UserField51 is NULL,'',concat('Company / Training Institution 1: ', cast(uf.UserField51 as nvarchar(max)), char(10)))
	, iif(convert(varchar(10),cast(uf.UserField52 as datetime)) = '' or uf.UserField52 is NULL,'',concat('Month / Year passed 1: ', convert(varchar(10),cast(uf.UserField52 as datetime),120), char(10)))
	, iif(uf.UserField5 = '' or uf.UserField5 is NULL,'',concat('Field of Study 1: ', cast(uf.UserField5 as nvarchar(max)), char(10)))
	, iif(uf.UserField50 = '' or uf.UserField50 is NULL,'',concat('Learning Institute 1: ', cast(uf.UserField50 as nvarchar(max)), char(10)))
	, iif(convert(varchar(10),cast(uf.UserField53 as datetime),120) = '' or uf.UserField53 is NULL,'',concat('Start Date Studies 1: ', convert(varchar(10),cast(uf.UserField53 as datetime),120), char(10)))
	, iif(convert(varchar(10),cast(uf.UserField54 as datetime),120) = '' or uf.UserField54 is NULL,'',concat('End Date Studies 1: ', convert(varchar(10),cast(uf.UserField54 as datetime),120), char(10)))
	, iif(uf.UserField6 = '' or uf.UserField6 is NULL,'',concat('Highiest Level 1: ', cast(uf.UserField6 as nvarchar(max)), char(10)))
	, iif(uf.UserField55 = '' or uf.UserField55 is NULL,'',concat('NQF Level 1: ', cast(uf.UserField55 as nvarchar(max)), char(10)))
	, iif(uf.UserField39 = '' or uf.UserField39 is NULL,'',concat('Memberships: ', cast(uf.UserField39 as nvarchar(max)), char(10)))
	, iif(uf.UserField76 = '' or uf.UserField76 is NULL,'',concat('Certification 1: ', cast(uf.UserField76 as nvarchar(max)), char(10)))
	, iif(uf.UserField14 = '' or uf.UserField14 is NULL,'',concat('Standard Applications: ', cast(uf.UserField14 as nvarchar(max)), char(10)))
	, iif(uf.UserField15 = '' or uf.UserField15 is NULL,'',concat('Language 1: ', cast(uf.UserField15 as nvarchar(max)), char(10)))
	, iif(uf.UserField75 = '' or uf.UserField75  is NULL,'',concat('Level 1: ', cast(uf.UserField75 as nvarchar(max)), char(10)))
	, iif(uf.UserField74 = '' or uf.UserField74 is NULL,'',concat('Language 2: ', cast(uf.UserField74 as nvarchar(max)), char(10)))
	, iif(uf.UserField73 = '' or uf.UserField73 is NULL,'',concat('Level 2: ', cast(uf.UserField73 as nvarchar(max)), char(10)))
	, iif(uf.UserField28 = '' or uf.UserField28 is NULL,'',concat('Employer history (Tab 4): ', cast(uf.UserField28 as nvarchar(max)), char(10)))
	, iif(cast(cdt.ContactNotes as nvarchar(max)) = '' or cdt.ContactNotes is NULL,'',concat('Contact Notes: ',cdt.ContactNotes))) as 'contact-note'
, concat(iif(cbn.ContactBusinessNote = '' or cbn.ContactBusinessNote is NULL,'',concat('***Business notes: ',char(10),cbn.ContactBusinessNote,'<hr>'))
		, iif(ccn.ContactCommunicationNote = '' or ccn.ContactCommunicationNote is NULL,'',concat('***Communication notes: '
		, char(10),ccn.ContactCommunicationNote))) as 'contact-comment'
from ContactMainTable cmt
left join (select ContactServiceID, max(CategoryName) as CategoryName from ContactCategoriesTable
	where CategoryName in ('Competitor', 'Personal', 'Inactive Client', 'Client', 'Source of Candidates', 'Restraint of Trade', 'Supplier','Reference')
	group by ContactServiceID) cct on cct.ContactServiceID = cmt.ContactServiceID 
	--> conditions to filter Contact/Candidate based on mapping | 1 contact may have multiple statuses
left join ContactDetailsTable cdt on cdt.ContactServiceID = cmt.ContactServiceID --> contact additional info
left join CompanyRef cf on cf.ParentContactServiceID = cmt.ParentContactServiceID --> Company full name for contact
left join UserFields uf on uf.ContactServiceID = cmt.ContactServiceID --> contact addition info from UserField
left join ContactPhone cp on cp.ContactServiceID = cmt.ContactServiceID --> contact combined phones
left join ContactFiles cfi on cfi.ContactID = cmt.ContactServiceID --> contact files
left join ContactEmailFinal cef on cef.ContactID = cmt.ContactServiceID --> contact mails with duplication recognition
left join ContactBusinessNote cbn on cbn.ContactID = cmt.ContactServiceID --> contact business notes | ActivityType = 14
left join ContactCommunicationNote ccn on ccn.ContactID = cmt.ContactServiceID --> contact communication notes | ActivityType = 15, 3
where cmt.Type = 1 and cmt.IsDeletedLocally = 0 --> Type 1 for Contact | 2 for Company | 3 for Job
and cct.ContactServiceID is not NULL --> condition if contactId is available from Contact Category
and cct.CategoryName in ('Competitor', 'Personal', 'Inactive Client', 'Client', 'Source of Candidates', 'Restraint of Trade', 'Supplier','Reference') --> conditional mapping for contact type

UNION ALL

select 'RH9999999','RH9999999','Default','Contact','','','','','','','This is default contact from data import',''