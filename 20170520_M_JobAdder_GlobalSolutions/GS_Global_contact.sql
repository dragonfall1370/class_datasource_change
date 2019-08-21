---Combine all notes for Contact
with AllNote as (select N.NoteID, N.Type, N.Source, N.Text, N.CreatedByUserID, N.DateCreated, U.Email, U.DisplayName
from Note N
left join [User] U on U.UserID = N.CreatedByUserID)

, ConNote as (select CN.ContactID, CN.NoteID, N.Type, N.Source, N.Text
	, convert(varchar,N.DateCreated,120) as DateCreated, N.Email, N.DisplayName
	from ContactNote CN
	left join AllNote N on CN.NoteID = N.NoteID)

, ConNoteFinal as (SELECT
     ContactID,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar,DateCreated,120) + ' || ' 
		 + 'Created by: ' + Email + ' - ' + DisplayName + ' || ' + 'Type: ' + Type + ' || ' + Text
          from  ConNote
          WHERE ContactID = a.ContactID
		  order by DateCreated desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM ConNote as a
GROUP BY a.ContactID)

----Combine note attachment for each category
, NoteAttach as (select NoteID, AttachmentID, concat(AttachmentID,'.original',
case when FileType = '' then ''
when FileType = 'application/msword' then '.doc'
when FileType = 'application/pdf' then '.pdf'
when FileType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' then '.xlsx'
when FileType = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' then '.docx'
when FileType = 'application/octet-stream' then right(FileName,charindex('.',reverse(FileName)))
when FileType = 'image/jpeg' then '.jpg'
when FileType = 'image/png' then '.png'
else '' end) as FileName
from NoteAttachment
where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf' or FileName like '%jpg' or FileName like '%png')

, ConNoteAttach as (select CN.ContactID, CN.NoteID, NA.AttachmentID, NA.FileName
	from ContactNote CN
	left join NoteAttach NA on CN.NoteID = NA.NoteID
	where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf')

, ConNoteAttaches as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  ConNoteAttach
          WHERE ContactID = a.ContactID
		  order by AttachmentID desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM ConNoteAttach as a
GROUP BY a.ContactID)

---Combine all documents >> No documents for contact

---Contact skill to be combined from Category and Subcategory
, ContCg_SCg as (select CS.ContactID, CS.CategoryID, Cg.Name as CategoryName, CS.SubCategoryID, SCg.Name as SubCategoryName
from ContactSkill CS 
left join Category Cg on CS.CategoryID = Cg.CategoryID
left join SubCategory SCg on CS.SubCategoryID = SCg.SubCategoryID)

, ContCombinedSubCat as (SELECT
     ContactID, CategoryID,
     STUFF(
         (SELECT ', ' + SubCategoryName
          from  ContCg_SCg
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM ContCg_SCg as a
GROUP BY a.ContactID, a.CategoryID)

, ContCategory as (select CCg.ContactID, CCg.CategoryID, Cg.Name as CategoryName, CCg.URLList as SubCategories
from ContCombinedSubCat CCg
left join Category Cg on CCg.CategoryID = Cg.CategoryID)

, ContCombinedCategory as (SELECT
     ContactID,
     STUFF(
         (SELECT char(10) + CategoryName + ': ' + SubCategories
          from  ContCategory
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList,
	STUFF(
         (SELECT ', ' + cast(CategoryID as varchar(max))
          from  ContCategory
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList2
FROM ContCategory as a
GROUP BY a.ContactID)

---Contact custom field (Global Solutions)
, CustFieldValue as (select ContactID, FieldID,
     STUFF(
         (SELECT ', ' + ValueText
          from  ContactCustomField
          WHERE ContactID = a.ContactID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM ContactCustomField as a
GROUP BY a.ContactID, a.FieldID)

, CusField as (select CFV.ContactID, CFV.FieldID, CF.Name as FieldName, CFV.URLList as CustomFieldValue 
from CustFieldValue CFV
left join CustomField CF on CFV.FieldID = CF.FieldID
where CF.EntityType = 'Contact')

--select top 10 * from CusField
--order by CompanyID -> Different count due to the custom field was deleted, ID = 1

, CombinedCustFieldValue as (select ContactID,
     STUFF(
         (SELECT char(10) + FieldName + ' - ' + CustomFieldValue
          from  CusField
          WHERE ContactID = a.ContactID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM CusField as a
GROUP BY a.ContactID)

---Combined phone from Company Address
, CombinedPhone as (SELECT ContactID, concat(coalesce(Phone + ',',''),coalesce(Mobile,'')) as ContactPhone from Contact)

---Combined email from Contact and ContactOtherEmail
, ConOtherEmail as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + Email
          from  ContactOtherEmail
          WHERE ContactID = a.ContactID
		  and Email like '%_@_%.__%'
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM ContactOtherEmail as a
GROUP BY a.ContactID)

, ConEmail as (select ContactID, URLList from ConOtherEmail
UNION 
select ContactID, Email from Contact where Email like '%_@_%.__%')

, ConCombinedEmail as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + URLList
          from  ConEmail
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM ConEmail as a
GROUP BY a.ContactID)

---Main script
select top 10 coalesce('GS' + convert(varchar,C.CompanyID),'GS9999999') as 'contact-companyId' --If company is NULL, Company Default will be GS9999999
, C.CompanyID
, concat('GS',C.ContactID) as 'contact-externalId'
, coalesce(C.FirstName,'Firstname') as 'contact-firstName'
, coalesce(C.LastName,concat('Lastname-',C.ContactID)) as 'contact-lastName'
, CCE.URLList as 'contact-email'
, iif(right(CP.ContactPhone,1)=',',left(CP.ContactPhone,len(CP.ContactPhone)-1),CP.ContactPhone) as 'contact-phone'
, CP.ContactPhone as '(ContactPhone)'
, C.Position as 'contact-jobTitle'
, C.OwnerUserID
, iif(U.Email like '%deleted%','',U.Email) as 'contact-owners'
, C.LinkedInUrl as 'contact-linkedIn',C.UpdatedByUserID,C.UpdatedBy
, left(concat('Contact External ID: ',C.ContactID,char(10)
	, iif(C.DateCreated = '' or C.DateCreated is NULL,'',concat('Date created: ',convert(varchar,C.DateCreated,120),char(10)))
	, iif(C.CreatedByUserID = '' or C.CreatedByUserID is NULL,concat('Created by: ',C.CreatedBy,char(10)),concat('Created by: ',U.DisplayName,char(10)))
	, iif(C.DateUpdated = '' or C.DateUpdated is NULL,'',concat('Last updated: ',convert(varchar,C.DateUpdated,120),char(10)))
	, iif(C.UpdatedByUserID = '' or C.UpdatedByUserID is NULL,concat('Updated by: ',C.UpdatedBy,char(10)),concat('Updated by: ',U.DisplayName,char(10)))
	, iif(C.FullName = '' or C.FullName is NULL,'',concat('Contact full name: ',C.FullName,char(10)))
	, iif(CS.Name = '' or CS.Name is NULL,'',concat('Status: ',CS.Name,char(10)))
	, iif(C.Summary = '' or C.Summary is NULL,'',concat('Summary: ',C.Summary,char(10)))
	, iif(CCCg.URLList = '' or CCCg.URLList is NULL,'',concat('Contact skills: ',CCCg.URLList,char(10)))
	, iif(C.OwnerUserID = '' or C.OwnerUserID is NULL,'',concat('Contact owner: ',C.OwnerUserID,'-',U.Email))
	, iif(CCFV.URLList = '' or CCFV.URLList is NULL,'',concat('Contact Custom Fields: ',replace(replace(replace(replace(CCFV.URLList,'&lt;','<'),'&gt;','>'),'&amp;','&'),'#x0D;',''))))
	,32000) as 'contact-note'
, left(replace(replace(replace(CNF.URLList,'&lt;','<'),'&gt;','>'),'&amp;','&'),32000) as 'contact-comment'
, REPLACE(CNA.URLList,'&amp;','&') as 'contact-document'
from Contact C
left join ConCombinedEmail CCE on CCE.ContactID = C.ContactID
left join CombinedPhone CP on CP.ContactID = C.ContactID
left join [User] U on U.UserID = C.OwnerUserID --contact ownerUserID
left join ContactStatus CS on CS.StatusID = C.StatusID
left join ConNoteFinal CNF on CNF.ContactID = C.ContactID
left join ConNoteAttaches CNA on CNA.ContactID = C.ContactID
left join ContCombinedCategory CCCg on CCCg.ContactID = C.ContactID
left join CombinedCustFieldValue CCFV on CCFV.ContactID = C.ContactID
where C.IsCandidateOnly = 0