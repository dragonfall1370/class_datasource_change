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

---Combined phone from Company Address
, CombinedPhone as (SELECT ContactID, concat(coalesce(Phone + ',',''),coalesce(Mobile,'')) as ContactPhone from Contact)

---Combined email from Contact and ContactOtherEmail
, ConOtherEmail as (SELECT
     ContactID,
     STUFF(
         (SELECT ',' + Email
          from  ContactOtherEmail
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM ContactOtherEmail as a
GROUP BY a.ContactID)

, ConEmail as (select ContactID, URLList from ConOtherEmail
UNION select ContactID, Email from Contact)

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
select coalesce('IH' + convert(varchar,C.CompanyID),'IH9999999') as 'contact-companyId' --If company is NULL, Company Default will be IH9999999
, CompanyID
, concat('IH',C.ContactID) as 'contact-externalId'
, coalesce(C.FirstName,'Firstname') as 'contact-firstName'
, coalesce(C.LastName,concat('Lastname-',C.ContactID)) as 'contact-lastName'
, CCE.URLList as 'contact-email'
, iif(right(CP.ContactPhone,1)=',',left(CP.ContactPhone,len(CP.ContactPhone)-1),CP.ContactPhone) as 'contact-phone'
, CP.ContactPhone
, C.Position as 'contact-jobTitle'
, C.OwnerUserID
, U.Email as 'contact-owners'
, C.LinkedInUrl as 'contact-linkedIn'
, concat('Contact External ID: ',C.ContactID,char(10)
	, iif(C.DateUpdated = '' or C.DateUpdated is NULL,'',concat('Last Updated: ',convert(varchar,C.DateUpdated,120),char(10)))
	, iif(C.FullName = '' or C.FullName is NULL,'',concat('Contact full name: ',C.FullName,char(10)))
	, iif(CS.Name = '' or CS.Name is NULL,'',concat('Status: ',CS.Name,char(10)))
	, iif(C.Summary = '' or C.Summary is NULL,'',concat('Summary: ',C.Summary,char(10)))
	, iif(C.OwnerUserID = '' or C.OwnerUserID is NULL,'',concat('Contact owner: ',C.OwnerUserID,'-',U.Email))
	) as 'contact-note'
, left(replace(replace(replace(CNF.URLList,'&lt;','<'),'&gt;','>'),'&amp;','&'),32000) as 'contact-comment'
, REPLACE(CNA.URLList,'&amp;','&') as 'contact-document'
from Contact C
left join ConCombinedEmail CCE on CCE.ContactID = C.ContactID
left join CombinedPhone CP on CP.ContactID = C.ContactID
left join [User] U on U.UserID = C.OwnerUserID --contact ownerUserID
left join ContactStatus CS on CS.StatusID = C.StatusID
left join ConNoteFinal CNF on CNF.ContactID = C.ContactID
left join ConNoteAttaches CNA on CNA.ContactID = C.ContactID
where C.IsCandidateOnly = 0