----CANDIDATE SUMMARY
create table CandSummary
(ContactID int PRIMARY KEY,
ProcessedText nvarchar(max)
)
go

insert into CandSummary SELECT 
     ContactID,
     STUFF(
         (SELECT char(10) + ProcessedText
          from  CandidateAttachment
          WHERE ContactID = a.ContactID
		  and ProcessedText is not NULL
		  order by AttachmentID desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandidateAttachment as a
where a.ProcessedText is not NULL
GROUP BY a.ContactID

select * from CandSummary

-----CANDIDATE ATTACHMENT
create table CandAttachmentFinal
(ContactID int PRIMARY KEY,
FileName2 nvarchar(max)
)
go


with CandAttachmentEdit as (SELECT ContactID, 
	case 
when FileType = '' or FileType is NULL then concat(AttachmentID,'.original',right(FileName,charindex('.',reverse(FileName))))
when FileType = 'application/octet-stream' then concat(AttachmentID,'.original',right(FileName,charindex('.',reverse(FileName))))
when FileType = 'binary/octet-stream' then concat(AttachmentID,'.original',right(FileName,charindex('.',reverse(FileName))))
when FileType = 'application/msword' then concat(AttachmentID,'.original','.doc')
when FileType = 'application/pdf' then concat(AttachmentID,'.original','.pdf')
when FileType = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet' then concat(AttachmentID,'.original','.xlsx')
when FileType = 'application/vnd.openxmlformats-officedocument.wordprocessingml.document' then concat(AttachmentID,'.original','.docx')
when FileType = 'application/vnd.openxmlformats-officedocument.word' then concat(AttachmentID,'.original','.docx')
when FileType = 'text/plain' then concat(AttachmentID,'.original','.doc')
else '' end as FileName
from CandidateAttachment
where FileName like '%pdf' or FileName like '%doc%' or FileName like '%xls%' or FileName like '%rtf' or FileName like '%txt')

insert into CandAttachmentFinal SELECT
     ContactID,
     STUFF(
         (SELECT ',' + FileName
          from  CandAttachmentEdit
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandAttachmentEdit as a
GROUP BY a.ContactID

select * from CandAttachmentFinal


-----CANDIDATE COMBINED NOTE (not yet for V2)
create table CandNoteFinal
(ContactID int PRIMARY KEY,
CandNote nvarchar(max)
)
go

with 

AllNote as (select N.NoteID, N.Type, N.Source, N.Text, N.CreatedByUserID, N.DateCreated, U.Email, U.DisplayName
from Note N
left join [User] U on U.UserID = N.CreatedByUserID)

, CandNote as (select CN.ContactID, CN.NoteID, N.Type, N.Source, N.Text
	, convert(varchar,N.DateCreated,120) as DateCreated, N.Email, N.DisplayName
	from CandidateNote CN
	left join AllNote N on CN.NoteID = N.NoteID)

insert into CandNoteFinal SELECT
     ContactID,
     STUFF(
         (SELECT char(10) + 'Created Date: ' + convert(varchar,DateCreated,120) + ' || ' 
		 + 'Created by: ' + Email + ' - ' + DisplayName + ' || ' + 'Type: ' + Type + ' || ' + Text
          from  CandNote
          WHERE ContactID = a.ContactID
		  order by DateCreated desc
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandNote as a
GROUP BY a.ContactID

select * from CandNoteFinal

select distinct ContactID from CandidateNote


-----CANDIDATE CUSTOM FIELDS (not yet v2)
create table CombinedCustFieldValue
(ContactID int PRIMARY KEY,
CustFieldValue nvarchar(max)
)
go

with CustFieldValue as (select ContactID, FieldID,
     STUFF(
         (SELECT ', ' + ValueText
          from  CandidateCustomField
          WHERE ContactID = a.ContactID
          FOR XML PATH (''))
          , 1, 1, '')  AS URLList
FROM CandidateCustomField as a
GROUP BY a.ContactID, a.FieldID)

, CusField as (select CFV.ContactID, CFV.FieldID, CF.Name as FieldName, CFV.URLList as CustomFieldValue 
from CustFieldValue CFV
left join CustomField CF on CFV.FieldID = CF.FieldID
where CF.EntityType = 'Contact')

--select top 10 * from CusField
--order by CompanyID -> Different count due to the custom field was deleted, ID = 1

insert into CombinedCustFieldValue select ContactID,
     STUFF(
         (SELECT char(10) + FieldName + ': ' + CustomFieldValue
          from  CusField
          WHERE ContactID = a.ContactID
          FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)')
          , 1, 1, '')  AS URLList
FROM CusField as a
GROUP BY a.ContactID