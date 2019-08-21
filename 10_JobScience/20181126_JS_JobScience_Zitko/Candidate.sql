with filedup as (select Id,iif(Attachment.Name is null or Attachment.Name = '','',Attachment.Name) as filename,ParentId from Attachment)

,filename as (select *,ROW_NUMBER() over ( partition by filename order by filename) as 'row_num' from filedup)

,filename2 as (select id, iif(row_num = 1,filename,concat(row_num,'-',filename)) as filename,ParentId from filename)
,docname as
(SELECT ParentId as 'ParentId',
    STUFF((SELECT DISTINCT ', ' + filename
           FROM filename2 a 
           WHERE a.ParentId = b.ParentId
          FOR XML PATH('')), 1, 2, '') as 'FileName'
FROM filename2 b
GROUP BY ParentId)

select RecordType.Name,
contact.Id as'candidate-externalId',
contact.AccountId as 'candidate-companyId',
[User].email as 'candidate-owners',
iif(Salutation is null or Salutation = '','',Salutation) as 'candidate-title',
iif(contact.FirstName is null or contact.FirstName = '','No First Name',contact.FirstName) as 'candidate-firstName',
iif(contact.LastName is null or contact.LastName = '','No Last Name',contact.LastName) as 'candidate-Lastname',
iif(contact.Title is null or contact.Title='','',contact.Title) as 'contact-jobTitle',
concat(nullif(concat(MailingStreet,', '),', '),
nullif(concat(MailingCity,', '),', '), 
nullif(concat(MailingState,', '),', '), 
nullif(concat(MailingCountry,', '),', '), 
nullif(MailingPostalCode,'')) as 'candidate-address',
iif(MailingCountry is null or MailingCountry ='','',MailingCountry) as 'candidate-Country',
iif(MailingPostalCode is null or MailingPostalCode ='','',MailingPostalCode) as 'candidate-zipCode',
iif(MailingState is null or MailingState ='','',MailingState) as 'candidate-State',
iif(MailingCity is null or MailingCity ='','',MailingCity) as 'candidate-city',
iif(contact.MobilePhone is null or contact.MobilePhone = '','',contact.MobilePhone) as 'candidate-phone',
iif(contact.Email is null or contact.Email = '','',contact.Email) as 'candidate-email',
docname.filename as 'candidate-resume'
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] on [User].Id = Contact.OwnerId
left join docname on contact.Id = docname.ParentId
where name='Candidate'

