with filedup as (select Id,iif(Attachment.Name is null or Attachment.Name = '','',Attachment.Name) as filename,ParentId from Attachment)

,filename as (select *,ROW_NUMBER() over ( partition by filename order by filename) as 'row_num' from filedup)

, filename2 as (select id, iif(row_num = 1,REPLACE(filename,',','-'),concat(row_num,'-',REPLACE(filename,',','-'))) as filename,ParentId from filename)

, emaildup as (select contact.Id, Email, row_number() over (partition by email order by email) as email_num
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
where RecordType.name='Contact')

, contactemail as (select id, iif(email_num = 1,email,concat(email_num,'-',email)) as email from emaildup where email <> '')

,main as (select RecordType.Name,
trim(contact.Id) as 'contact-externalId',
trim(Contact.AccountId) as 'contact-companyId',
b.Email as 'contact-owners',
Contact.FirstName as 'contact-firstName',
Contact.LastName as 'contact-lastName',
iif(Contact.Title is null or Contact.Title = '','',Contact.Title) as 'contact-jobTitle',
iif(Contact.MobilePhone is null or Contact.MobilePhone = '','',iif(contact.phone like '[0-9]%/%','',contact.phone)) as 'contact-phone',
iif(d.Email is null or Contact.Email = '','',d.Email) as 'contact-email',
iif(Contact.Skype_ID__c is null or Contact.Skype_ID__c ='','',Contact.Skype_ID__c) as 'contact-skype',
concat('External ID: ',contact.Id, (char(13)+char(10)),iif(
contact.description is null,'',nullif(concat('Description: ', (char(13)+char(10)), contact.Description),concat('Description: ', (char(13)+char(10))))))as 'contact-note',
--c.filename as 'contact-document', (all null)
ROW_NUMBER() over (partition by contact.ID order by contact.ID) as 'row_num'

from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] b on b.Id = Contact.OwnerId
left join filename2 c on contact.id = c.Id
left join contactemail d on contact.id = d.id
where RecordType.name='Contact')



select * from main where row_num = 1


