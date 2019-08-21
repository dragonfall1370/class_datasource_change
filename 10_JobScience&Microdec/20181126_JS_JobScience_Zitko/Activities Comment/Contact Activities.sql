with task2 as (select a.id, Subject, replace(a.Description,'CC:',concat((char(13)+char(10)),'CC:')) as 'Description', WhoId, LastModifiedDate from Task a)

,task3 as (select id, Subject, replace(Description,'Attachment:',concat((char(13)+char(10)),'Attachment:')) as 'Description', WhoId, LastModifiedDate from task2)

select a.id as external_id,
concat('Subject: ',
b.Subject, (char(13)+char(10)), 'Note:', (char(13)+char(10)),
iif(b.description <> '',dbo.udf_StripHTML(replace(b.Description,'Subject:',concat((char(13)+char(10)),'Subject:',(char(13)+char(10))))),'')) as 'Content' ,

b.LastModifiedDate as insert_timestamp,
'comment' as 'category', 
'contact' as 'type', 
-10 as 'user_account_id'
from Contact a left join task3 b on a.Id = b.WhoId
left join RecordType on a.RecordTypeId+'AAQ' = RecordType.id
where Recordtype.Name = 'Contact' and WhoId is not null
Go
--------------------


with task2 as (select a.id, Subject, replace(a.Description,'CC:',concat((char(13)+char(10)),'CC:')) as 'Description', WhoId, LastModifiedDate from Event a)

,task3 as (select id, Subject, replace(Description,'Attachment:',concat((char(13)+char(10)),'Attachment:')) as 'Description', WhoId, LastModifiedDate from task2)

select a.id as external_id,
concat('Subject: ',
b.Subject, (char(13)+char(10)), 'Note:', (char(13)+char(10)),
iif(b.description <> '',dbo.udf_StripHTML(replace(b.Description,'Subject:',concat((char(13)+char(10)),'Subject:',(char(13)+char(10))))),'')) as 'Content' ,

b.LastModifiedDate as insert_timestamp,
'comment' as 'category', 
'contact' as 'type', 
-10 as 'user_account_id'
from Contact a left join task3 b on a.Id = b.WhoId
left join RecordType on a.RecordTypeId+'AAQ' = RecordType.id
where Recordtype.Name = 'Contact' and WhoId is not null
Go

SELECT a.Body as content, 
b.id,
a.CreatedDate as insert_timestamp,
'comment' as 'category', 
'contact' as 'type', 
-10 as 'user_account_id'
  FROM [Zitko].[dbo].[Note] a
  left join Contact b on a.ParentId = b.Id where b.id is not null




