--select * from ts2__Job__c where ts2__Contact__c = ''
--update ts2__Job__c set ts2__Contact__c = '0' where ts2__Contact__c = ''
--go
--update ts2__Job__c set ts2__Account__c = '0' where ts2__Account__c = ''
--go

----select recordtypeid from ts2__Job__c
--update ts2__Job__c set recordtypeid = '01220000000hzkNAAQ' where RecordTypeId = '01220000000hzkN'
--go
--update ts2__Job__c set RecordTypeId = '01220000000hzjZAAQ' where RecordTypeId = '01220000000hzjZ'
--go
--update ts2__Job__c set RecordTypeId = '01220000000hzjaAAA' where RecordTypeId = '01220000000hzja'
--go
with tempcontact as (select 
contact.Id as 'contact_id',
Contact.AccountId as 'company_id',
b.Email as 'contact-owners',
Contact.FirstName as 'contact-firstName',
Contact.LastName as 'contact-lastName',
row_number() over (partition by contact.accountid order by contact.accountid) as 'row_num'
from contact left join RecordType on contact.RecordTypeId+'AAQ' = RecordType.id
left join [User] b on b.Id = Contact.OwnerId)
, tempcontact2 as (select * from tempcontact where row_num = 1)
,jobname as (select id, name, ROW_NUMBER() over( partition by name order by name ) as 'name_num' from ts2__Job__c)

select
a.Id as 'position-externalId',
iif(a.ts2__Account__c ='' or a.ts2__Account__c is null,'0',a.ts2__Account__c) as 'position-companyID',
iif(a.ts2__Contact__c is null or a.ts2__Contact__c = '',d.contact_id,a.ts2__Contact__c) as 'position-contactId',
iif(b.Phone is null or b.Phone ='','',b.Phone) as 'Client Phone',
concat(nullif(b.BillingCity,''),iif(b.billingState is null or b.billingState = '','', concat(', ', b.BillingState))) as 'Client Address',
iif(e.name_num = 1,e.name,concat(e.name_num,'-',e.name)) as 'position-title',
iif(c.Name = 'Perm' or c.Name = 'Closed','PERMANENT','TEMPORARY') as 'position-type',
iif(a.ts2__Date_Posted__c is null or a.ts2__Date_Posted__c ='','',a.ts2__Date_Posted__c) as 'position-startDate',
iif(a.ts2__Date_Filled__c is null or a.ts2__Date_Filled__c ='','',a.ts2__Date_Filled__c) as 'position-endDate',
iif(a.Key_Skill_or_Qualification__c is null or a.Key_Skill_or_Qualification__c ='','',a.Key_Skill_or_Qualification__c) as 'Skill',
[User].Email as 'position-owners',
iif(a.ts2__Openings__c is null or a.ts2__Openings__c ='','',a.ts2__Openings__c) as 'position-headcount',
concat('External ID: ',a.Id, (char(13)+char(10)),
nullif(concat('Skills: ',a.Key_Skill_or_Qualification__c),'Skills: ')
) as 'position-Note'

from ts2__Job__c a left join Account b on a.ts2__Account__c = b.id
left join RecordType c on a.RecordTypeId = c.Id
left join [User] on [User].Id = a.OwnerId
left join tempcontact2 d on a.ts2__Account__c = d.company_id
left join jobname e on a.Id = e.Id




