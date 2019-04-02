--Department
select id,Department from Contact where Department <> ''

--Address Info
select Id,concat(
nullif(concat(mailingStreet,', '),', '),
nullif(concat(mailingCity, ', '),', '),
nullif(concat(mailingState, ', '),', '),
nullif(concat(mailingCountry, ', '),', '), 
nullif(mailingPostalCode,'')
) as 'contact-location'
from Contact where MailingStreet <> ''

--Mobile Phone
select Id,Phone from Contact where Phone <> ''

select 'add_con_info' as 'Additional_type',
Id as 'External_id',
Phone as 'CustomValue',
'Mobile Phone' as 'lookup_name',
getdate() as insert_timestamp
from Contact

--OtherPhone
select Id,Otherphone from Contact where OtherPhone <> ''

select 'add_con_info' as 'Additional_type',
Id as 'External_id',
Otherphone as 'CustomValue',
'OtherPhone' as 'lookup_name',
getdate() as insert_timestamp
from Contact

--OK to Contact?
select 'add_con_info' as 'Additional_type',
Id as 'External_id',
iif(Do_Not_Contact__c = 0,'Yes','No') as 'CustomValue',
'OK to Contact?' as 'lookup_name',
getdate() as insert_timestamp
from Contact
-------------------------
with test as (select id, iif(Do_Not_Contact__c = 0,'Yes','No') as Contactable__c from Account)


, test2 as (select 'add_con_info' as 'Additional_type',
a.Id as 'External_id',
b.Contactable__c as 'CustomValue',
'OK to Contact?' as 'lookup_name',
getdate() as insert_timestamp
from Contact a left join test b on a.AccountId = b.Id
left join RecordType on a.RecordTypeId+'AAQ' = RecordType.id
where RecordType.name='Contact')

select * from test2 where CustomValue is not null

