with test as (select a.Employment_id, b.id, ROW_NUMBER() over (partition by b.id order by b.id) as 'row_num' 
from Employment a left join company b on a.CompanyName = b.compname where b.id is not null),

document as (select id,cid,iif(doctitle = '' or doctitle is null,'',concat(docs_id,'-',replace(doctitle,',','_'),'.',FileExt)) as filename from docs),

ownermail as (select distinct enterby, b.email from company a left join  MhUsers b on a.enterby = b.username),

dupmail as (select a.cid, b.Contactemail from People a left join Employment b on a.cid = b.Cid where RoleType = 1 and Contactemail <> ''),

dupmail2 as (select *, ROW_NUMBER() over ( partition by contactemail order by contactemail ) as 'mail_num' from dupmail),

dupmailman as (select cid, iif(mail_num <> 1, concat(mail_num,'-',contactemail),contactemail) as email from dupmail2),

test2 as (select 
a.cid as 'contact-externalId',

--case when (b.id <> 0 or b.id is not null) then b.id
--when (b.id = 0 or b.id is null) then c.id 
--when (a.Employment_id is null) then '0'
--else '0' end select * from people where RoleType = 1 
--as 'contact-companyId',
iif(b.id = 0, c.id, b.id) as 'contact-companyId',
iif(a.first is null or a.first ='','No First Name',a.first) as 'contact-firstName',
iif(a.last is null or a.last ='','No Last Name',a.last) as 'contact-lastName',
a.MiddleName as 'contact-middleName',
iif(d.email is null or d.email = '','',d.email) as 'contact-owners',
iif(b.title is null or b.title = '','',b.title) as 'contact-position',
b.cphone as 'contact-phone',
a.workphone as 'workphone',
a.CellPhone as 'cellphone',
a.homephone as 'homephone',
f.email as 'contact-email',
a.email2 as 'email2',
a.birthday as 'birthday',
concat('External ID: ',a.cid,(char(13)+char(10)),
nullif(concat('Main Detail > Work Profile Tab: ',ContactsNotes,(char(13)+char(10))), concat('Main Detail > Work Profile Tab: ',(char(13)+char(10)))),
nullif(concat((char(13)+char(10)),'Alternate Email: ',b.contactemail2,(char(13)+char(10))),concat((char(13)+char(10)),'Alternate Email: ',(char(13)+char(10)))),




iif(a.optoutbulk = 0,'No Bulk Status: No', 'No Bulk Status: Yes'),(char(13)+char(10)),
iif(a.bs is null or a.bs = '','',
((nullif(concat('Bs: ',a.bs),'Bs: ')))), nullif(concat(' - Bs Year: ',a.bsyear),' - Bs Year: 0'),(char(13)+char(10)),
nullif(concat('BsDeg: ',a.bsdeg,(char(13)+char(10))),concat('BsDeg: ',(char(13)+char(10)))),
nullif(concat('ms: ',a.ms),'ms: '), nullif(concat(' - Ms Year: ',a.msyear),' - Ms Year: 0'),(char(13)+char(10)),
nullif(concat('MsDeg: ',a.msdeg,(char(13)+char(10))),concat('MsDeg: ',(char(13)+char(10)))),
nullif(concat('phd: ',a.ms),'phd: '), nullif(concat(' - PhD Year: ',a.phdyear),' - PhD Year: 0'),(char(13)+char(10)),
nullif(concat('PhDDeg: ',a.phddeg,(char(13)+char(10))),concat('PhDDeg: ',(char(13)+char(10)))),
nullif(concat('Notes: ',(char(13)+char(10)),a.notes),concat('Notes: ',(char(13)+char(10)))) 
)as 'contact-Note',
iif(e.filename is null or e.filename ='','',e.filename) as 'contact-Document',
a.employment_Id,

ROW_NUMBER() over (partition by a.cid order by a.cid) as 'row_num'
from People a
left join Employment b on a.Employment_Id = b.Employment_id
left join test c on a.Employment_Id = c.Employment_id
left join ownermail d on a.CandidRecruiter = d.enterby
left join document e on a.cid = e.cid
left join dupmailman f on a.cid = f.cid
where a.RoleType = 1),

--select * from test2 where row_num = 1 and [contact-companyId] = 0

test3 as (select [contact-externalId],
iif([contact-companyId] is null,0,[contact-companyId]) as 'contact-companyId',
[contact-firstName],
[contact-lastName],
[contact-middleName],
[contact-owners],
[contact-position],
concat(nullif(concat('Phone 1: ',[contact-phone]),'Phone 1: '),
iif([contact-phone] is null or [contact-phone] = '',
nullif(concat('Phone 2: ',cellphone),'Phone 2: '),nullif(concat(' - Phone 2: ',cellphone),' - Phone 2: ')),
iif([contact-phone] = '' and cellphone = '', 
nullif(concat('Work Phone: ',workphone),'Work Phone: '),nullif(concat(' - Work Phone: ',workphone),' - Work Phone: '))) as 'workphone',
iif(cellphone is null or cellphone = '','', cellphone) as cellphone,
--iif(workphone is null or workphone = '','',workphone) as 'contact-phone',
iif(homephone is null or homephone = '','',homephone) as 'contact-phone', ---home phone


iif([contact-email] is null,'',[contact-email]) as 'contact-email',
[contact-Note],
[contact-Document],

ROW_NUMBER() over (partition by [contact-email] order by [contact-email]) as 'mail_num'
from test2 where row_num = 1)

select * from test3 

where [contact-externalId] = 2995

--select * from people where cid=9754




----- aditional fixed note
--select a.cid as 'contact-externalId',

--concat('External ID: ',a.cid,(char(13)+char(10)),
--nullif(concat('Main Detail > Work Profile Tab: ',(char(13)+char(10)),ContactsNotes,(char(13)+char(10))), concat('Main Detail > Work Profile Tab: ',(char(13)+char(10)),(char(13)+char(10)))),
--nullif(concat((char(13)+char(10)),'Alternate Email: ',b.contactemail2,(char(13)+char(10))),concat((char(13)+char(10)),'Alternate Email: ',(char(13)+char(10))))




----iif(a.optoutbulk = 0,'No Bulk Status: No', 'No Bulk Status: Yes'),(char(13)+char(10)),
----iif(a.bs is null or a.bs = '','',
----((nullif(concat('Bs: ',a.bs),'Bs: ')))), nullif(concat(' - Bs Year: ',a.bsyear),' - Bs Year: 0'),(char(13)+char(10)),
----nullif(concat('BsDeg: ',a.bsdeg,(char(13)+char(10))),concat('BsDeg: ',(char(13)+char(10)))),
----nullif(concat('ms: ',a.ms),'ms: '), nullif(concat(' - Ms Year: ',a.msyear),' - Ms Year: 0'),(char(13)+char(10)),
----nullif(concat('MsDeg: ',a.msdeg,(char(13)+char(10))),concat('MsDeg: ',(char(13)+char(10)))),
----nullif(concat('phd: ',a.ms),'phd: '), nullif(concat(' - PhD Year: ',a.phdyear),' - PhD Year: 0'),(char(13)+char(10)),
----nullif(concat('PhDDeg: ',a.phddeg,(char(13)+char(10))),concat('PhDDeg: ',(char(13)+char(10)))),
----nullif(concat('Notes: ',(char(13)+char(10)),a.notes),concat('Notes: ',(char(13)+char(10)))) 
--)as 'contact-Note'

--from People a
--left join Employment b on a.Employment_Id = b.Employment_id
--where a.RoleType = 1
