with test as (select a.Employment_id, b.id, ROW_NUMBER() over (partition by b.id order by b.id) as 'row_num' 
from Employment a left join company b on a.CompanyName = b.compname where b.id is not null),
TempPeople as (select * from People),

ownermail as (select distinct enterby, b.email from company a left join  MhUsers b on a.enterby = b.username),

notnullmail as (select cid,email from People where len(trim(char(13) + char(10) from trim(isnull(email, '')))) > 0),

dupmail as (select *,ROW_NUMBER() over (partition by email order by email) as 'mail_num' from notnullmail where email like '%@%.%'),


docfile2 as (select a.cid,iif(b.doctitle is null or b.doctitle = '','',concat(docs_id,'-',replace(doctitle,',','_'),'.',FileExt)) as 'doctitle',ROW_NUMBER() over (partition by doctitle order by doctitle) as 'row_num' from people a
left join docs b on a.cid = b.cid),

docfile as (select cid, doctitle as 'doctitle' from docfile2),

docname as ( select cid,string_agg(cast(doctitle as nvarchar(max)),',') as filename from docfile group by cid ),

test2 as (select 
a.cid as 'candidate-externalId',
iif(a.first = '' or a.first is null,'No First Name',a.first) as 'candidate-firstName',
a.MiddleName as 'candidate-middleName',
iif(a.last = '' or a.last is null, 'No Last Name',a.last) as 'candidate-lastName',
a.street as 'candidate-address',
a.city as 'candidate-city',
a.state as 'candidate-State',
a.zip as 'candidate-zipCode',

case when a.locale = 'CA' then 'CA'
when a.locale = 'cananda' then 'CA'
when a.locale = 'Canada' then 'CA'
when a.locale = 'USA' then 'US'
when a.locale = 'Australia' then 'AU'
else '' end as 'candidate-Country',
iif(g.CompanyName is null or g.CompanyName = '','',g.CompanyName) as 'candidate-company1',
iif(g.title is null or g.title = '','',g.title) as 'candidate-jobTitle1',
iif(i.email is null or i.email = '','',i.email) as 'candidate-owners',
a.position as 'candidate-position',

--iif(g.cphone is null or g.cphone = '','',g.cphone) as 'candidate-phone',

--concat(nullif(concat('Phone 1: ',g.cphone),'Phone 1: '),iif(g.cphone is null or g.cphone = '',
--nullif(concat('Phone 2: ',a.cellphone),'Phone 2: '),nullif(concat(' - Phone 2: ',a.cellphone),' - Phone 2: '))
--,iif(g.cphone = '' and a.cellphone = '', nullif(concat('Work Phone: ', g.cphone2),'Work Phone: '),nullif(concat(' - Work Phone: ',g.cphone2),' - Work Phone: '))
--) as 'candidate-phone',

iif(a.CellPhone is null or a.CellPhone = '','',a.CellPhone) as 'candidate-phone',
iif(a.CellPhone is null or a.CellPhone = '','',a.CellPhone) as 'candidate-mobilephone',
iif(a.homephone is null or a.homephone ='','',a.homephone) as 'candidate-homePhone',

case when (h.mail_num <> 1) then concat(h.mail_num,'-',h.email)
when (h.mail_num = 1) then h.email
else '' end
as 'candidate-email',

a.email2 as 'candidate-workEmail',
iif(a.birthday is null or a.birthday = '','',cast(a.birthday as varchar(255))) as 'candidate-dob',
iif(e.sourcename is null or e.sourcename = '','',e.sourcename) as 'CandidateSource',
j.filename as 'Candidate-Document',

concat(
'External ID: ', a.cid,(char(13)+char(10)),
nullif(concat('Preferred Name: ',a.nickname),'Preferred Name: '),(char(13)+char(10)),
nullif(concat('Home Email: ',a.email,(char(13)+char(10))),concat('Home Email: ',(char(13)+char(10)))),
nullif(concat('Alternative Email: ',g.contactemail),'Alternative Email: '),(char(13)+char(10)),
nullif(concat('Nick Name: ',a.nickname),'Nick Name: '),(char(13)+char(10)),
nullif(concat('Home Phone: ',a.homephone),'Home Phone: '),(char(13)+char(10)),
iif(a.optoutbulk = 0,'No Bulk 
: No', 'No Bulk Status: Yes'),(char(13)+char(10)),
iif(a.active = 1, 'Active Status: Yes', 'Active Status: No'),(char(13)+char(10)),
concat('Candidate Status: ',d.CandidStatusName),(char(13)+char(10)),
nullif(concat('Referred By: ',f.last,' ',f.first),'Referred By: '),(char(13)+char(10)),
nullif(concat('Specific Job Title Sought: ',a.position),'Specific Job Title Sought'),(char(13)+char(10)),
nullif(concat('Available to Start: ',a.AvailableToStart),'Available to Start: '),(char(13)+char(10)),
nullif(concat('Comment: ',a.relocate,(char(13)+char(10))),concat('Comment: ',(char(13)+char(10)))),

iif(a.bs is null or a.bs = '','',
((nullif(concat('Bs: ',a.bs),'Bs: ')))), nullif(concat(' - Bs Year: ',a.bsyear),' - Bs Year: 0'),(char(13)+char(10)),
nullif(concat('BsDeg: ',a.bsdeg,(char(13)+char(10))),concat('BsDeg: ',(char(13)+char(10)))),
nullif(concat('ms: ',a.ms),'ms: '), nullif(concat(' - Ms Year: ',a.msyear),' - Ms Year: 0'),(char(13)+char(10)),
nullif(concat('MsDeg: ',a.msdeg,(char(13)+char(10))),concat('MsDeg: ',(char(13)+char(10)))),
nullif(concat('phd: ',a.ms),'phd: '), nullif(concat(' - PhD Year: ',a.phdyear),' - PhD Year: 0'),(char(13)+char(10)),
nullif(concat('PhDDeg: ',a.phddeg,(char(13)+char(10))),concat('PhDDeg: ',(char(13)+char(10)))),
nullif(concat('Notes: ',a.notes),'Notes: '),(char(13)+char(10)),
nullif(concat('Contacts Notes: ',g.contactsnotes),'Contacts Notes: ')
)as 'candidate-Note',
a.employment_Id,

ROW_NUMBER() over (partition by a.cid order by a.cid) as 'row_num'
from People a
left join Employment b on a.Employment_Id = b.Employment_id
left join test c on a.Employment_Id = c.Employment_id
left join CandidStatus d on a.CandidStatus_ID = d.CandidStatus_ID
left join Sources e on a.Sources_ID = e.sources_id
left join TempPeople f on a.SourceNotesId = f.cid
left join Employment g on a.Employment_Id = g.Employment_id
left join dupmail h on a.cid = h.cid
left join ownermail i on a.CandidRecruiter = i.enterby
left join docname j on a.cid = j.cid
where a.RoleType = 0 and a.deleteflag = 0)

--select * from test2 where row_num = 1 and [candidate-companyId] = 0

select * from test2 where row_num = 1

