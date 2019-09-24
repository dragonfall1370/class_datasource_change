with contact as (select * from people where RoleType = 1),

ownermail as (select distinct enterby, b.email from company a left join  MhUsers b on a.enterby = b.username),

test as (select a.Employment_id, b.id, ROW_NUMBER() over (partition by b.id order by b.id) as 'row_num' 
from Employment a left join company b on a.CompanyName = b.compname where b.id is not null),

document as (select id,cid,iif(doctitle = '' or doctitle is null,'',concat(docs_id,'-',replace(doctitle,',','_'),'.',FileExt)) as filename from docs),

dupmail as (select a.cid, b.Contactemail from People a left join Employment b on a.cid = b.Cid where RoleType = 1 and Contactemail <> ''),

dupmail2 as (select *, ROW_NUMBER() over ( partition by contactemail order by contactemail ) as 'mail_num' from dupmail),

dupmailman as (select cid, iif(mail_num <> 1, concat(mail_num,'-',contactemail),contactemail) as email from dupmail2),

test4 as (select 
a.reference as 'position-externalId',
iif(e.[contact-externalId] is null or e.[contact-externalId] ='','0',e.[contact-externalId]) as 'position-contactId',
e.[contact-companyId],
iif(a.job_title is null or a.job_title ='','No Job Name',a.job_title) as 'position-title'
--iif(c.email is null or c.email = '','',c.email)  as 'position-owners',
--[dbo].[udf_StripHTML](a.job_desc) as 'position-publicDescription',
--a.JobNotes as 'position-internalDescription',
--iif(a.JobsDate_placed is null or a.JobsDate_placed = '',a.dateenter,a.JobsDate_placed) as 'position-startDate',
--iif(a.expire = 1,convert(datetime,dateadd(dd,-1, cast(getdate() as date)),11),'2019-10-29 00:00:00.000') as 'position-endDate',
--case when (a.EmploymentTerm = 'Contract') then 'CONTRACT'
--when (a.EmploymentTerm = 'Permanent') then 'PERMANENT'
--when (a.EmploymentTerm = 'Project') then 'TEMPORARY'
--when (a.EmploymentTerm = '*') then 'PERMANENT'
--when (a.EmploymentTerm = 'Both') then 'PERMANENT'
--else 'PERMANENT' end
--as 'position-type',
--'FULL_TIME' as 'position-employmentType',
--concat('External ID: ',a.reference,(char(13)+char(10)),
--iif(a.expire = 1,'Job Closed: Yes' + (char(13)+char(10)),'Job Closed: No' + (char(13)+char(10))),
--'Job Won: No', (char(13)+char(10)),
--nullif(concat('Filled By: ',a.enterby),'Filled By: '), (char(13)+char(10)),
--nullif(concat('Recruiter: ',a.JobsCounselor),'Recruiter: '), (char(13)+char(10)),
--nullif(concat('Pipeline Status: ',a.status),'Pipeline Status: '), (char(13)+char(10)),
--nullif(concat('Division: ',a.jobscity),'Division: '),
--nullif(concat('Created By',a.enterby),'Created By'), (char(13)+char(10)),
--nullif(concat('Modified By',a.modifiedby),'Modified By'), (char(13)+char(10)),
--nullif(concat('Requisition: ',a.req_number),'Requisition: '), (char(13)+char(10)),
--nullif(concat('#Available: ',a.positions),'#Available: '), (char(13)+char(10)),
--nullif(concat('Job Percent Fee: 0',a.JobPercentFee),'Job Percent Fee: 0'), (char(13)+char(10)),
--nullif(concat('Job Flat Fee: ',a.JobFlatFee),'Job Flat Fee: '),
--nullif(concat('Employment Type: ',d.description),'Employment Type: ')
--) as 'position-Note'

from jobs a
left join contact b on a.cont_last = b.last and a.cont_first = b.first
left join ownermail c on a.JobAccountManager = c.enterby
left join EmploymentTypes d on a.JobEmploymentTypeID = d.Id
left join [Dempton-contact] e on a.id = e.[contact-companyId]
),

test5 as (select *,ROW_NUMBER() over (partition by [position-externalId] order by [position-externalId]) as 'row_num' from test4),

test6 as (select *, ROW_NUMBER() over (partition by [position-title] order by [position-title]) as 'job_num' from test5 where row_num = 1)

select iif(job_num = 1,[position-title],concat(job_num,' - ',[position-title])) as 'position-title2',
iif([position-contactId] is null or [position-contactId] = '',0,[position-contactId]) as 'position-contactId2' , * from test6

where [contact-companyId] = 481