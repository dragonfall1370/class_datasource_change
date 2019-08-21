with 
document1 as (select table1,code,reverse(left(REVERSE(filename), charindex('\', REVERSE(filename)) - 1)) as filename from attachment)

,document2 as (select code,string_agg(cast(trim(filename) as varchar(max)),',') as filename

from document1 where table1 = 'VC' group by code)

,test as (select  
a.code as 'position-externalId',
iif(contactcod = '' or contactcod is null,'0',contactcod) as 'position-contactId',
iif(title = '' or title is null,'No Job Name',trim(title)) as 'position-title',
consult as 'position-owners',
case when type = 'T' then 'TEMPORARY'
when type = 'F' then 'PERMANENT'
when type = 'P' then 'PERMANENT'
when type = 'C' then 'CONTRACT'
else '' end
as 'position-type',
start_date as 'position-startDate',
end_date as 'position-endDate',
concat(
concat('External ID: ',a.code),(char(13)+char(10)),
nullif(concat('Profit: ',profit,(char(13)+char(10))),concat('Profit: ',(char(13)+char(10)))),
nullif(concat('Candidate Rate: ',candrate,(char(13)+char(10))),concat('Candidate Rate: ',(char(13)+char(10)))),
nullif(concat('Client Rate: ',clientrate,(char(13)+char(10))),concat('Client Rate: ',(char(13)+char(10))))
--nullif(concat('Start Date: ',start_date,(char(13)+char(10))),concat('Start Date: ',(char(13)+char(10)))),
--nullif(concat('Review Date: ',end_date,(char(13)+char(10))),concat('Review Date: ',(char(13)+char(10))))
)as 'position-notes',

iif(b.filename is null or b.filename ='','',b.filename) as 'position-document',
a.contclient
from vacancies a
left join document2 b on a.code = b.code)


, test2 as (select ROW_NUMBER() over ( partition by [position-title] order by [position-title]) as rn,* from test)

,test3 as (select concat(b.forename,b.surname) as name,iif([position-title] is null or [position-title] = '','No Job Title',iif(rn=1,[position-title],concat(rn,'-',[position-title]))) as position, a.*
from test2 a
left join contact b on a.[position-contactId] = b.code)

,test4 as (select
case when [position-contactid] = '0' then '0'
when [position-contactid] <> '0' and name <> '' then [position-contactid]
when [position-contactid] <> '0' and name = '' then concat(substring([position-contactid],0,charindex('-',[position-contactid],0)),'9999')
end as correct_position_contactid,
* from test3)

select 
case when correct_position_contactid like '%-%' then [position-notes]
when correct_position_contactid = '0' then [position-notes]
else concat('Contact Link: ', contclient,(char(13)+char(10)),[position-notes])
end as true_note
,* from test4