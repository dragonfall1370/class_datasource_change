with 
--document1 as (select table1,code,reverse(left(REVERSE(filename), charindex('\', REVERSE(filename)) - 1)) as filename from attachment)

--,document2 as (select code,string_agg(cast(trim(filename) as varchar(max)),',') as filename

--from document1 where table1 = 'VC' group by code),

test as (select  
a.code as 'position-externalId',
iif(contactcod = '' or contactcod is null,'0',contactcod) as 'position-contactId',
isnull(clientref,'') as companyid,
iif(title = '' or title is null,'No Job Name',trim(title)) as 'position-title',
isnull(b.s37,'') as 'position-owners',
isnull(places,'') as headcount,
isnull(duration,'') as 'contract-length',
case when type = 'T' then 'TEMPORARY'
when type = 'F' then 'PERMANENT'
when type = 'P' then 'PERMANENT'
when type = 'C' then 'CONTRACT'
else '' end
as 'position-type',
convert(datetime, CONVERT(float,logged)) as 'position-startDate', 
convert(datetime, CONVERT(float,isnull(replace(END_DATE,'-284882',''),''))) as 'position-endDate',
concat(
concat('External ID: ',a.code),(char(13)+char(10)),
nullif(concat('Log on by: ',a.[by],(char(13)+char(10))),concat('Log on by: ',(char(13)+char(10)))),
nullif(concat('Client Rate: ',clientrate,(char(13)+char(10))),concat('Client Rate: ',(char(13)+char(10)))),
'Status: ',(case
when a.status = 1 then 'Immediate'
when a.status = 3 then 'Ongoing'
when a.status = 4 then 'On Hold'
when a.status = 5 then 'Filled by us'
when a.status = 6 then 'No Candidates'
when a.status = 7 then 'Timed Out'
when a.status = 8 then 'Client Cancelled'
when a.status = 9 then 'Client Unsatisfied'
when a.status = 10 then 'Client Unsuitable'
when a.status = 11 then 'Filled by another agency'
when a.status = 12 then 'Filled by Client'
when a.status = 13 then 'Filled Internally'
when a.status = 14 then 'Speculative Search'
when a.status = 20 then 'Deleted'
when a.status = 80 then 'Archieved'
end)
)as 'position-notes'

--,iif(b.filename is null or b.filename ='','',b.filename) as 'position-document'
from vacancies a
left join secure b on trim(a.CONSULT) = b.initials
--left join document2 b on a.code = b.code
)


, test2 as (select ROW_NUMBER() over ( partition by [position-title] order by [position-title]) as rn,* from test)

select iif([position-title] is null or [position-title] = '','No Job Title',iif(rn=1,[position-title],concat(rn,'-',[position-title]))) as position,* from test2