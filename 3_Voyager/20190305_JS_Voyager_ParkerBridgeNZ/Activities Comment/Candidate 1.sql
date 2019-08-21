with event1 as (select * from events where len(trim(isnull(cast(notes as varchar(max)), ''))) > 0)


select trim(a.code) as external_id,
concat(
nullif(concat('Subject: ',b.action,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),a.notes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
a.date as insert_timestamp,
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id'
from event1 a 
left join secure c on a.consult = c.initials
left join actions b on a.type = b.code
where a.code <> '' and a.notes is not null