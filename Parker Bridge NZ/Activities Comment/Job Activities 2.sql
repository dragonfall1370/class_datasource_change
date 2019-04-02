select trim(a.vacancy) as external_id,
concat( 'Consulter: ', trim(c.username), ' - ', c.s37,(char(13)+char(10)),
nullif(concat('Subject: ',b.action,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),a.notes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
a.date as insert_timestamp,
'comment' as 'category', 
'job' as 'type', 
-10 as 'user_account_id'
from eventsarchive a 
left join secure c on a.consult = c.initials
left join actions b on a.type = b.code
where a.vacancy <> ''