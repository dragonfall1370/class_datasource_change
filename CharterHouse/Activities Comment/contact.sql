  select a.contcode as external_id,
concat('Subject: Call Log', (char(13)+char(10)),
nullif(concat('Activity Notes: ',(char(13)+char(10)),a.notes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
a.when1 as insert_timestamp,
'comment' as 'category', 
'contact' as 'type', 
-10 as 'user_account_id'
from calllog a
where a.contcode <> ''


