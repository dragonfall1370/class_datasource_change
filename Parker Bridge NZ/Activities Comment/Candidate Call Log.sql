  select trim(a.candcode) as external_id,
concat('Subject: Call Log',
nullif(concat('Activity Notes: ',(char(13)+char(10)),a.notes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
a.when1 as insert_timestamp,
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id'
from calllog a
where a.candcode <> '' and notes <> ''