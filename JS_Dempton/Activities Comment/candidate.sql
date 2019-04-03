with test as (select cid from people where RoleType = 0)

, test2 as (select a.cid as external_id,
concat(
nullif(concat('Activity Type: ',a.activity,(char(13)+char(10))),concat('Acitivity Type: ',(char(13)+char(10)))),
nullif(concat('Name: ', a.Activityfirstname, ' ' , a.Activitylastname, (char(13)+char(10))),concat('Name: ',' ',(char(13)+char(10)))),
nullif(concat('Subject: ',a.regarding,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),a.activitynotes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
a.DateEnter as insert_timestamp,
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id'
from test b left join
activity a on a.cid = b.cid
where a.cid <> 0)

select * from test2 where Content <> ''