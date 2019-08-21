
select a.id as external_id,
concat(
nullif(concat('Activity Type: ',a.activity,(char(13)+char(10))),concat('Acitivity Type: ',(char(13)+char(10)))),
'Name: ', a.Activityfirstname, ' ' , a.Activitylastname, (char(13)+char(10)),
nullif(concat('Subject: ',a.regarding,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),a.activitynotes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
a.DateEnter as insert_timestamp,
'comment' as 'category', 
'company' as 'type', 
-10 as 'user_account_id'
from activity a where a.id <> 0