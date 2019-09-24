with test as (select cid from people where RoleType = 1)

, test2 as (select a.cid as external_id,
concat(
nullif(concat('Activity Type: ',a.activity,(char(13)+char(10))),concat('Acitivity Type: ',(char(13)+char(10)))),
nullif(concat('Name: ', a.Activityfirstname, ' ' , a.Activitylastname, (char(13)+char(10))),concat('Name: ',' ',(char(13)+char(10)))),
nullif(concat('Subject: ',a.regarding,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),a.activitynotes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
a.DateEnter as insert_timestamp,
'comment' as 'category', 
'contact' as 'type', 
-10 as 'user_account_id'
from test b left join
activity a on a.cid = b.cid
where a.cid <> 0)

,test3 as (select contacts_id as external_id,
concat(
nullif(concat('Activity Type: ',activity,(char(13)+char(10))),concat('Acitivity Type: ',(char(13)+char(10)))),
nullif(concat('Name: ', Activityfirstname, ' ' , Activitylastname, (char(13)+char(10))),concat('Name: ',' ',(char(13)+char(10)))),
nullif(concat('Subject: ',regarding,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),activitynotes),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
DateEnter as insert_timestamp,
'comment' as 'category', 
'contact' as 'type', 
-10 as 'user_account_id'
from activity where contacts_id <> 0)

select * from test2 where Content <> ''
union all
select * from test3





--with test as (select cid, first, last from people where RoleType = 1)

--,test2 as (select a.cid as external_id,
--concat(
--nullif(concat('Name: ', a.Activityfirstname, ' ' , a.Activitylastname, (char(13)+char(10))),concat('Name: ',' ',(char(13)+char(10)))),
--nullif(concat('Subject: ',a.regarding,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
--nullif(concat('Activity Notes: ',(char(13)+char(10)),a.activitynotes),concat('Activity Notes: ',(char(13)+char(10))))
--) as 'Content',
--a.DateEnter as insert_timestamp,
--'comment' as 'category', 
--'contact' as 'type', 
---10 as 'user_account_id',
--a.Activitylastname, a.Activityfirstname,
--ROW_NUMBER() over (partition by a.activitylastname, a.activityfirstname order by a.activitylastname) as 'row_num'
--from test b left join
--activity a on a.cid = b.cid
--where a.cid <> 0)

--,test3 as (select * from test2 where row_num = 1)

--,test4 as (select a.external_id, a.Content, b.regarding, b.Activityfirstname, b.Activitylastname, b.Activitynotes from test3 a left join activity b on concat( a.Activityfirstname, a.Activitylastname ) = concat(b.Activityfirstname, b.Activitylastname)
--where b.cid = 0)

--,test5 as (select *,ROW_NUMBER() over (partition by external_id,regarding order by external_id) as rn from test4)

--select external_id,
--concat(nullif(concat('Name: ', Activityfirstname, ' ' , Activitylastname, (char(13)+char(10))),concat('Name: ',' ',(char(13)+char(10)))),
--nullif(concat('Subject: ',regarding,(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
--nullif(concat('Activity Notes: ',(char(13)+char(10)),activitynotes),concat('Activity Notes: ',(char(13)+char(10))))
--) as 'Content'
--from test5 
--where Content <> ''
--order by external_id


