with test as (select a.organisation_ref,b.last_name,b.first_name from position a left join person b on a.person_ref = b.person_ref)
,test3 as (select * from lookup where code_type = 123)

,test2 as (select a.organisation_ref as external_id,
concat(
--'Name: ', b.last_name, ' ' , b.first_name, (char(13)+char(10)),
'Author: ',b.last_name,' ',b.first_name,' - ',b.email_address,(char(13)+char(10)),
nullif(concat('Subject: ',replace(a.displayname,'\x0d\x0a',''),(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),replace(a.notes,'\x0d\x0a','')),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
cast(a.event_date as datetime) as insert_timestamp,
'comment' as 'category', 
'company' as 'type', 
-10 as 'user_account_id',
c.description,
event_ref
from event a 
left join person b on a.create_user = b.person_ref
left join test3 c on a.z_last_type = c.code
where a.organisation_ref <> '')

select * from test2
--,test3 as (select *,ROW_NUMBER() over (partition by content order by content) as rn from test2)

--select * from test3 where rn = 1


