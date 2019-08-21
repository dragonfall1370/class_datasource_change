with 
person2 as (select last_name,first_name,person_ref as 'external_id',email_address from person)
,test as (select a.event_ref,b.person_ref as 'external_id',a.event_date,a.z_last_type,z_last_outcome,
concat(
--'Name: ', b.last_name, ' ' , b.first_name, (char(13)+char(10)),
'Author: ',d.last_name,' ',d.first_name,' - ',d.email_address,(char(13)+char(10)),
'To: ',e.last_name,' ',e.first_name,' - ',e.email_address,(char(13)+char(10)),
nullif(concat('Subject: ',replace(a.displayname,'\x0d\x0a',''),(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),replace(a.notes,'\x0d\x0a','')),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content'
from event a right join event_role b on a.event_ref = b.event_ref
left join person d on a.create_user = d.person_ref
left join person2 e on b.person_ref = e.external_id
)

,test2 as (select a.*, b.description as 'type2',event_date as 'start_date',
'comment' as 'category', 
'candidate' as 'type', 
-10 as 'user_account_id'
from test a
left join lookup b on a.z_last_type = b.code
where external_id <> ''
and b.code_type = 123)

select * from test2 where external_id <> ''