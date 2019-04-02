--with test as (select a.organisation_ref,b.last_name,b.first_name from position a left join person b on a.person_ref = b.person_ref)
with test3 as (select * from lookup where code_type = 123)

select a.opportunity_ref as external_id,
concat(
--'Name: ', b.last_name, ' ' , b.first_name, (char(13)+char(10)),
'Author: ',b.last_name,' ',b.first_name,' - ',b.email_address,(char(13)+char(10)),
nullif(concat('Subject: ',replace(a.displayname,'\x0d\x0a',' '),(char(13)+char(10))),concat('Subject: ',(char(13)+char(10)))),
nullif(concat('Activity Notes: ',(char(13)+char(10)),replace(a.notes,'\x0d\x0a',' ')),concat('Activity Notes: ',(char(13)+char(10))))
) as 'Content',
cast(a.event_date as datetime) as insert_timestamp,
'comment' as 'category', 
'job' as 'type', 
-10 as 'user_account_id',
c.description,
event_ref
from event a 
left join person b on a.create_user = b.person_ref
left join test3 c on a.z_last_type = c.code
--left join test b on a.organisation_ref = b.organisation_ref
where a.opportunity_ref <> '' and a.notes <> ''