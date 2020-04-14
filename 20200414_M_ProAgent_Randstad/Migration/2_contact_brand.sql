select *
from team_group
where group_type = 'BRAND' --1105-Professionals 1106-CA 1107-Challenged 1108-Staffing

select contact_id, team_group_id, insert_timestamp
from team_group_contact

--MAIN SCRIPT
insert into team_group_contact(contact_id, team_group_id, insert_timestamp)
(select id as contact_id
, 1105 team_group_id
, current_timestamp insert_timestamp 
from contact
where external_id ilike 'REC%'
and deleted_timestamp is NULL --37511

UNION ALL
select id as contact_id
, 1106 team_group_id
, current_timestamp insert_timestamp 
from contact
where external_id ilike 'REC%'
and deleted_timestamp is NULL --37511

UNION ALL
select id as contact_id
, 1107 team_group_id
, current_timestamp insert_timestamp 
from contact
where external_id ilike 'REC%'
and deleted_timestamp is NULL --37511

UNION ALL
select id as contact_id
, 1108 team_group_id
, current_timestamp insert_timestamp 
from contact
where external_id ilike 'REC%'
and deleted_timestamp is NULL --37511
)