--EXISTING CONTACT
select id
from contact
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'REC%') --11376

--
select max(id), count(id)
from team_group_contact --max=150045 | count=150045

--Audit check
with con_vc as (select id
	from contact
	where deleted_timestamp is NULL
	and (external_id is NULL or external_id not ilike 'REC%'))
	
select team_group_contact.*
from team_group_contact, con_vc
where team_group_contact.contact_id = con_vc.id --1 row


--| MAIN SCRIPT <BRAND> |--
insert into team_group_contact (contact_id, team_group_id, insert_timestamp)
select id as contact_id
, 1105 as team_group_id --BRAND
, current_timestamp as insert_timestamp
from contact
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'REC%')
on conflict on constraint team_group_contact_team_group_id_contact_id_key
	do nothing
	

--| MAIN SCRIPT <BRANCH> |--
insert into team_group_contact (contact_id, team_group_id, insert_timestamp)
select id as contact_id
, 1149 as team_group_id --BRANCH
, current_timestamp as insert_timestamp
from contact
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'REC%')
on conflict on constraint team_group_contact_team_group_id_contact_id_key
	do nothing