-->> COMPANY <<--
select *
from team_group
where 1=1
--and name ilike '%東京本社%' --BRANCH | 1149
and name ilike '%Professional%' --BRAND | 1105

--EXISTING COMPANY
select id
from company
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CPY%') --24376

--
select max(id), count(id)
from team_group_company --max=59125 | count=29640

--Audit check
with com_vc as (select id
	from company
	where deleted_timestamp is NULL
	and (external_id is NULL or external_id not ilike 'CPY%'))
	
select team_group_company.*
from team_group_company, com_vc
where team_group_company.company_id = com_vc.id

--| MAIN SCRIPT <BRAND> |--
insert into team_group_company (company_id, team_group_id, insert_timestamp)
select id as company_id
, 1105 as team_group_id --BRAND
, current_timestamp as insert_timestamp
from company
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CPY%')
on conflict on constraint team_group_company_team_group_id_company_id_key
	do nothing
	
--->> NOT USING BRANCH FOR COMPANY
--| MAIN SCRIPT <BRANCH> |--
insert into team_group_company (company_id, team_group_id, insert_timestamp)
select id as company_id
, 1149 as team_group_id --BRANCH
, current_timestamp as insert_timestamp
from company
where deleted_timestamp is NULL
and (external_id is NULL or external_id not ilike 'CPY%')
on conflict on constraint team_group_company_team_group_id_company_id_key
	do nothing