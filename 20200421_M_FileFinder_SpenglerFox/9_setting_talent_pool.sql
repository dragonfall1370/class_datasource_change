/* BEFORE RUNNING ADD TEMP COLUMN
alter table candidate_group
add column candidate_group_ext_id character varying (100) --in this case, iterim idassigment will be candidate_group_ext_id
*/

with interim as (
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where assignmenttitle ilike '%interim%' --408 rows
	
	UNION
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where idcompany in ('826df702-f17e-4939-9566-75dc74e3b21b', 'd6d459aa-4e5e-4771-a0a4-1b99fce610a4')
) --409 rows

select distinct idassignment candidate_group_ext_id
, trim(assignmenttitle) as group_name
, -10 as owner_id
, 1 as share_permission --1 public -2 private -3 share
, now() as insert_timestamp
from interim