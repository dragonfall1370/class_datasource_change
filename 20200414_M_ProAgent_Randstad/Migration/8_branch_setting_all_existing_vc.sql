--JOB BRANCH
insert into branch_record (branch_id, record_id, record_type, insert_timestamp)
select 1127 branch_id --1127 | 東京本社 (Tokyo Head Office)
, id record_id
, 'job' record_type
, current_timestamp as insert_timestamp
from position_description
	where deleted_timestamp is NULL
	and (external_id is NULL or external_id not ilike 'JOB%')
	on conflict on constraint branch_record_branch_id_record_id_record_type_key
		do nothing


--CANDIDATE BRANCH
insert into branch_record (branch_id, record_id, record_type, insert_timestamp)
select 1127 branch_id --1127 | 東京本社 (Tokyo Head Office)
, id record_id
, 'candidate' record_type
, current_timestamp as insert_timestamp
from candidate
	where deleted_timestamp is NULL
	and (external_id is NULL or external_id not ilike 'CDT%')
	on conflict on constraint branch_record_branch_id_record_id_record_type_key
		do nothing

	
--AUDIT
select record_id, count(*)
from branch_record
where record_type = 'job'
group by record_id
having count(*) > 1

select *
from branch_record
where record_id in (204959, 204861, 204958)