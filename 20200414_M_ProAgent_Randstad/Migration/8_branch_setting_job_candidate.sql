--JOB BRANCH
insert into branch_record (branch_id, record_id, record_type, insert_timestamp)
select 1149 branch_id
, position_id record_id
, 'job' record_type
, insert_timestamp
from team_group_position
where team_group_id = 1149 --22903 rows
	on conflict on constraint branch_record_branch_id_record_id_record_type_key
	do nothing


--CANDIDATE BRANCH
insert into branch_record (branch_id, record_id, record_type, insert_timestamp)
select 1149 branch_id
, candidate_id record_id
, 'candidate' record_type
, insert_timestamp
from team_group_candidate
where team_group_id = 1149
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

--USING DELETE IF MORE THAN 1 RECORD FOR RANDSTAD
delete from branch_record
where id in (22822, 22916, 22917)