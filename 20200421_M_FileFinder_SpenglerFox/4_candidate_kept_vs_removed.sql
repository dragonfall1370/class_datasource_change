/*
create table mike_tmp_tobekept_candidate(
idperson character varying (100)
)

*/

--CANDIDATE TO BE KEPT
with interim as (
	select idassignment, assignmenttitle, idcompany, assignmentno
	from "assignment"
	where assignmenttitle ilike '%interim%' --408 rows
	
	UNION
	select idassignment, assignmenttitle, idcompany, assignmentno
	from "assignment"
	where idcompany in ('826df702-f17e-4939-9566-75dc74e3b21b', 'd6d459aa-4e5e-4771-a0a4-1b99fce610a4')
)

, selected_job as (select idassignment
	--, now() as deleted_timestamp
	--, 1 position_category --remove only job but not job leads
	--, 1 to_be_deleted
	from interim
	where assignmentno::int in (2001656,2001606,2001386,2001365,2001337,2001331,2001330,2001190,2001138,2001076,2001030,2000998,2000556,2000555,2000486,1008882,1008881,1008860,1008862,1008869,1008857,1008871,1008867,1008859,1008868,1008861,1008864,1008856,1008866)
	
	
UNION ALL	
select idassignment
	from "assignment" a
	where a.assignmentno in ('1004879','1007680','1008886','1011960','1013354','2001160','2001522','2001595','2001616','2001645','2001646','2001647')
)

select distinct idperson
from assignmentcandidate
where idassignment in (select idassignment from selected_job)

/* BACKUP EXISTING CANDIDATE NOT WITHIN TOBEKEPT CANDIDATE
select id, external_id, insert_timestamp, deleted_timestamp
into mike_tobedeleted_candidate_20200324
from candidate
where 1=1
and deleted_timestamp is NULL
and external_id is not NULL
and external_id not in (select idperson from mike_tmp_tobekept_candidate) --106150
*/

--MAIN SCRIPT
update candidate
set deleted_timestamp = '2020-03-24 18:00:00'
from mike_tobedeleted_candidate_20200324 m
where m.id = candidate.id