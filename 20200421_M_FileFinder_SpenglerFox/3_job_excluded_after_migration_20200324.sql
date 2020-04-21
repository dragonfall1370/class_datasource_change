-->>UPDATE DELETED TIMESTAMP FOR EXCLUDED JOBS
select id
from candidate
where id in (select id from mike_tobedeleted_candidate_20200324)

update candidate
set deleted_timestamp = '2020-03-24 18:00:00'
from mike_tobedeleted_candidate_20200324 m
where m.id = candidate.id

select id
from candidate
where deleted_timestamp is NULL

select *
from position_description --569
where deleted_timestamp is not NULL

--->> DELETE JOBS EXCLUDED FROM THE LIST
--delete compensation
select *
into tmp_compensation_20200325
from compensation

delete from compensation
where position_id in (select id from position_description where deleted_timestamp is not NULL) --379

--delete position_candidate
select *
into tmp_position_candidate_20200325
from position_candidate

delete from position_candidate
where position_description_id in (select id from position_description where deleted_timestamp is not NULL) --835

--Deleted position_agency_consultant
select *
into tmp_position_agency_consultant_20200325
from position_agency_consultant --371

delete from position_agency_consultant
where position_id in (select id from position_description where deleted_timestamp is not NULL) --189
--
select *
into tmp_position_description_20200325
from position_description --569

delete from position_description
where deleted_timestamp is not NULL