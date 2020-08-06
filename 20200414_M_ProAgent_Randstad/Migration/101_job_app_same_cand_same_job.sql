--Double check | Job application (same job / same candidate)
with job_app_filter as (select position_description_id, candidate_id, count(*)
	from position_candidate
	group by position_description_id, candidate_id
	having count(*) > 1)

select pc.id, pc.position_description_id, pc.candidate_id, pc.candidate_id_bkup, c.external_id
, pc.insert_timestamp
, pc.rejected_date
, pc.status
--into mike_tmp_position_candidate_same_candidate_and_job_20200806
from position_candidate pc
left join candidate c on c.id = pc.candidate_id_bkup
where 1=1
and concat_ws('', pc.position_description_id, pc.candidate_id) in
				(select concat_ws('', position_description_id, candidate_id)
				from job_app_filter)
--and pc.insert_timestamp > '2020-07-03' --migration time
order by position_description_id, candidate_id


--Delete 1 special case
select id, total_jobs
from candidate
where id = 118495

select id, position_description_id, candidate_id
from position_candidate
where candidate_id = 118495

select id, position_description_id, candidate_id, candidate_id_bkup, rejected_date, status, sub_status_id
from position_candidate
where position_description_id = 240812
and candidate_id = 118495


select *
from sub_status
where id in (15, 59)

--Delete merged job application
delete from position_candidate
where id = 694300