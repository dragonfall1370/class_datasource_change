select *
from position_candidate_count_view
where candidate_id = 98652

select *
from candidate_long_list_count_view
where candidate_id = 98652


select id, total_jobs
from candidate
where id = 98652

--MAIN SCRIPT TO UPDATE UNIQUE CANDIDATES WITH MERGED APPLICATIONS
with job_apps as 
	(select candidate_id, count(distinct position_description_id) as total_jobs
			from position_candidate
			group by candidate_id)
			
, total_jobs as (select c.id
			, c.total_jobs as origin
			, ja.total_jobs
			from candidate c
			join job_apps ja on ja.candidate_id = c.id
			where 1=1
			and c.total_jobs != ja.total_jobs
			--and ja.candidate_id = 98652
			) --select * from total_jobs --6875 rows
			
update candidate c
set total_jobs = t.total_jobs
from total_jobs t
where t.id = c.id


--UPDATE JOB COUNTS TO DUPLICATE CANDIDATES
-----BACK UP
select id, first_name, last_name, total_jobs, insert_timestamp, external_id
--into mike_candidate_total_jobs_bkup_20200617
from candidate

-----UPDATE
update candidate
set total_jobs = 0
where id not in (select candidate_id from position_candidate group by candidate_id)
and external_id ilike '%CDT%' --78018 records