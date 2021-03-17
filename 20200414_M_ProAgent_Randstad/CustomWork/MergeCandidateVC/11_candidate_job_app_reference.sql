--Job App check reference
with job_app as (select candidate_id
		, count(*) as job_app_counts
		, max(status) as max_status
		from position_candidate
		group by candidate_id)
		
--select * from job_app where candidate_id in (47494, 47495, 125250)
		
, latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, ja1.job_app_counts as slave_job_app_counts
	, ja1.max_status as slave_max_status
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, ja2.job_app_counts as master_job_app_counts
	, ja2.max_status as master_max_status
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join job_app ja1 on ja1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	left join job_app ja2 on ja2.candidate_id = m.master
	where m.master is not NULL
	--and rn = 1
	--and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) select * from latest_candidate



---JOB COUNTS
--AUDIT CHECK
with latest_candidate as (select m.candidate_id as slave_candidate_id
	, c1.insert_timestamp as slave_reg_date
	, ce1.last_activity_date as slave_last_activity_date
	, c1.total_jobs as slave_total_jobs
	, m.master as master_candidate_id
	, c2.insert_timestamp as master_reg_date
	, ce2.last_activity_date as master_last_activity_date
	, c2.total_jobs as master_total_jobs
	from mike_tmp_candidate_dup_name_mail_dob_master_slave m
	left join candidate c1 on c1.id = m.candidate_id
	left join candidate_extension ce1 on ce1.candidate_id = m.candidate_id
	left join candidate c2 on c2.id = m.master
	left join candidate_extension ce2 on ce2.candidate_id = m.master
	where m.master is not NULL
	--and rn = 1
	--and coalesce(ce1.last_activity_date, c1.insert_timestamp) > coalesce(ce2.last_activity_date, c2.insert_timestamp) --slave more update than master
	order by m.candidate_id
) select * from latest_candidate


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
			and c.id in (select master from mike_tmp_candidate_dup_name_mail_dob_master_slave)
			) --select * from total_jobs -- rows
			
update candidate c
set total_jobs = t.total_jobs
from total_jobs t
where t.id = c.id


--UPDATE JOB COUNTS TO DUPLICATE CANDIDATES
-----BACK UP
select id, first_name, last_name, total_jobs, insert_timestamp, external_id
--into mike_candidate_total_jobs_bkup_20200908
from candidate


--AUDIT CHECK
with job_apps as 
	(select candidate_id, count(distinct position_description_id) as total_jobs
			from position_candidate
			group by candidate_id)
			
select *
from mike_tmp_candidate_dup_name_mail_dob_master_slave m
left join job_apps ja on ja.candidate_id = m.candidate_id


-----UPDATE
update candidate
set total_jobs = 0
where id in (select candidate_id from mike_tmp_candidate_dup_name_mail_dob_master_slave)