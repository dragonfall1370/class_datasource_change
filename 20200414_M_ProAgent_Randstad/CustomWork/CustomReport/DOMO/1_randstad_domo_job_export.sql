with job_brand as (select t.position_id
	--, t.team_group_id
	, string_agg(tg.name,',') as brand
	from team_group_position t
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = t.team_group_id
	group by t.position_id
	)
	
, job_branch as (select b.record_id as position_id
	--, b.branch_id
	, string_agg(tg.name, ',') as branch
	from branch_record b
	left join (select * from team_group where group_type = 'BRANCH') tg on tg.id = b.branch_id
	where 1=1
	and record_type = 'job'
	group by b.record_id
	)
	
, job_app as (select id as job_app_id
	, candidate_id
	, position_description_id
	, associated_date
	, sent_date
	, hire_date
	, case when associated_date is not null then DATE_PART('day', current_timestamp - associated_date)
		else 0 end as time_to_shortlist
	, case when sent_date is not null then DATE_PART('day', sent_date - associated_date)
		else 0 end as time_to_sent
	from position_candidate pc
	where insert_timestamp > '20208-08-01' --Filter date to limit the job app counts
	) --select * from job_app


, job_app_rotten as (select pc.id as job_app_id
	, pc.candidate_id
	, pc.position_description_id
	, pc.last_stage_date
	, ce.last_activity_date
	, DATE_PART('day', ce.last_activity_date - pc.last_stage_date) as rotten_days
	from position_candidate pc
	left join candidate_extension ce on ce.candidate_id = pc.candidate_id
	where pc.insert_timestamp > '20208-08-01'
	) --select * from job_app_rotten

, job_owners as (select pac.position_id 
	, string_agg(u.name, ',') as job_owners
	from position_agency_consultant pac
	left join user_account u on u.id = pac.user_id
	group by pac.position_id 
	)

, job_reopen as (select position_id
	, max(insert_timestamp::date) as reopen_date --latest reopen date for job
	from activity
	where 1=1
	and "content" = 'This job has been re-opened'
	group by position_id
	)
	
, placement_profit as (select o.id
	, pc.position_description_id
	, coalesce(o.projected_profit, o.profit, 0) as placement_profit
	from offer o
	left join position_candidate pc on pc.id = o.position_candidate_id
	where 1=1
	and pc.status > 300 --placements
	)
	
select --count(*)
	pd.id as "Job ID"
	, jbr.brand as "Brand"
	, jbc.branch as "Branch"
	, pd.insert_timestamp "Job entry date"
	, coalesce(ja.time_to_shortlist, 0) as "Time to shortlist"
	, coalesce(ja.time_to_sent, 0) as "Time from shortlist to cv sent"
	, coalesce(jar.rotten_days, 0) as "Rotting time (close if not focusing)"
	, jo.job_owners "Job owner"
	, coalesce(ja.hire_date, pd.head_count_close_date) "Close date / placement date"
	, jro.reopen_date as "Re-open date (if applicable)"
	, pe.last_activity_date as "Last activity date"
	, case when pd.published_date is not NULL then 'Yes'
			else 'No' end as Posted
	, pd.published_date as "Posting date"
	, coalesce(pp.placement_profit, 0) as "Job Fee amount (GP)"
from position_description pd
left join position_extension pe on pe.position_id = pd.id --unique
left join job_brand jbr on jbr.position_id = pd.id --unique
left join job_branch jbc on jbc.position_id = pd.id --unique
left join job_app ja on ja.position_description_id = pd.id --multiple
left join job_app_rotten jar on jar.position_description_id = pd.id --multiple
left join job_owners jo on jo.position_id = pd.id --unique
left join job_reopen jro on jro.position_id = pd.id --unique
left join placement_profit pp on pp.position_description_id = pd.id --multiple
where 1=1
and pd.insert_timestamp > '2020-08-01' --filter date for created jobs
order by pd.id