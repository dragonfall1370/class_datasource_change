with job_brand as (select t.position_id
	--, t.team_group_id
	, string_agg(tg.name,',') as brand
	from team_group_position t
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = t.team_group_id
	where team_group_id in (1125, 1124, 1123) --Professionals, 障がい者, CA
	group by t.position_id
	) --select * from job_brand where position_id = 249389
	
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
	where 1=1
	--and insert_timestamp > '20208-08-01' --Filter date to limit the job app counts
	and pc.insert_timestamp between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval --current week
	) --select * from job_app

, last_activity as (select job_id
	, max(insert_timestamp) as last_activity_date
	from activity_job
	group by job_id)

, job_app_rotten as (select pc.id as job_app_id
	, pc.candidate_id
	, pc.position_description_id
	, pc.last_stage_date
	, ce.last_activity_date
	, DATE_PART('day', ce.last_activity_date - pc.last_stage_date) as rotten_days
	from position_candidate pc
	left join candidate_extension ce on ce.candidate_id = pc.candidate_id
	where 1=1
	--and pc.insert_timestamp > '20208-08-01'
	and pc.insert_timestamp between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval --current week
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
	, coalesce(o.projected_profit, 0) as placement_profit --removing profit, if contract, projected_profit >= profit
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
	, coalesce(ja.hire_date, pd.head_count_close_date)::date "Close date / placement date"
	, coalesce(pe.last_activity_date, la.last_activity_date)::date as "Last activity date"
	, jro.reopen_date::date as "Re-open date (if applicable)"
	, case when pd.published_date is not NULL then 'Yes'
			else 'No' end as Posted
	, pd.published_date as "Posting date"
	--, c.projected_profit * coalesce(pd.head_count, 1) as "Forecast Fee amount (GP)" -- from position_description
	, case when pd.position_type = 1 then coalesce(pd.forecast_annual_fee, 0)
		when pd.position_type > 1 then coalesce(c.projected_profit, 0) end as "Forecast Fee amount (GP)"
	, coalesce(pp.placement_profit, 0) as "Job Fee amount (GP)"
from position_description pd
join job_brand jbr on jbr.position_id = pd.id --unique
left join job_branch jbc on jbc.position_id = pd.id --unique
left join position_extension pe on pe.position_id = pd.id --unique
left join last_activity la on la.job_id = pd.id --unique
left join job_app ja on ja.position_description_id = pd.id --multiple
left join job_app_rotten jar on jar.job_app_id = ja.job_app_id --following by job_app_id instead 20201211
left join job_owners jo on jo.position_id = pd.id --unique
left join job_reopen jro on jro.position_id = pd.id --unique
left join placement_profit pp on pp.position_description_id = pd.id --multiple
left join compensation c on c.position_id = pd.id --added 20200928
where 1=1
--and pd.insert_timestamp > '2020-08-01' --filter date for created jobs
and pd.insert_timestamp between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval --current week
--and pd.insert_timestamp between date_trunc('month', current_timestamp) and date_trunc('month', current_timestamp) + interval '1 month' - '1 day'::interval --current month
--and pd.id = 249389
order by pd.id desc