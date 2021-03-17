with cand_brand as (select tgc.candidate_id
		--, tgc.team_group_id
		, string_agg(tg.name, ',') as brand
	from team_group_candidate tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125, 1124, 1123) --Professionals, 障がい者, CA
	group by tgc.candidate_id
	)
	
, cand_branch as (select b.record_id as candidate_id
		--, b.branch_id
		, string_agg(tg.name, ',') as branch
	from branch_record b
	left join (select * from team_group where group_type = 'BRANCH') tg on tg.id = b.branch_id
	where 1=1
	and record_type = 'candidate'
	group by b.record_id
	)
/*	
, job_app as (select id as job_app_id
	, pc.candidate_id
	, pc.position_description_id
	, pc.associated_date
	, pc.rejected_date
	, pc.sent_date
	, pc.hire_date booked_date --taken from position_candidate as actioned date
	, pc.offer_date
	, pc.interview1_date
	, pc.interview2_date
	, case when rejected_date is not null then DATE_PART('day', c.last_activity_date - pc.rejected_date)
			else 0 end as time_to_reject
	, coalesce(DATE_PART('day', current_timestamp - c.last_activity_date), 0) as rotting_time
	, case when sent_date is not null then DATE_PART('day', pc.sent_date - pc.associated_date)
			else 0 end as time_to_sent
	from position_candidate pc
	left join candidate_extension c on c.candidate_id = pc.candidate_id
	where 1=1
	--and pc.insert_timestamp > '2020-08-01'
	)
*/	
, job_app_latest as (select pc.candidate_id
		, max(status) max_status
		, max(pc.associated_date) max_associated_date
		, max(pc.rejected_date) max_rejected_date
		, max(pc.sent_date) max_sent_date
		, max(pc.hire_date) max_booked_date --taken from position_candidate as actioned date
		, max(pc.offer_date) max_offer_date
		, max(pc.interview1_date) max_interview1_date
		, max(pc.interview2_date) max_interview2_date
	from position_candidate pc
	group by pc.candidate_id)

, job_app as (select j.candidate_id
		, j.max_associated_date
		, j.max_rejected_date
		, j.max_sent_date
		, j.max_booked_date
		, j.max_offer_date
		, j.max_interview1_date
		, j.max_interview2_date
		, case when max_rejected_date is not null then DATE_PART('day', c.last_activity_date - j.max_rejected_date)
				else 0 end as time_to_reject
		, coalesce(DATE_PART('day', current_timestamp - c.last_activity_date), 0) as rotting_time
		, case when max_sent_date is not null then DATE_PART('day', j.max_sent_date - j.max_associated_date)
				else 0 end as time_to_sent
	from job_app_latest j
	join candidate_extension c on c.candidate_id = j.candidate_id
	) --select * from job_app

, cand_activity as (select ac.candidate_id
	, ac.activity_id
	, a.content as candidate_comment
	, ac.insert_timestamp
	, row_number() over(partition by ac.candidate_id order by ac.insert_timestamp desc) as rn
	from activity_candidate ac
	left join (select id, candidate_id, content from activity) a on a.id = ac.activity_id
	)

, placement_profit as (select o.id
	, pc.candidate_id
	, coalesce(o.projected_profit, 0) as placement_profit
	from offer o
	left join position_candidate pc on pc.id = o.position_candidate_id
	where 1=1
	and pc.status >= 300 --placements
	)
	
select 
	c.id::text as "Candidate ID"
	, cbr.brand as "Brand"
	, cbc.branch as "Branch"
	, to_char(c.insert_timestamp, 'YYYY-MM-DD') as "Candidate entry date (NCAD)"
	, coalesce(ja.time_to_sent, 0)::text as "Time to CV sent (CV''s sent)"
	--, coalesce(ja.time_to_reject, 0) as "Time to reject"
	, coalesce(ja.rotting_time, 0)::text as "Rotting time (status update)"
	, to_char(ja.max_booked_date, 'YYYY-MM-DD') as "Placement dates"
	, to_char(ja.max_offer_date, 'YYYY-MM-DD') as "Offer dates"
	, to_char(ja.max_interview1_date, 'YYYY-MM-DD') as "1st interview dates"
	, to_char(ja.max_interview2_date, 'YYYY-MM-DD') as "2nd+ interview dates"
	, cs.name as "Source"
	, ua.name as "Sourcer"
	, ca.candidate_comment as "Last activity comment"
	, to_char(coalesce(ce.last_activity_date, ca.insert_timestamp), 'YYYY-MM-DD') as "Last activity date"
	--, coalesce(pp.placement_profit, 0) as "Placed fee amount (GP)"
from candidate c
	join cand_brand cbr on cbr.candidate_id = c.id
	join cand_branch cbc on cbc.candidate_id = c.id
	left join job_app ja on ja.candidate_id = c.id
	left join candidate_source cs on cs.id = c.candidate_source_id
	left join user_account ua on ua.id = c.source_contact_id
	left join candidate_extension ce on ce.candidate_id = c.id
	left join (select * from cand_activity where rn=1) ca on ca.candidate_id = c.id --latest activity
	--left join placement_profit pp on pp.candidate_id = c.id
where 1=1
and c.deleted_timestamp is NULL
--and c.insert_timestamp::date = now()::date
--and c.insert_timestamp between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '6 days' --current week
order by c.id