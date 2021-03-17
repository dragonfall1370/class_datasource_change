with cand_brand as (select tgc.candidate_id
	--, tgc.team_group_id
	, string_agg(tg.name, ',') as brand
	from team_group_candidate tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	--where team_group_id in (1125, 1123)
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
	and pc.insert_timestamp > '2020-08-01'
	)

, cand_activity as (select candidate_id
	, max(insert_timestamp) as last_activity_comment
	from activity_candidate
	group by candidate_id
	)

, placement_profit as (select o.id
	, pc.candidate_id
	, coalesce(o.projected_profit, o.profit, 0) as placement_profit
	from offer o
	left join position_candidate pc on pc.id = o.position_candidate_id
	where 1=1
	and pc.status > 300 --placements
	)
	
select 
	c.id as "Candidate ID"
	, cbr.brand as "Brand"
	, cbc.branch as "Branch"
	, c.insert_timestamp as "Candidate entry date (NCAD)"
	, coalesce(ja.time_to_sent, 0) as "Time to CV sent (CV''s sent)"
	, coalesce(ja.time_to_reject, 0) as "Time to reject"
	, coalesce(ja.rotting_time, 0) as "Rotting time (status update)"
	, ja.booked_date as "Placement dates"
	, ja.offer_date as "Offer dates"
	, ja.interview1_date as "1st interview dates"
	, ja.interview2_date as "2nd+ interview dates"
	, cs.name as "Source"
	, ua.name as "Sourcer"
	, ca.last_activity_comment as "Last activity comment"
	, ce.last_activity_date as "Last activity date"
	, coalesce(pp.placement_profit, 0) as "Placed fee amount (GP)"
from candidate c
left join job_app ja on ja.candidate_id = c.id
left join cand_brand cbr on cbr.candidate_id = c.id
left join cand_branch cbc on cbc.candidate_id = c.id
left join candidate_source cs on cs.id = c.candidate_source_id
left join user_account ua on ua.id = c.source_contact_id
left join candidate_extension ce on ce.candidate_id = c.id
left join cand_activity ca on ca.candidate_id = c.id
left join placement_profit pp on pp.candidate_id = c.id
where 1=1
and c.deleted_timestamp is NULL
and c.insert_timestamp > '2020-08-01'
order by c.id