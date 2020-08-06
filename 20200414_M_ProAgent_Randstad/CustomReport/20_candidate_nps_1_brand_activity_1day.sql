with cand_brand as (select tgc.candidate_id
	, tgc.team_group_id
	, tg.name as brand
	from team_group_candidate tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125, 1123)
	)

/* 
ACTIVE VALUE: active=0-passive 1-active 2-donotcontact 3-blacklist 
MET-NOT MET: status=1-met 2-notmet

select kc.id
, kc.kpi_category_id
, kct.title
, kc.kpi_library_id
, kl.kpi_name
from kpi_criteria kc
left join kpi_library kl on kl.id = kc.kpi_library_id
left join kpi_category kct on kct.id = kc.kpi_category_id
where kc.kpi_library_id = 34

*/

--KPI library in 'CA - NCAD' or 'NCAD'
, kpi_lib_name as (select *
	from kpi_library_alt_name
	where alt_name ilike '%CAD%'
	and language = 'en')

, kpi_action as (select *
	from kpi_library
	where id in (select kpi_library_id from kpi_lib_name)
	) --select * from kpi_action

, cand_activity as (select id as activity_id
		, candidate_id
		, insert_timestamp as lastest_activity_date
		, row_number() over(partition by candidate_id order by insert_timestamp desc) rn
		, kpi_action
		from activity
		where 1=1
		and candidate_id > 0
		and kpi_action in (select id::text from kpi_action)
		) --select * from cand_activity

--MAIN SCRIPT
select id
, first_name
, last_name
, first_name_kana
, last_name_kana
, c.email as primary_email
, cand_brand.brand
, ca.lastest_activity_date
, case status
		when 1 then 'MET'
		when 2 then 'NOT MET'
		else NULL end as met_notmet
, case active
		when 0 then 'Active'
		when 1 then 'Passive'
		when 2 then 'Do not contact'
		when 3 then 'Blacklist'
		else NULL end as active
from candidate c
join cand_brand on cand_brand.candidate_id = c.id
join (select * from cand_activity where rn=1) ca on ca.candidate_id = c.id
where 1=1
and c.deleted_timestamp is NULL
and c.active in (0, 1) --status active or passive
and c.status in (1) --only MET candidates
and (ca.lastest_activity_date + interval '24 hours')::date = now()::date
order by id