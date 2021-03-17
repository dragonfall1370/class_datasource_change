with consultant_team as (select tgu.user_id
		, string_agg(tg.name, ',') as team
	from team_group_user tgu
	left join (select * from team_group where group_type = 'TEAM') tg on tg.id = tgu.team_group_id
	where 1=1
	--and team_group_id in (1125, 1124, 1123) --Professionals, 障がい者, CA
	group by tgu.user_id
	)

, kpi_activated as (
	select id
	, kpi_name
	, coalesce(nullif(kln.alt_name, ''), kl.kpi_name) as kpi_name_final
	, seq
	, coalesce(activated, 0) as is_active
	, case when related_type = 1 then 'Candidate'
			when related_type = 2 then 'Contact'
			when related_type = 3 then 'Company'
			when related_type = 4 then 'Job'
			when related_type = 5 then 'Application'
			when related_type = 6 then 'Deal'
			when related_type = 7 then 'Job lead'
			else 'Other'
		end as entity
	, case when id in (57, 58, 59, 82) then 'fee'
		else 'action' end as kpi_type
	from kpi_library kl
	left join (select * from kpi_library_alt_name where language = 'en') kln on kln.kpi_library_id = kl.id
	where activated = 1 --activated kpi
	and related_type <> 6 --exclude 'Deal'
	order by kl.seq
)

, curency_code as (
	select c.key
	  , case when coalesce(ce.value, 0) = 0 THEN 1 ELSE 1/ce.value END::double precision as value
	from currencies c
	join currencies_exchange ce ON c.code = ce.code_to
	where ce.code_from = 'JPY'
)

--TARGERTS
, targets as (select ka.user_account_id as consultant_key
			, ka.kpi_library_id as kpi_lib_key
			, ka.time_frame
			, ka.target_score
			, ka.start_date as org_start_date
			, ka.end_date as org_end_date
			, case when ka.end_date is NULL then date_trunc('week', current_timestamp)
					else ka.start_date end as start_date
			, case when ka.end_date is NULL then date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval --current week end date
					else ka.end_date end as end_date
		--, coalesce(ka.end_date, ka.start_date + '1 month'::interval) as end_date --added 1 month if end_date is NULL | remove this line
			, case when kpi_type = 'fee' then round((ka.target_score * coalesce(cc.value, 1))::integer, 1)
					else ka.target_score end as target
			, kpi_type
			, ua.name
			, row_number() over(partition by ka.user_account_id, ka.kpi_library_id order by ka.start_date, ka.end_date) as rn
		from kpi_assignment ka
				join user_account ua on ka.user_account_id = ua.id
				join kpi_activated k on k.id = ka.kpi_library_id
				left join curency_code cc on cc.key = ka.currency_type
		where ua.id <> -10 
		and ua.deleted_timestamp is null
		and ka.target_score is not NULL
		--and ua.id = 28983 --checking 1 consultant
		--and ka.end_date is not NULL --filter with end_date
		--and ka.end_date >= now() --only valid timeframe
		and (ka.end_date is NULL or (ka.start_date between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval)) --current week view
		and ka.time_frame = 1 --week view
		order by k.kpi_type, ka.user_account_id
		) 	
		--select * from targets where kpi_lib_key = 34 order by consultant_key, kpi_lib_key		
		--select * from targets where consultant_key = 28984 order by consultant_key, kpi_lib_key

/*
select *
from user_account
where name = 'Chihiro Tochisako'
*/

, cand_owner as (select id as candidate_id
			, (json_array_elements(candidate_owner_json::json)->>'ownerId')::int as candidate_owner_id
	from candidate 
	where candidate_owner_json is not null)

, tmp_candidate_owner as (select candidate_id
	, string_agg(ua.name, ',' order by ua.id) as candidate_owners
	from cand_owner co
	left join user_account ua on ua.id = co.candidate_owner_id
	where 1=1
	and ua.deleted_timestamp is NULL
	group by candidate_id
	) --select * from tmp_candidate_owner

, tmp_job_owner as (select pac.position_id AS job_id
	, pac.user_id ::int AS user_id
	, coalesce ((1/tmp_1.no_job_owner) * 100, 100) AS shared
	from position_agency_consultant pac
	left join (select position_id, count(user_id) as no_job_owner
				from position_agency_consultant pac
				group by position_id) tmp_1 on pac.position_id = tmp_1.position_id
	)

--ACTUAL ACTIONS
, kca as (select unnest(string_to_array(kpi_related_lib, ','))::int as kpi_lib_key
				, kca.candidate_id
				, coalesce(kca.position_id, aj.job_id) as position_id
				, coalesce(kca.contact_id, acon.contact_id) as contact_id
				, coalesce(kca.company_id, ac.company_id) as company_id
				, kca.user_account_id
				, kca.insert_timestamp		
				, kca.comment_id
				, kca.id as comment_action_id
			from kpi_comment_action kca
			left join activity a on (a.id = kca.comment_id or a.id =  kca.task_id)
			left join activity_job as aj on aj.activity_id = kca.comment_id
			left join activity_company as ac on ac.activity_id = kca.comment_id
			left join activity_contact as acon on acon.activity_id = kca.comment_id
			where kpi_related_lib is not null
			and kpi_related_lib <> ''
			and kca.insert_timestamp between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval --current week view
			)

, actual_actions as (select consultant_key
			, kpi_lib_key
			, count(distinct comment_action_id) as actual
			--, date_key
			--, shortlisted_key
			--, comment_action_id
			--, candidate_key
			--, job_key
			--, contact_key
			--, company_key
		from
			(select kca.user_account_id as consultant_key
				, kpi_lib_key
				--, kpi_related_lib
				, fu_convert_time_zone(kca.insert_timestamp, '', ua.timezone) AS date_key
				, coalesce(kca.candidate_id, -1) as candidate_key
				, coalesce(kca.position_id, -1) as job_key
				, coalesce(kca.contact_id, -1) as contact_key
				, coalesce(kca.company_id, -1) as company_key
				, coalesce(pd.position_category, 3) as position_category
			, pc.shortlisted_user_id as shortlisted_key
			, kca.comment_action_id
			from kca
			join user_account ua ON kca.user_account_id = ua.ID
			left join position_description as pd on pd.id = kca.position_id
			left join position_candidate AS pc ON pc.position_description_id = kca.position_id AND pc.candidate_id = kca.candidate_id
			where ua.id <> -10 
			and ua.deleted_timestamp is null
			and kca.user_account_id IS NOT NULL
			and kpi_lib_key in (select id from kpi_activated where kpi_type <> 'fee')
			) as b
		group by consultant_key, kpi_lib_key
		) --select * from actual_actions where consultant_key = 29099 and kpi_lib_key = 77 --check actual actions

--TMP TABLES
, user_currency_timezone as
	(
		SELECT upper(c.code) as currency_type
		, timezone 
		FROM user_account ua 
		JOIN currencies c on ua.currency_type = c.key
		WHERE ua.id = -10
		LIMIT 1
	)
	
, tmp_ce_user AS 
	(-- convert rate of all currencies into admin currency setting
		SELECT c.key AS key_from
		       , CASE WHEN COALESCE(ce.value, 0) = 0 THEN 1 ELSE 1/ce.value END::DOUBLE PRECISION AS value 
			   , c.code
		  FROM currencies c 
		  INNER JOIN currencies_exchange ce ON c.code = ce.code_to
		  INNER JOIN user_currency_timezone uc on uc.currency_type = ce.code_from
	)
	
, tmp_user_status as
	(	
		SELECT ua.id as consultant_id
		, ua.name as consultant_name
		, case when ua.locked_user = 0 then 'Active'
		when ua.locked_user = 1 then 'Inactive'
		else 'Other' end as consultant_status
	    , array_to_string(array_agg(tg_team.name), ','::text)  AS team_name
	    , array_to_string(array_agg(tg_brand.name), ','::text)  AS brand_name
	    , array_to_string(array_agg(tg_branch.name), ','::text)  AS branch_name
		FROM user_account ua
		LEFT JOIN team_group_user tgu ON tgu.user_id = ua.id
		LEFT JOIN team_group tg_team ON tg_team.id = tgu.team_group_id AND tg_team.group_type::text = 'TEAM'::TEXT
		LEFT JOIN team_group tg_brand ON tg_brand.id = tgu.team_group_id AND tg_brand.group_type::text = 'BRAND'::TEXT
		LEFT JOIN team_group tg_branch ON tg_branch.id = tgu.team_group_id AND tg_branch.group_type::text = 'BRANCH'::text
		WHERE ua.system_admin = 0 
		AND ua.deleted_timestamp IS NULL 
		GROUP BY ua.id
	)
	
, tmp_company_owner as
	(
		SELECT c.id AS company_id
		, c.name AS company_name
		, (((SELECT array_to_string(array_agg(user_account.name), ','::text) AS array_to_string
        FROM user_account
        WHERE user_account.deleted_timestamp IS NULL AND (user_account.id = ANY (c.company_owner_ids)))))::character varying(200) AS company_owners
     FROM company c
	)

, tmp_contact_owner as
	(
		SELECT c.id AS contact_id
		, c.contact_owner_ids
		, concat_ws(' ', coalesce(c.first_name || ' ', ''), coalesce(c.middle_name || ' ', ''), coalesce(c.last_name || ' ', '')) AS contact_name
		, ((( SELECT array_to_string(array_agg(user_account.name), ', '::text) as array_to_string
          FROM user_account
          WHERE user_account.deleted_timestamp IS NULL AND (user_account.id = ANY (c.contact_owner_ids)))))::character varying(200) AS contact_owners
    FROM contact c
	)
	
--ACTUAL REVENUE
, tmp_placed as 
	(
		SELECT case when o.position_type = 1 THEN 57 ELSE 58 END AS kpi_lib_key
		, pc.position_description_id AS job_id
		, pd.name AS job_name
		, pc.id AS app_id
		, pc.candidate_id AS candidate_id
		, opi.first_name||' '|| opi.last_name as candidate_name
		, tcao.candidate_owners
		, o.id AS offer_id
		, CASE WHEN ors.user_id IS NOT NULL THEN ors.user_id ELSE tjo.user_id END AS consultant_id
		, CASE WHEN ors.user_id IS NOT NULL THEN ors.shared ELSE tjo.shared END AS shared
		, CASE 
			WHEN ors.user_id IS NOT NULL THEN coalesce(ors.amount * tmp_ce_user.value, 0) 
			WHEN tjo.user_id IS NOT NULL THEN coalesce(o.projected_profit * tjo.shared * tmp_ce_user.value, 0)
			ELSE 0 END AS fee
		, CASE WHEN ors.user_id IS NOT NULL THEN 'Profit Split' ELSE 'Job Owner' END AS fee_source
		, o.currency_type AS currency_type
		, o.position_type AS job_type
		, COALESCE(opi.placed_date, pc.hire_date, pc.placed_date ) AS action_date
		, opi.client_company_id AS company_id
		, opi.client_company_name AS company_name
		, tco.company_owners
		, opi.client_contact_id AS contact_id
		, opi.client_contact_name AS contact_name
		, tcono.contact_owners
		, DATE_TRUNC('WEEK', coalesce(opi.placed_date, pc.hire_date, pc.placed_date ))::DATE AS first_of_week
		,(DATE_TRUNC('WEEK', coalesce(opi.placed_date, pc.hire_date, pc.placed_date )) + interval '1 week' - interval '1 day')::DATE AS last_of_week
		, extract(WEEK from coalesce(opi.placed_date, pc.hire_date, pc.placed_date)) AS week_name
		from offer o
		left join offer_revenue_split ors ON o.id = ors.offer_id 
		left join position_candidate pc ON  o.position_candidate_id = pc.id 
		left join tmp_ce_user ON o.currency_type = tmp_ce_user.key_from
		left join offer_personal_info opi ON o.id = opi.offer_id
		left join tmp_candidate_owner tcao ON tcao.candidate_id = pc.candidate_id
		left join tmp_job_owner tjo ON tjo.job_id = pc.position_description_id  AND ors.user_id IS NULL
		left join tmp_company_owner tco ON tco.company_id =  opi.client_company_id
		left join tmp_contact_owner tcono ON tcono.contact_id = opi.client_contact_id
		left join candidate c ON c.id = pc.candidate_id 
		left join position_description pd ON pd.id = pc.position_description_id 
		where pc.status >= 300 AND pc.status < 400
		--AND COALESCE(opi.placed_date, pc.hire_date, pc.placed_date) >= (DATE_TRUNC('MONTH', NOW()- INTERVAL '12 months'))
		and coalesce(opi.placed_date, pc.hire_date, pc.placed_date) between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - interval '1 day' --current week view
		and c.deleted_timestamp IS null
	) --select * from tmp_placed

, actual_fee as (
	select consultant_id
		, kpi_lib_key
		, sum(fee) as actual_fee
	from tmp_placed
	group by consultant_id, kpi_lib_key
	) ---select * from actual_fee


--COMBINE ACTUAL ACTIONS AND FEES
, actual as (select *
	, 'action' as actual_type
	from actual_actions
	
	UNION ALL
	select *
	, 'fee' as actual_type
	from actual_fee
	) --select * from actual where consultant_key = 29634
	
--COMPARE TARGETS / ACTUAL
select to_char(current_timestamp, 'YYYY-MM-DD') as "Export Date"
	, to_char(t.start_date, 'YYYY-MM-DD') "Date From"
	, to_char(coalesce(t.end_date, date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval), 'YYYY-MM-DD') "Date To"
	, ct.team "Team"
	--, t.consultant_key
	, t.name "Consultant Name"
	--, t.kpi_lib_key
	, ka.kpi_name_final
	--, t.time_frame
	, t.target::text
	, coalesce(a.actual, 0)::text as actual
	, t.kpi_type
	from targets t
	left join actual a on t.consultant_key = a.consultant_key and t.kpi_lib_key = a.kpi_lib_key
	left join kpi_activated ka on ka.id = t.kpi_lib_key
	left join consultant_team ct on t.consultant_key = ct.user_id
	where 1=1
	and t.rn = 1 --if setting goals from next week
	--and ka.kpi_name_final = 'Placement Based Revenue' --check kpi name
	order by t.name, t.kpi_lib_key


/* AUDIT CHECK AND COMPARE WITH INTELLIGENCE
, a as (
	select t.start_date::date "Date From"
	, coalesce(t.end_date, date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval)::date "Date To"
	, ct.team "Team"
	--, t.consultant_key
	, t.name "Consultant Name"
	--, t.kpi_lib_key
	, ka.kpi_name_final
	--, t.time_frame
	, t.target
	, coalesce(a.actual, 0) as actual
	, t.kpi_type
	from targets t
	left join actual a on t.consultant_key = a.consultant_key and t.kpi_lib_key = a.kpi_lib_key
	left join kpi_activated ka on ka.id = t.kpi_lib_key
	left join consultant_team ct on t.consultant_key = ct.user_id
	where 1=1
	and t.rn = 1 --if setting goals from next week
	--and ka.kpi_name_final = 'Placement Based Revenue' --check kpi name
	--order by t.name, kpi_lib_key
)

select kpi_name_final, count(kpi_name_final) count, sum(target) target, sum(actual) actual
from a
where kpi_name_final = 'Placement Based Revenue'
group by kpi_name_final
*/