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
	and id in (74, 5, 7, 73, 76, 1, 3, 75, 78, 22, 24, 77, 80, 18, 20, 79
	,35 ,37 ,70 ,72 ,69 ,71 ,17 ,61 ,86 ,85 ,34 ,88 ,87 ,60 ,39 ,40 ,81 ,41 
	,63 ,45 ,46 ,47 ,48 ,49 ,50 ,51 ,84 ,52 ,53 ,54 ,55 ,56 ,82 ,57 ,59 ,58 ,64 ,65
	,89 ,90 ,91 ,92 ,93, 94, 95, 57, 58, 59, 82)
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
			, ka.start_date as start_date
			, ka.end_date as end_date	
			--, coalesce(ka.end_date, ka.start_date + '1 month'::interval) as end_date --added 1 month if end_date is NULL | remove this line
			, case when kpi_type = 'fee' then round((ka.target_score * coalesce(cc.value, 1))::integer, 1)
					else ka.target_score end as target
			, kpi_type
			, ua.name
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
		and ka.start_date between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval --current week view
		and ka.time_frame = 1 --week view
		order by k.kpi_type, ka.user_account_id
		) --select * from targets

, cand_owner as (select id as candidate_id
			, (json_array_elements(candidate_owner_json::json)->>'ownerId')::int as candidate_owner_key
	from candidate
	where candidate_owner_json is not null)


, job_owner as (select pac.position_id
		, user_id
		, name as job_owner
		, total_job_owner
		--, row_number() over	(partition by pac.position_id order by name asc) as row_num
	from position_agency_consultant pac
	left join (select position_id, count (user_id) as total_job_owner 
				from position_agency_consultant 
				group by position_id) as c on pac.position_id = c.position_id
	left join user_account ua on pac.user_id = ua.id)


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
			and kpi_lib_key in 
				(5, 1, 22, 18, 35, 37, 70, 72, 69, 71, 17, 61, 34, 60, 39, 40, 41, 63, 45, 46, 47, 48, 49, 50, 51, 52, 53, 54, 55, 56, 64, 65
				, 7, 73, 3, 75, 24, 77, 20, 79, 42, 43, 85, 87, 89 ,90 ,91 ,92 ,93, 94, 95)
			) as b
		group by consultant_key, kpi_lib_key
		) --select * from actual_actions where consultant_key = 29099 and kpi_lib_key = 77 --check actual actions
	
--ACTUAL REVENUE
, tmp_1_rev as (
	select o.position_candidate_id as pcid
	  , ors.amount * cc.value AS pc_amount_1 
	  , ors.user_id as consultant_key_1
	  , ors.offer_id
	from offer_revenue_split ors
	join offer o ON ors.offer_id = o.id
	left join curency_code cc ON o.currency_type = cc.key
	where 1=1
) 
, tmp_a as (select position_id, count(distinct user_id) as cnt from position_agency_consultant group by position_id)

, tmp_b as (
	select pc.id as pcid
	  , pac.user_id as consultant_key_1
	  , pc.shortlisted_user_id as shortlisted_key_1
	from position_candidate pc
	inner join position_agency_consultant pac ON pc.position_description_id = pac.position_id
	WHERE pc.id NOT IN (SELECT pcid from tmp_1_rev)
) 
, tmp_other_item as (
	select o.position_candidate_id as pcid
	  , sum(oii.item_quantity * oii.item_charge) as other_invoice
	from offer_invoice_item oii
	inner join offer o ON oii.offer_id = o.id
	group by o.position_candidate_id
)
, pd_account_temp AS
( 
	SELECT pac.position_id
	  , COALESCE(array_to_string(array_agg(ua.name), '', ''::text), ''''::text) AS agency_consultant_names
	FROM position_agency_consultant pac 
	INNER JOIN user_account ua ON pac.user_id = ua.id
	WHERE ua.deleted_timestamp is null
	GROUP BY position_id
)
, tmp_1_pro as (		
		SELECT * 
		FROM 
		(	SELECT tmp.pcid, tmp.position_id, cid, pc_timestamp, status
				, (projected_profit + COALESCE(tmp_other_item.other_invoice, 0))* cc.value / tmp_a.cnt AS pc_amount_1
				, gross_annual_salary * cc.value AS gross_annual_salary
				, pay_rate * cc.value AS pay_rate
				, projected_charge_rate * cc.value AS projected_charge_rate
				, fee_model_type, position_type, head_count, date_key
				, tmp.position_category
				, tmp.shortlisted_key_1
				, tmp.id
			FROM 
			(	SELECT pc.id AS pcid
				  , pc.position_description_id AS position_id
				  , pc.candidate_id AS cid
				  , pc.shortlisted_user_id as shortlisted_key_1
					, CASE	
						WHEN pc.status >= 300 AND pc.status <= 303 AND COALESCE(opi.placed_date, pc.hire_date) IS NOT NULL THEN COALESCE(opi.placed_date, pc.hire_date) 	 			
						ELSE pc.associated_date END AS pc_timestamp
				  , 300 as status
					, o.projected_profit AS o_projected_profit, o.currency_type AS o_currency_type
					, CASE 
						WHEN o.projected_profit IS NOT NULL THEN o.projected_profit			
						WHEN comp.projected_profit IS NOT NULL THEN comp.projected_profit 
						ELSE 0 
					END AS projected_profit
					, CASE 
						WHEN o.gross_annual_salary IS NOT NULL THEN o.gross_annual_salary			
						WHEN comp.gross_annual_salary IS NOT NULL THEN comp.gross_annual_salary 
						ELSE 0 
					END AS gross_annual_salary
					, CASE
						WHEN o.pay_rate IS NOT NULL THEN o.pay_rate			
						WHEN comp.pay_rate IS NOT NULL THEN comp.pay_rate 
						ELSE 0 
					END AS pay_rate
					, CASE 
						WHEN  o.projected_charge_rate IS NOT NULL THEN o.projected_charge_rate			
						WHEN comp.projected_charge_rate IS NOT NULL THEN comp.projected_charge_rate 
						ELSE 0 
					END AS projected_charge_rate
					, CASE 
						WHEN o.currency_type IS NOT NULL THEN o.currency_type 
						WHEN comp.currency_type IS NOT NULL THEN comp.currency_type		
					END AS currency_type 
					, comp_m.fee_model_type
					, COALESCE(o.position_type, pd.position_type) as position_type
					, pd.head_count
					, COALESCE(opi.placed_date, pc.hire_date, opi.offer_date, pc.offer_date, pc.associated_date) date_key
					, pd.position_category
					, o.id
				FROM position_candidate pc
				LEFT JOIN compensation comp ON pc.position_description_id = comp.position_id		
				LEFT JOIN compensation_fee_model comp_m ON comp_m.compensation_id = comp.id
				JOIN pd_account_temp pat ON pc.position_description_id = pat.position_id
				JOIN position_description pd ON pc.position_description_id = pd.id
				JOIN candidate c ON c.id = pc.candidate_id
				LEFT JOIN offer o ON pc.id = o.position_candidate_id
				LEFT OUTER JOIN offer_personal_info opi ON o.id = opi.offer_id
				WHERE pc.status > 200 AND pc.status <= 303
					AND c.deleted_timestamp IS NULL 
					AND pc.rejected_date IS NULL
					AND pd.floated_job = 0	
					AND ((comp.projected_profit >= 0 AND comp.currency_type IS NOT NULL) OR (o.projected_profit >= 0 AND o.currency_type IS NOT NULL))
					AND (pc.id IN (select distinct pcid from tmp_1_rev) OR pc.id NOT IN (SELECT position_candidate_id from offer))
			) tmp 
			LEFT JOIN curency_code cc ON tmp.currency_type = cc.key
			LEFT JOIN tmp_a ON tmp_a.position_id = tmp.position_id
			LEFT JOIN tmp_other_item ON tmp_other_item.pcid = tmp.pcid
		) tmp2 		
)
, tmp_1 as (
	select tmp_1_pro.pcid
		, tmp_1_pro.position_id
		, tmp_1_pro.cid
		, tmp_1_pro.pc_timestamp
		, tmp_1_pro.status
		, tmp_1_pro.pc_amount_1
		, tmp_1_pro.gross_annual_salary
		, tmp_1_pro.pay_rate
		, tmp_1_pro.projected_charge_rate
		, tmp_1_pro.fee_model_type
		, tmp_1_pro.position_type
		, tmp_1_pro.head_count
		, tmp_1_pro.date_key
		, tmp_1_pro.position_category
		, tmp_1_pro.id
		, coalesce(tmp_1_rev.pc_amount_1, 0) as pc_amount --https://hrboss.atlassian.net/browse/DATA-1053
		, coalesce(tmp_1_rev.consultant_key_1, tmp_b.consultant_key_1, -1000) as consultant_key
		, coalesce(tmp_1_pro.shortlisted_key_1, tmp_b.shortlisted_key_1, -1) as shortlisted_key
	from tmp_1_pro 
	left outer join tmp_1_rev ON tmp_1_pro.pcid = tmp_1_rev.pcid and tmp_1_pro.id = tmp_1_rev.offer_id
	left outer join tmp_b ON tmp_1_pro.pcid = tmp_b.pcid
	where 1=1
	and tmp_1_pro.date_key between date_trunc('week', current_timestamp) and date_trunc('week', current_timestamp) + interval '1 week' - '1 day'::interval --current week view
)
, tmp_3 as (select *
		, rank() over (partition by position_id order by status desc, pc_amount desc ) as ord	
	from tmp_1
)
, tmp_4 as (
	select tmp_3.position_id as job_key
	    , cid as candidate_key
		, pc_amount as actual 
		, CASE WHEN fee_model_type = 3 THEN 59
			WHEN position_type = 1 THEN 57 --NFI / Fees | Permanent
			ELSE 58 --NFI / Margin | Contract & Temporary
		END as kpi_lib_key
		, tmp_3.consultant_key
		, tmp_3.shortlisted_key
		, date_key
		, tmp_3.position_category
		, tmp_3.id as offer_id
		, pcid
		, pc_amount
	from tmp_3
	INNER JOIN user_account ua ON tmp_3.consultant_key = ua.ID
	--https://hrboss.atlassian.net/browse/DATA-1029
	--where pc_amount > 0
)

, actual_fee as (
	select consultant_key
		, kpi_lib_key
		, sum(actual) as actual_fee
	from tmp_4
	group by consultant_key, kpi_lib_key
	)
	
--COMBINE ACTUAL ACTIONS AND FEES
, actual as (select *
	, 'action' as actual_type
	from actual_actions
	
	UNION ALL
	select *
	, 'fee' as actual_type
	from actual_fee
	)
	
--COMPARE TARGETS / ACTUAL
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
--and ka.kpi_name_final = 'CA - CV Sent' --check kpi name
--order by t.name, kpi_lib_key