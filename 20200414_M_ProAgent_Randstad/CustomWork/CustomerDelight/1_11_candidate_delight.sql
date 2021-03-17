with cand_brand as (select tgc.candidate_id
		--, tgc.team_group_id
		, string_agg(tg.name, ',') as brand
	from team_group_candidate tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125, 1123) --Professionals, CA, 1124-障がい者
	group by tgc.candidate_id
	)
	
, cand_activity as (select candidate_id
	, max(insert_timestamp) as last_activity_date
	from activity_candidate
	group by candidate_id
	)

--Candidate source selected
, selected_source as (select id, name
	from candidate_source
	where id in (29255,29297,29234,29175,29260,29300,29299,29091,29308,29307,29304,29310,29311,29305,29312,29173,29314,29315,29316,29317)
	)

--CF field value
, custom_field as (select cffv.form_id as join_form_id
	, cffv.field_id as join_field_id
	, cfl.translate as join_field_translate
	, cffv.field_value as join_field_value
	from configurable_form_language cfl
	left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
	where 1=1
	and cfl.language = 'en' --input language
	--and cffv.field_id = 11312 --input field
	order by cffv.field_value::int)

--CF Office in charge
, office_in_charge as (select a.additional_id as candidate_id
	, a.form_id
	, a.field_id
	, a.field_value
	, cf.join_field_translate as office_in_charge
	from additional_form_values a
	left join (select * from custom_field where join_field_id = 11312) cf on cf.join_field_value = a.field_value
	where field_id = 11312
	--and field_value <> '' --removed conditions if any
	) --select * from office_in_charge

--CF Gender --at present
, gender as (select a.additional_id as candidate_id
	, a.form_id
	, a.field_id
	, a.field_value
	, cf.join_field_translate as gender
	from additional_form_values a
	left join (select * from custom_field where join_field_id = 11304) cf on cf.join_field_value = a.field_value
	where field_id = 11304
	--and field_value <> '' --removed conditions if any
	) --select * from gender

--EXCLUDED CONDITIONS
---used in #12 delight candidate
, int_1month as (select id, candidate_id, position_description_id
	, associated_date
	, interview1_date
	, status
	, coalesce(interview1_date, associated_date) as filter_date--used for data import
	, last_stage_date
	from position_candidate
	where 1=1
	and status > 102 --SENT stage
	--and last_stage_date between coalesce(interview1_date, associated_date) and coalesce(interview1_date, associated_date) + interval '1 month'
	and coalesce(interview1_date, associated_date) between current_timestamp - interval '1 month' and current_timestamp
	)
	
---placed date within 6 months and start date within 1 month
, placed_condition as (select opi.offer_id
	, pc.candidate_id
	, opi.placed_date, opi.start_date
	, current_timestamp - interval '6 months' as place_within
	, current_timestamp - interval '1 month' as start_within
	from offer_personal_info opi
	join offer o on o.id = opi.offer_id
	join position_candidate pc on pc.id = o.position_candidate_id
	where 1=1
	and opi.placed_date between now() - interval '6 months' and now()
	and opi.start_date between now() - interval '1 month' and now()
	--and now() - interval '3 months' --considered if no of records are low
	)

--MAIN SCRIPT
select --c.id, 
	c.email
	, concat_ws(' ', c.last_name, c.first_name, '様') as "Salutation"
	, o.office_in_charge as "Unit"
	, cl.state as "Area"
	, 'プロフェッショナル事業本部' as "Region"
	, '' as "Concernnumber"
	, 'プロフェッショナル' as "Field1"
	, 'Professional' as "Field2"
	, date_part('year', age(date_of_birth)) as "Field3"
	, gd.gender as "Field4"
	--, c.note as "Field5"
	, '' as "Field5" --changed requirement on 2020-10-20
from candidate c
join cand_brand cb on cb.candidate_id = c.id
join cand_activity ca on ca.candidate_id = c.id
left join office_in_charge o on o.candidate_id = c.id
left join gender gd on gd.candidate_id = c.id
left join common_location cl on cl.id = c.current_location_id --current location
where 1=1
and c.active in (0, 1) --status: active or passive
and c.status in (1) --only MET candidates
and c.insert_timestamp between current_timestamp - interval '2.5 months'  and current_timestamp - interval '1.5 months'
and ca.last_activity_date between current_timestamp - interval '1 months' and current_timestamp
and c.candidate_source_id in (select id from selected_source)
and c.id not in (select distinct candidate_id from int_1month) --12-filter conditions
and c.id not in (select distinct candidate_id from placed_condition) --13-filter conditions
order by c.id