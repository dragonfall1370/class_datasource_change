with job_brand as (select tgc.position_id
	, tgc.team_group_id
	, tg.name as brand
	from team_group_position tgc
	left join (select * from team_group where group_type = 'BRAND') tg on tg.id = tgc.team_group_id
	where team_group_id in (1125) --1125-Professionals | 1123-CA
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
, office_in_charge as (select a.additional_id as job_id
	, a.form_id
	, a.field_id
	, a.field_value
	, cf.join_field_translate as office_in_charge
	from additional_form_values a
	left join (select * from custom_field where join_field_id = 11313) cf on cf.join_field_value = a.field_value
	where field_id = 11313
	and field_value <> ''
	) --select * from office_in_charge

--GET JOB CONTACTS
, job_contact as (select pd.id
	, pd.contact_id
	, o.office_in_charge
	, jb.brand
	, row_number() over(partition by pd.contact_id, o.office_in_charge order by pd.id, o.office_in_charge) as rn
	from position_description pd
	join job_brand jb on jb.position_id = pd.id
	left join office_in_charge o on o.job_id = pd.id
	--where o.office_in_charge is not NULL --bind condition if any
	) --select * from job_contact where office_in_charge is not NULL and contact_id = 24407


--JOB HAVING INTERVIEW WITHIN 1 MONTH
, int_1month as (select pc.id, pc.candidate_id, pc.position_description_id
	, pd.contact_id
	, pc.associated_date
	, pc.interview1_date
	, pc.status
	, coalesce(pc.interview1_date, pc.associated_date) as filter_date--used for data import
	, pc.last_stage_date
	from position_candidate pc
	left join position_description pd on pd.id = pc.position_description_id
	where 1=1
	and pc.status > 102 --SENT stage
	--and last_stage_date between coalesce(interview1_date, associated_date) and coalesce(interview1_date, associated_date) + interval '1 month'
	and coalesce(pc.interview1_date, pc.associated_date) between current_timestamp - interval '1 month' and current_timestamp
	)
	
--EXCLUDED CONDITIONS
---placed date within 6 months and start date within 1 month
, placed_condition as (select opi.offer_id
	, pc.candidate_id
	, pc.position_description_id
	, pd.contact_id
	, opi.placed_date, opi.start_date
	, current_timestamp - interval '6 months' as place_within
	, current_timestamp - interval '1 month' as start_within
	from offer_personal_info opi
	join offer o on o.id = opi.offer_id
	join position_candidate pc on pc.id = o.position_candidate_id
	left join position_description pd on pd.id = pc.position_description_id
	where 1=1
	and opi.placed_date between now() - interval '6 months' and now()
	and opi.start_date between now() - interval '1 month' and now()
	--and now() - interval '3 months' --considered if no of records are low
	and pc.position_description_id in (select id from job_contact) --bind conditions from job brand
	)
	
--MAIN SCRIPT
select --jc.contact_id, 
c.email as "Email"
, concat_ws(chr(10), com.name, concat_ws(' ', c.last_name, c.first_name, '様')) as "Salutation"
, office_in_charge as "Unit"
, cl.state as "Area"
, 'プロフェッショナル事業本部' as "Region"
, '' as "Concernnumber"
, 'プロフェッショナル' as "Field1"
, 'Professional' as "Field2"
, com.name as "Field3"
, '' as "Field4"
, '' as "Field5"
from job_contact jc
join contact c on c.id = jc.contact_id
left join company com on com.id = c.company_id
left join common_location cl on cl.id = c.current_location_id
where 1=1
and nullif(c.email, '') is not NULL
and jc.rn = 1 --get distinct job office in charge
and jc.contact_id in (select distinct contact_id from int_1month) --14-filter conditions
and jc.contact_id not in (select distinct contact_id from placed_condition) --15-filter conditions
--and jc.contact_id = 24407
order by jc.contact_id