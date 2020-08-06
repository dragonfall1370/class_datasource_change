---ADVANDCED SEARCH: OPEN TO HP = OPEN | HP PUBLIC APPROVAL = CURRENTLY APPLYING
with open_to_hp as (select *
	from additional_form_values
	where field_id = 1048
	and field_value = '1') --公開 | Open

, hp_approval as (select *
	from additional_form_values
	where field_id = 11317
	and field_value = '1' --申請中 | currently applying
	and additional_id in (select additional_id from open_to_hp))

, job_owners as (select p.position_id
	, string_agg(u.name, ', ') as job_owners
	from position_agency_consultant p
	left join user_account u on p.user_id = u.id
	group by p.position_id)

, office_in_charge as (select cffv.form_id as join_form_id
	, cffv.field_id as join_field_id
	, cfl.translate as join_field_translate
	, cffv.field_value as join_field_value
	from configurable_form_language cfl
	left join configurable_form_field_value cffv on cffv.title_language_code = cfl.language_code
	where cfl.language = 'ja' --input language
	and cffv.field_id = 11313 --input field
	)


, division as (select a.additional_id
	, join_field_translate as division
	from additional_form_values a
	left join office_in_charge o on a.field_value = o.join_field_value
	where field_id = 11313
	--and position(',' in field_value ) > 1 --check if multiple values
	) --office in charge 

--MAIN SCRIPT
select 
pd.id
, pd.name
, '公開' as open_to_hp
, '申請中' as hp_approval
, c.name as company_name
, pd.insert_timestamp as reg_date
, case pd.position_type 
	when 1 then 'PERMANENT'
	when 2 then 'CONTRACT'
	end as job_type
, j.job_owners
, d.division
--, hp.field_value
--, pd.head_count_open_date, pd.head_count_close_date
, concat_ws('', 'https://randstad.vincere.io/jobDetail/loadJobDetail.do?id=', pd.id::text, '#/tabActive=jobAdditionalInformation') as URL
from position_description pd
join hp_approval hp on hp.additional_id = pd.id
left join division d on d.additional_id = pd.id
left join job_owners j on j.position_id = pd.id
left join company c on c.id = pd.company_id
where 1=1
and pd.head_count_open_date < now()
and pd.head_count_close_date > now()
and floated_job = 0
and position_category = 1
order by pd.id desc