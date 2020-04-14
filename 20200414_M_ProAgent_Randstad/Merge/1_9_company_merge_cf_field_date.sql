with latest_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_date_value
	, 'add_com_info' additional_type
	, 1001 form_id
	, 11331 field_id
	from mike_tmp_company_dup_check2 m
	join (select * from additional_form_values where form_id = 1001 and field_id = 11331) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') --2255 rows
	and a.field_date_value is not NULL
)

, older_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_date_value
	, 'add_com_info' additional_type
	, 1001 form_id
	, 11331 field_id
	from mike_tmp_company_dup_check m
	join (select * from additional_form_values where form_id = 1001 and field_id = 11331) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and vc_company_id not in (select vc_company_id from latest_company)
	and a.field_date_value is not NULL
)

--IF USING OVERWRITE
, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group

--MAIN SCRIPT
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_date_value)
select 'add_com_info' additional_type
, vc_company_id as additional_id
, 1001 form_id
, 11331 field_id
, field_date_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_date_value = excluded.field_date_value;