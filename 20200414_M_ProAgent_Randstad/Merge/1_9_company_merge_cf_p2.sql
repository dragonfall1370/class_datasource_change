--#CF PANO | 11275 | Free Text
---Latest PA companies
with latest_company as (select *
	, 'add_com_info' additional_type
	, 1001 form_id
	, 11275 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) a
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') --549 rows
)

---Older PA companies
, older_company as (select *
	, 'add_com_info' additional_type
	, 1001 
	, 11275 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) a
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and vc_company_id not in (select vc_company_id from latest_company) --2395 rows
)

, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group --1338


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_com_info' additional_type
, vc_company_id as additional_id
, 1001 form_id
, 11275 field_id
, com_ext_id field_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;

	
--#CF Name of representative | 11274 | Free Text
---Latest PA companies
with latest_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 form_id
	, 11274 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 11274) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

---Older PA companies
, older_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 
	, 11274 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 11274) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and vc_company_id not in (select vc_company_id from latest_company) -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group --825 rows


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_com_info' additional_type
, vc_company_id as additional_id
, 1001 form_id
, 11274 field_id
, cf.field_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
	
--#CF Business Details | 1002 | Text Area
---Latest PA companies
with latest_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 form_id
	, 1002 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1002) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

---Older PA companies
, older_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 
	, 1002 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1002) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and vc_company_id not in (select vc_company_id from latest_company) -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group --1089 rows


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_com_info' additional_type
, vc_company_id as additional_id
, 1001 form_id
, 1002 field_id
, cf.field_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	

--#CF Business Characteristic | 1022 | Text Area
with merged_cf as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, concat_ws(chr(10), ('[Merged from PA: ' || m.com_ext_id || '] ') , nullif(a.field_value,' ')) as merged_cf
	, 'add_com_info' additional_type
	, 1001 form_id
	, 1022 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1022 and field_value <> chr(10)) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and a.field_value is not NULL and a.field_value <> ''
	--and m.com_ext_id = 'CPY017629'
)

--IF USING FOR APPENDING
, cf_group as (select vc_company_id
	, additional_type, form_id, field_id
	, string_agg(merged_cf, chr(10) order by rn asc) as cf_group
	from merged_cf
	group by vc_company_id, additional_type, form_id, field_id
	) --select * from cf_group --778 rows
	
insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_com_info' additional_type
, vc_company_id
, 1001 form_id
, 1022 field_id
, cf_group field_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = concat_ws(chr(10) || chr(13), nullif(additional_form_values.field_value, ''), excluded.field_value);
	
	
--#CF Listed Stock Market | 1015 | Dropdown
---Latest PA companies
with latest_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 form_id
	, 1015 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1015) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

---Older PA companies
, older_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 
	, 1015 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1015) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and vc_company_id not in (select vc_company_id from latest_company) -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group --488 rows | select count(*) from additional_form_values where field_id = 1015


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_com_info' additional_type
, vc_company_id as additional_id
, 1001 form_id
, 1015 field_id
, cf.field_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
	
--#CF Fisical Closing Month | 1026 | Dropdown
---Latest PA companies
with latest_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 form_id
	, 1026 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1026) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

---Older PA companies
, older_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	, 'add_com_info' additional_type
	, 1001 
	, 1026 field_id
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1026) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and vc_company_id not in (select vc_company_id from latest_company) -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group --237 rows | select count(*) from additional_form_values where field_id = 1026


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_com_info' additional_type
, vc_company_id as additional_id
, 1001 form_id
, 1026 field_id
, cf.field_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;


--USING FOR OVERWRITTEN
---Latest PA companies
with latest_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1114) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) > coalesce(vc_latest_date, '1900-01-01') -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

---Older PA companies
, older_company as (select m.vc_company_id
	, m.vc_pa_company_id
	, m.rn
	, a.field_value
	from (select * from mike_tmp_company_dup_check2 where vc_pa_company_id not in (select vc_pa_company_id from mike_tmp_company_dup_check)) m
	join (select * from additional_form_values where form_id = 1001 and field_id = 1114) a on a.additional_id = m.vc_pa_company_id
	where 1=1
	and rn = 1
	and coalesce(vc_pa_update_date, vc_pa_reg_date) < coalesce(vc_latest_date, '1900-01-01')
	and vc_company_id not in (select vc_company_id from latest_company) -- rows
	and a.field_value is not NULL and a.field_value <> ''
)

, cf_group as (select *
	from latest_company
	UNION
	select *
	from older_company) --select * from cf_group --1000 rows | select count(*) from additional_form_values where field_id = 1114


insert into additional_form_values (additional_type, additional_id, form_id, field_id, field_value)
select 'add_com_info' additional_type
, vc_company_id as additional_id
, 1001 form_id
, 1114 field_id
, cf.field_value
from cf_group cf
on conflict on constraint additional_form_values_pkey
	do update
	set field_value = excluded.field_value;
