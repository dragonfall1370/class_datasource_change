with pa_contact as (select c.company_id
		, company_id_bkup
		, c.id as contact_id
		, lower(overlay(replace(replace(c.last_name, N'　', ''), ' ', '') placing '' from 1 for length(c.external_id) + 2)) as pa_contact_name
		, concat(lower(replace(replace(first_name_kana, N'　', ''), ' ', '')), lower(replace(replace(last_name_kana, N'　', ''), ' ', ''))) pa_contact_name_kana
		, c.email as vc_pa_origin_email
		, case when c.email like '%_@_%.__%' then lower(overlay(trim(c.email) placing '' from 1 for length(external_id) + 1))
			else NULL end as pa_contact_email
		, c.insert_timestamp
		, m.update_date as pa_update_date
	from contact c
	left join mike_tmp_pa_contact_merged m on m.contact_id = c.id --linked check pa information
	where c.company_id <> c.company_id_bkup
	order by c.company_id, c.id
	) --9614 rows

, vc_contact as (select c.company_id 
		, c.company_id_bkup
		, c.id as contact_id
		, concat_ws('', lower(replace(replace(first_name, N'　', ''), ' ', '')), lower(replace(replace(last_name, N'　', ''), ' ', ''))) as vc_fname_lname
		, concat_ws('', lower(replace(replace(last_name, N'　', ''), ' ', '')), lower(replace(replace(first_name, N'　', ''), ' ', ''))) as vc_lname_fname
		, concat_ws('', c.first_name_kana, c.last_name_kana) as origin_contact_name_kana
		, concat_ws('', lower(replace(replace(first_name_kana, N'　', ''), ' ', '')), lower(replace(replace(last_name_kana, N'　', ''), ' ', ''))) as vc_fname_lname_kana
		, concat_ws('', lower(replace(replace(last_name_kana, N'　', ''), ' ', '')), lower(replace(replace(first_name_kana, N'　', ''), ' ', ''))) as vc_lname_fname_kana
		, c.email as vc_origin_email
		, case when c.email like '%_@_%.__%' then lower(overlay(trim(c.email) placing '' from 1 for length(c.id::text) + 2))
			else NULL end as vc_contact_email
		, c.insert_timestamp
		, ce.last_activity_date
	from contact c
	join contact_extension ce on ce.contact_id = c.id
	where 1=1
	and c.company_id in (select distinct company_id from pa_contact)
	and c.id not in (select distinct contact_id from pa_contact)
	order by c.company_id, c.id
	) --5526 rows
	
, pa_vc_email_name as (select pa.company_id
	, pa.company_id_bkup
	, pa.contact_id
	, pa.pa_contact_name
	, pa.pa_contact_name_kana
	, pa.pa_contact_email
	, pa.insert_timestamp
	, pa.pa_update_date
	, vc.company_id as new_company_id
	, vc.contact_id as merged_contact_id
	, vc.vc_fname_lname
	, vc.vc_lname_fname
	, vc.vc_contact_email
	, vc.insert_timestamp as vc_insert_timestamp
	, vc.last_activity_date
	, 1 rnk
	from pa_contact pa
	join (select * from vc_contact where vc_contact_email <> '') vc
			on vc.vc_contact_email = pa.pa_contact_email and (vc.vc_fname_lname = pa.pa_contact_name or vc.vc_lname_fname = pa.pa_contact_name) --2070 rows
	where pa.company_id = vc.company_id
	) --1729 rows

, pa_vc_email_kana as (select pa.company_id
	, pa.company_id_bkup
	, pa.contact_id
	, pa.pa_contact_name
	, pa.pa_contact_name_kana
	, pa.pa_contact_email
	, pa.insert_timestamp
	, pa.pa_update_date
	, vc.company_id as new_company_id
	, vc.contact_id as merged_contact_id
	, vc.vc_fname_lname
	, vc.vc_lname_fname
	, vc.vc_contact_email
	, vc.insert_timestamp as vc_insert_timestamp
	, vc.last_activity_date
	, 2 rnk
	from pa_contact pa
	join (select * from vc_contact where vc_contact_email <> '') vc
			on vc.vc_contact_email = pa.pa_contact_email and (vc.vc_fname_lname_kana = pa.pa_contact_name_kana or vc.vc_lname_fname_kana = pa.pa_contact_name_kana)
	where pa.company_id = vc.company_id
	and pa.contact_id not in (select contact_id from pa_vc_email_name)
	) --select * from pa_vc_email_kana

, pa_vc_name_kana as (select pa.company_id
	, pa.company_id_bkup
	, pa.contact_id
	, pa.pa_contact_name
	, pa.pa_contact_name_kana
	, pa.pa_contact_email
	, pa.insert_timestamp
	, pa.pa_update_date
	, vc.company_id as new_company_id
	, vc.contact_id as merged_contact_id
	, vc.vc_fname_lname
	, vc.vc_lname_fname
	, vc.vc_contact_email
	, vc.insert_timestamp as vc_insert_timestamp
	, vc.last_activity_date
	, 3 rnk
	from pa_contact pa
	join vc_contact vc
			on (vc.vc_fname_lname = pa.pa_contact_name or vc.vc_lname_fname = pa.pa_contact_name)
			and (vc.vc_fname_lname_kana = pa.pa_contact_name_kana or vc.vc_lname_fname_kana = pa.pa_contact_name_kana)
	where pa.company_id = vc.company_id
	and pa.contact_id not in (select contact_id from pa_vc_email_name)
	)
	
, pa_vc_name as (select pa.company_id
	, pa.company_id_bkup
	, pa.contact_id
	, pa.pa_contact_name
	, pa.pa_contact_name_kana
	, pa.pa_contact_email
	, pa.insert_timestamp
	, pa.pa_update_date
	, vc.company_id as new_company_id
	, vc.contact_id as merged_contact_id
	, vc.vc_fname_lname
	, vc.vc_lname_fname
	, vc.vc_contact_email
	, vc.insert_timestamp as vc_insert_timestamp
	, vc.last_activity_date
	from pa_contact pa
	join vc_contact vc
			on (vc.vc_fname_lname = pa.pa_contact_name or vc.vc_lname_fname = pa.pa_contact_name)
	where pa.company_id = vc.company_id
	and pa.contact_id not in (select contact_id from pa_vc_email_name)
	) 
	
, pa_vc_name_rn as (select *
	, row_number() over(partition by company_id, contact_id order by coalesce(last_activity_date, vc_insert_timestamp) desc, merged_contact_id desc) rn
	from pa_vc_name)
	
, vc_pa_dup as (select distinct * from pa_vc_email_name
	UNION
	select distinct * from pa_vc_email_kana
	UNION
	select distinct * from pa_vc_name_kana
	UNION 
	select distinct * from pa_vc_name_rn 
	where rn = 1
	and contact_id not in (select contact_id from pa_vc_name_kana)
	) 
	
--select *
--from pa_vc_email_name where contact_id = 33557

--select contact_id, count(*) from vc_pa_dup
--group by contact_id
--having count(*) > 1

/*
--select * from vc_pa_dup where contact_id = 38952

--Check dup
select contact_id, count(*) from vc_pa_dup
group by contact_id
having count(*) > 1
	--where contact_id = 64124

select * from mike_tmp_contact_dup_check
where contact_id = 38952
*/

select distinct *
, row_number() over(partition by company_id, contact_id order by coalesce(last_activity_date, vc_insert_timestamp) desc, merged_contact_id desc) rn
into mike_tmp_contact_dup_check2
from vc_pa_dup
where 1=1 --2207 rows
--and contact_id = 64124
	
/*	
	select *
	from mike_tmp_contact_dup_check --1730 rows
*/

/* AUDIT CHECK
select contact_id, count(*)
from mike_tmp_contact_dup_check2
group by contact_id
having count(*) > 1 --contact_id in (33557, 38950, 42176, 38951, 41111)

select *
from mike_tmp_contact_dup_check2
where contact_id in (33557, 38950, 42176, 38951, 41111)
and rn = 1

select company_id, contact_id, new_company_id, merged_contact_id
from mike_tmp_contact_dup_check2
where rn = 1
except
select company_id, contact_id, new_company_id, merged_contact_id
from mike_tmp_contact_dup_check
where rn = 1 --590 cases
*/