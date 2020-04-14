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
	from pa_contact pa
	join (select * from vc_contact where vc_contact_email <> '') vc
			on vc.vc_contact_email = pa.pa_contact_email and (vc.vc_fname_lname_kana = pa.pa_contact_name_kana or vc.vc_lname_fname_kana = pa.pa_contact_name_kana)
	where pa.company_id = vc.company_id
	and pa.contact_id not in (select contact_id from pa_vc_email_name)
	)

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
	from pa_contact pa
	join vc_contact vc
			on (vc.vc_fname_lname = pa.pa_contact_name or vc.vc_lname_fname = pa.pa_contact_name)
			and (vc.vc_fname_lname_kana = pa.pa_contact_name_kana or vc.vc_lname_fname_kana = pa.pa_contact_name_kana)
	where pa.company_id = vc.company_id
	and pa.contact_id not in (select contact_id from pa_vc_email_name)
	) 
	
, vc_pa_dup as (select * from pa_vc_email_name
	UNION
	select * from pa_vc_email_kana
	UNION
	select * from pa_vc_name_kana
	)

	select *
	, row_number() over(partition by company_id, contact_id order by coalesce(last_activity_date, vc_insert_timestamp) desc, merged_contact_id desc) rn
	--into mike_tmp_contact_dup_check
	from vc_pa_dup
	where 1=1
	--and contact_id in (33557, 38950, 42176, 38951, 41111) --check sample contact


/* AUDIT ON CONTACT DUP CHECK
--CHECK REFERENCE AFTER MERGED INTO COMPANIES
with contact_filter as (select c.company_id
	, company_id_bkup
	, c.id as contact_id
	, concat_ws('', c.first_name, c.last_name) as origin_contact_name
	, case 
	when c.external_id ilike 'REC%' then lower(overlay(replace(replace(c.last_name, N'　', ''), ' ', '') placing '' from 1 for length(c.external_id) + 2))
		else concat(lower(replace(replace(first_name, N'　', ''), ' ', '')), lower(replace(replace(last_name, N'　', ''), ' ', '')))
			end as vc_contact_name
	, concat_ws('', c.first_name_kana, c.last_name_kana) as origin_contact_name_kana
	, concat(lower(replace(replace(first_name_kana, N'　', ''), ' ', '')), lower(replace(replace(last_name_kana, N'　', ''), ' ', ''))) vc_contact_name_kana 
	, c.email as vc_origin_contact_email
	, case 
			when c.external_id ilike 'REC%' and c.email like '%_@_%.__%' then lower(overlay(trim(c.email) placing '' from 1 for length(external_id) + 1))
			when (c.external_id ilike 'REC%' or c.external_id is NULL) and c.email like '%_@_%.__%' then lower(overlay(trim(c.email) placing '' from 1 for length(c.id::text) + 2))
			else NULL end as vc_contact_email
	, c.insert_timestamp
	, ce.last_activity_date
	, m.update_date as pa_update_date
	from contact c
	left join contact_extension ce on ce.contact_id = c.id
	left join mike_tmp_pa_contact_merged m on m.contact_id = c.id --linked check pa information
	where exists (select company_id from contact where c.company_id = contact.company_id and company_id <> company_id_bkup)
	order by c.company_id, c.id
	)

--check on contact email | 10800
select *
, row_number() over(partition by company_id, vc_contact_email 
			order by coalesce(company_id_bkup, 1) asc, pa_update_date desc, coalesce(last_activity_date, insert_timestamp, '1900-01-01') desc ) as email_rn
, 1 as dataset
from contact_filter
where vc_contact_email is not NULL

UNION
--check on first_name | last_name | 15139
select *
, row_number() over(partition by company_id, vc_contact_name 
			order by coalesce(company_id_bkup, 1) asc, pa_update_date desc, coalesce(last_activity_date, insert_timestamp, '1900-01-01') desc ) as name_rn
, 2 as dataset
from contact_filter
where vc_contact_name is not NULL and vc_contact_name <> ''

UNION
--check on first_name_kana | last_name_kana | 10990
select *
, row_number() over(partition by company_id, vc_contact_name_kana 
			order by coalesce(company_id_bkup, 1) asc, pa_update_date desc, coalesce(last_activity_date, insert_timestamp, '1900-01-01') desc ) as name_rn
, 3 as dataset
from contact_filter
where vc_contact_name_kana is not NULL and vc_contact_name_kana <> ''
*/