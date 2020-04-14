with pa_contact as (select c.[企業 PANO ] as com_ext_id
	, trim(c.[採用担当者ID]) as con_ext_id
	, [採用担当者] as origin_name
	, lower(replace(replace([採用担当者], N'　', ''), ' ', '')) as pa_contact_name
	, [フリガナ] as origin_name_kana
	, lower(replace(replace([フリガナ], N'　', ''), ' ', '')) as pa_contact_name_kana
	, case when [メール] like '%_@_%.__%' then lower(trim([メール]))
		else NULL end as pa_contact_email
	, convert(datetime, [登録日], 120) as created_date
	from csv_rec c)

, vc_contact_filter as (select id as vc_contact_id
	, company_id as vc_company_id
	, concat(lower(replace(replace(first_name, N'　', ''), ' ', '')), lower(replace(replace(last_name, N'　', ''), ' ', ''))) vc_contact_name
	, concat(lower(replace(replace(first_name_kana, N'　', ''), ' ', '')), lower(replace(replace(last_name_kana, N'　', ''), ' ', ''))) vc_contact_name_kana
	, case when contact_email like '%_@_%.__%' then lower(trim(contact_email))
			else NULL end as vc_contact_email
	, insert_timestamp
	from vc_contact)

, company_name as (select id, name
	, charindex('(',name) begin_string
	, charindex(')',name) end_string
	, insert_timestamp
	, last_activity_date
	from vc_company)

--3 cases with wrong company name ID (37906,15424,21300)
, vc_company_name as (select id, name
	, substring(name, begin_string + 1
		, case when end_string < begin_string then len(name) - begin_string - 1
			else end_string - begin_string - 1 end) as vc_company_name
	, insert_timestamp
	, last_activity_date
	from company_name
	where 1=1
	and begin_string > 0 and end_string > 0
	--and end_string - begin_string < 1
	--and id = 37906
	)

, vc_company_dup as (select [PANO ] as pa_company_id
	, [会社名] as pa_company_name
	, vc.id as vc_company_id
	, vc.name as vc_company_name
	from csv_recf c
	join vc_company_name vc on replace(replace([会社名], N'　', ''), ' ', '') = replace(replace(vc_company_name, N'　', ''), ' ', '')
	where vc.id is not NULL)

/* AUDIT ALL CONTACT RECORDS
select pc.*
from pa_contact pc
--join vc_company_dup vcd on vcd.pa_company_id = pc.com_ext_id
where com_ext_id in (select pa_company_id from vc_company_dup) --12521 rows
AND
(pc.contact_email in (select contact_email from vc_contact_filter where contact_email <> '')
OR pc.contact_name in (select contact_name from vc_contact_filter where contact_name <> '')
OR pc.contact_name_kana in (select contact_name_kana from vc_contact_filter where contact_name_kana <> '')
)
*/

--MAIN SCRIPT
	---> Duplicated contacts with email address
, all_dup_contact as (select pc.*
	, vcf.*
	, vcd.pa_company_id
	from pa_contact pc
	join (select * from vc_contact_filter where vc_contact_email <> '') vcf on vcf.vc_contact_email = pc.pa_contact_email
	join (select distinct pa_company_id, vc_company_id from vc_company_dup) vcd on vcd.vc_company_id = vcf.vc_company_id
	where 1=1
	and pc.com_ext_id = vcd.pa_company_id --2289 rows
	--and pc.com_ext_id <> vcd.pa_company_id --1278 excluded
	
	
	UNION ALL
	---> Duplicated contacts with contact name
	select pc.*
	, vcf.*
	, vcd.pa_company_id
	from pa_contact pc
	join (select * from vc_contact_filter where vc_contact_name <> '') vcf on vcf.vc_contact_name = pc.pa_contact_name
	join (select distinct pa_company_id, vc_company_id from vc_company_dup) vcd on vcd.vc_company_id = vcf.vc_company_id
	where 1=1
	and pc.com_ext_id = vcd.pa_company_id --2140 rows
	--and pc.com_ext_id <> vcd.pa_company_id --1278 excluded
	
	
	UNION ALL
	---> Duplicated contacts with contact name kana
	select pc.*
	, vcf.*
	, vcd.pa_company_id
	from pa_contact pc
	join (select * from vc_contact_filter where vc_contact_name_kana <> '') vcf on vcf.vc_contact_name_kana = pc.pa_contact_name_kana
	join (select distinct pa_company_id, vc_company_id from vc_company_dup) vcd on vcd.vc_company_id = vcf.vc_company_id
	where 1=1
	and pc.com_ext_id = vcd.pa_company_id --4 rows
	)

select distinct com_ext_id
, con_ext_id
, pa_contact_name
, pa_contact_name_kana
, pa_contact_email
, vc_contact_id
, vc_contact_email
, vc_contact_name
, vc_contact_name_kana
, vc_company_id
from all_dup_contact --2774 rows

/*
---MERGED FROM VINCERE
--contacts migrated from PA
with pa_contact as (select company_id
		, c.com_ext_id
		, contact_id
		, trim(c.con_ext_id) as con_ext_id
		, contact_name
		, lower(replace(replace(contact_name, N'　', ''), ' ', '')) as pa_contact_name
		, contact_name_kana as origin_name_kana
		, lower(replace(replace(contact_name_kana, N'　', ''), ' ', '')) as pa_contact_name_kana
		, case when contact_email like '%_@_%.__%' then lower(trim(contact_email))
				else NULL end as pa_contact_email
		, reg_date
		, m.vc_pa_company_id
		, m.pa_company_name
		, m.vc_company_id
		, m.vc_origin_company_name
	from mike_tmp_pa_contact_merged c
	join mike_tmp_company_dup_check m on m.vc_pa_company_id = c.company_id --only listed company in duplicate check
	--where company_id in (select vc_pa_company_id from mike_tmp_company_dup_check)
	) --9614 rows

, vc_contact as (select company_id as vc_company_id
		, c.id as vc_contact_id
		, concat(lower(replace(replace(first_name, N'　', ''), ' ', '')), lower(replace(replace(last_name, N'　', ''), ' ', ''))) vc_contact_name
		, concat(lower(replace(replace(first_name_kana, N'　', ''), ' ', '')), lower(replace(replace(last_name_kana, N'　', ''), ' ', ''))) vc_contact_name_kana
		, c.email as vc_origin_contact_email
		, case when c.email like '%_@_%.__%' then lower(overlay(trim(c.email) placing '' from 1 for length(c.id::text) + 2))
				else NULL end as vc_contact_email
		, c.insert_timestamp
		, ce.last_activity_date
		from contact c
		join contact_extension ce on ce.contact_id = c.id
		where c.deleted_timestamp is NULL
		and (external_id is NULL or external_id not ilike 'REC%') --11293 rows
		and company_id in (select vc_company_id from mike_tmp_company_dup_check) --5258 rows
		)

---> Duplicated contacts with email address
select pc.*
	, vc.*
	from pa_contact pc
	join (select * from vc_contact where vc_contact_email <> '') vc on vc.vc_contact_email = pc.pa_contact_email
	where 1=1
	and pc.com_ext_id = m.com_ext_id
*/