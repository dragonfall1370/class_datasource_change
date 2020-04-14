--VC CANDIDATES WITH MERGING CONDITIONS
with pa_candidate as (select [PANO ] as cand_ext_id
	, case when [メール] like '%_@_%.__%' then lower(trim([メール]))
		else NULL end as pa_email
	, convert(datetime, 登録日, 120) as created_date
	from csv_can
	where nullif([メール], '') is not NULL)

, vc_cand_filter as (select id as vc_cand_id
	, first_name, last_name
	, lower(trim(email)) as vc_email
	, convert(datetime, insert_timestamp, 120) as insert_timestamp
	from vc_candidate
	where nullif(email, '') is not NULL)

--MAIN SCRIPT
select c.cand_ext_id
, c.pa_email
, c.created_date
, vcf.vc_cand_id
, vcf.first_name
, vcf.last_name
, vcf.vc_email
, vcf.insert_timestamp
from pa_candidate c
join vc_cand_filter vcf on vcf.vc_email = c.pa_email
where c.pa_email is not NULL

/* AUDIT
select candidate_id
from candidate_extension
group by candidate_id
having count(*) > 1


select id, first_name, last_name, trigger_index_update_timestamp, *
from candidate
where external_id ilike 'CDT%'
order by trigger_index_update_timestamp
limit 100
*/