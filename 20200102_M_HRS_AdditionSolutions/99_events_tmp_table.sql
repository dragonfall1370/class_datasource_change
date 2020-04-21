--Event temp table
with t0 as (select __pk as event_id
	, replace(_fk_contact_list, char(11),',') as _fk_contact_list
	from [20191030_155243_events]
	)

, t1 as (select event_id, value as contact_id
	from t0
	cross apply string_split(_fk_contact_list,','))

, t2 as (select event_id
	, contact_id
	from t1 where contact_id <> '')

select distinct __pk as event_id
	, _fk_candidate_list
	, _fk_consultant
	, case
	      when t2.event_id is not null then t2.contact_id
	      else coalesce(nullif(_fk_contact_list, ''), null)
	      end as contact_id
	, _fk_job
	, date_start
	, date_end
	, colour_code
	, replace(description, char(11), char(10)) as description
	, summary
	, created_by
	, convert(datetime, stamp_created, 103) as stamp_created
--into events --#temp table
from [20191030_155243_events] a
left join t2 on t2.event_id = a.__pk --7906 rows

/* AUDIT
select *
from ABC
where event_id in (1074, 1084, 1093)

select distinct __pk, _fk_contact_list
from [20191030_155243_events]
where len(_fk_contact_list) > 10
*/