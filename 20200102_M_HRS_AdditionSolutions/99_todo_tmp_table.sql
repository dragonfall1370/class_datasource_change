--Todo temp table
with t0 as (select __pk as todo_id
	, replace(_fk_contact, char(11),',') as _fk_contact
	from [20191030_160039_todos]
	)

, t1 as (select todo_id, value as contact_id
	from t0
	cross apply string_split(_fk_contact, ','))

, t2 as (select todo_id
	, contact_id
	from t1 where contact_id <> '')

select distinct a.__pk as todo_id
	, case
	      when t2.todo_id is not null then t2.contact_id
	      else coalesce(nullif(a._fk_contact, ''), null)
	      end as contact_id
	, convert(datetime, stamp_created, 103) as stamp_created
	, a._fk_candidate
	, a._fk_consultant
	, a._fk_contact
	, a._fk_job
	, a.created_by
	, a.summary
	, replace(description, char(11), char(10)) as description
	, a.ae_colour_code
	, a.date_start
	, a.date_end
	, _fk_communication
into todo --#temp table
from [20191030_160039_todos] a
left join t2 on t2.todo_id = a.__pk --1194