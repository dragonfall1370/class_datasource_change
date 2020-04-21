with t0 as (select __pk as email_id
	, case when _kf_candidate = 'NO RESULT' then NULL
		else replace(replace(_kf_candidate, '.0', ''), char(11), ',') end as _kf_candidate
	from [20191030_154447_emails]
	)

, t1 as (select email_id, value as candidate_id
	from t0
	cross apply string_split(_kf_candidate,','))

, t2 as (select email_id
	, candidate_id
	from t1 where candidate_id <> '') --6607

select distinct __pk as email_id
	, e._kf_account
	, e._kf_consultant
	, e.account_name
	, e.[from]
	, e.[to]
	, e.cc
	, e.bcc
	, e.[subject]
	, replace(replace(e.body, char(11), char(10)), concat('Ê', char(10)), '') as body
	, e._kf_candidate
	, case
	      when t2.email_id is not null then iif(t2.candidate_id = 'NO RESULT', NULL, t2.candidate_id)
	      else coalesce(nullif(_kf_candidate, ''), null)
	      end as candidate_id
	, e._kf_contact_to
	, case when e._kf_job = 'NO RESULT' then NULL
		else replace(_kf_job, '.0', '') end job_id
	, stage
	, [priority]
	, convert(datetime, e.stamp_created, 103) as stamp_created
--into emails --#temp table
from [20191030_154447_emails] e
left join t2 on t2.email_id = e.__pk --49730 rows
--where email_id in (55710)

/*
update emails
set candidate_id = NULL
where candidate_id in ('NO RESULT', char(11), '')
*/

/*AUDIT
select distinct __pk, _kf_candidate
from [20191030_154447_emails]
where len(_kf_candidate) > 10
*/