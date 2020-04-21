--Emails activities
with emails as (select e.__pk as email_id
	, e._kf_consultant
	, e.account_name
	, e.[from]
	, e.[to]
	, e.cc
	, e.bcc
	, e.[subject]
	, replace(e.body, char(11), char(10)) as body
	, e._kf_candidate
	, case when trim(value) = 'NO RESULT' then NULL
		else trim(value) end as candidate_id
	, e._kf_contact_to
	, case when e._kf_job = 'NO RESULT' then NULL
		else replace(_kf_job, '.0', '') end job_id 
	, e.stamp_created
	from [20191030_154447_emails] e
	cross apply string_split(replace(_kf_candidate, '.0', ''), char(11))
	where _kf_candidate not in ('')
	)

select e.email_id
	, e._kf_consultant
	, e.account_name
	, e.[from]
	, e.[to]
	, e.cc
	, e.bcc
	, e.[subject]
	, replace(e.body, char(11), char(10)) as body
	, e._kf_candidate as candidate_list
	, c.name_first
	, c.name_last
	, e._kf_contact_to
	, cont.name_first
	, c.name_last
	, e.job_id
	, e.stamp_created
from emails e
left join (select * from [20191030_153350_contacts] where type = 'Candidate') c on c.__pk = e.candidate_id
left join [20191030_153350_contacts] cont on cont.__pk = e._kf_contact_to
where e.candidate_id <> e._kf_contact_to