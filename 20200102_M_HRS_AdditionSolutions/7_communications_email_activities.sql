--Communication | Emails as activities
with email_activities as (select c.communication_id
		, coalesce('AS' + nullif(convert(varchar(max), trim(char(11) from c.company_id)), ''), NULL) as com_ext_id
		, coalesce('AS' + nullif(convert(varchar(max), ev._kf_contact_to), ''), NULL) as con_ext_id
		, coalesce('AS' + nullif(convert(varchar(max), ev.candidate_id), ''), NULL) as job_ext_id
		, coalesce('AS' + nullif(convert(varchar(max), can.__pk), ''), NULL) as cand_ext_id
		, coalesce('[Email]' + char(10) + nullif(
			concat_ws(char(10)
				, coalesce('[Company] ' + nullif(com.name, ''), NULL)
				, coalesce('[Contact] ' + nullif(concat_ws(' ', nullif(con.name_first, ''), nullif(con.name_last, '')), ''), NULL)
				, coalesce('[Job] ' + nullif(j.title, ''), NULL)
				, coalesce('[Candidate] ' + nullif(concat_ws(' ', nullif(can.name_first, ''), nullif(can.name_last, '')), ''), NULL)
				, coalesce('[Account name] ' + nullif(ev.account_name, ''), NULL)
				, coalesce('[Priority] ' + nullif(ev.[priority], ''), NULL)
				, coalesce('[Stage] ' + nullif(ev.stage, '') + char(10), NULL)
				, coalesce('[From] ' + nullif(ev.[from], ''), NULL)
				, coalesce('[To] ' + nullif(ev.[to], ''), NULL)
				, coalesce('[CC] ' + nullif(ev.[cc], ''), NULL)
				, coalesce('[BCC] ' + nullif(convert(varchar(max), ev.[bcc]), ''), NULL)
				, coalesce('[Email attachments] ' + nullif(replace(email_attachments, char(11), ', '), ''), NULL)
				, coalesce('[Subject] ' + ev.[subject], NULL)
				, coalesce('[Body] ' + char(10) + nullif(ev.[body], '') + char(10), NULL)
			), ''), NULL) as comment_activities
		, coalesce(nullif(ev.stamp_created, ''), c.stamp_created) as stamp_created
		, 'comment' as category
		, -10 as user_account_id
	from communications c
	left join emails ev on c._fk_email = ev.email_id
	left join (select * from [20191030_153350_contacts] where type = 'Contact') con on con.__pk = ev._kf_contact_to
	left join (select * from [20191030_153350_contacts] where type = 'Candidate') can on can.__pk = ev.candidate_id
	left join [20191030_153350_companies] com on com.__pk = trim(char(11) from c.company_id)
	left join [20191030_155620_jobs] j on j.__pk = c.job_id
	where c._fk_email > 0
	and nullif(ev.body, '') is not NULL
	)

select *
from email_activities
where coalesce(com_ext_id, con_ext_id, job_ext_id, cand_ext_id) is not NULL