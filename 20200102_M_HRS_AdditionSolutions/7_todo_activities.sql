--Todo as activities
with todo_activities as (select t.todo_id
	, NULL as com_ext_id
	, coalesce('AS' + nullif(convert(varchar(max), t.contact_id), ''), NULL) as con_ext_id
	, coalesce('AS' + nullif(convert(varchar(max), t._fk_job), ''), NULL) as job_ext_id
	, coalesce('AS' + nullif(convert(varchar(max), t._fk_candidate), ''), NULL) as cand_ext_id
	, coalesce('[To do]' + char(10) + nullif(
		concat_ws(char(10)
			--, coalesce('[Created by] ' + nullif(t.created_by,''), NULL)
			, coalesce('[From] ' + nullif(c.ae_name_from, ''), NULL)
			, coalesce('[To] ' + nullif(c.ae_name_to, ''), NULL)
			, coalesce('[Contact] ' + nullif(concat_ws(' ', nullif(con.name_first, ''), nullif(con.name_last, '')), ''), NULL)
			, coalesce('[Job] ' + nullif(j.title, ''), NULL)
			, coalesce('[Candidate] ' + nullif(concat_ws(' ', nullif(can.name_first, ''), nullif(can.name_last, '')), ''), NULL)
			, coalesce('[Summary] ' + nullif(t.summary,''), NULL)
			, coalesce('[Description] ' + char(10) + nullif(t.description,''), NULL)
			, coalesce('[Colour code] ' + nullif(t.ae_colour_code,''), NULL)
			, coalesce('[Date start] ' + nullif(t.date_start,''), NULL)
			, coalesce('[Date end] ' + nullif(t.date_end,''), NULL)
		), ''), NULL) as comment_activities
	, t.stamp_created as stamp_created
	, 'comment' as category
	, -10 as user_account_id
	from todo t
	left join communications c on c.communication_id = t._fk_communication
	left join (select * from [20191030_153350_contacts] where type = 'Contact') con on con.__pk = t.contact_id
	left join (select * from [20191030_153350_contacts] where type = 'Candidate') can on can.__pk = t._fk_candidate
	left join [20191030_155620_jobs] j on j.__pk = t._fk_job
	)

select *
from todo_activities
where coalesce(con_ext_id, job_ext_id, cand_ext_id) is not NULL