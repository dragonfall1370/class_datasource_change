--Events as activities
with event_activities as (select c.communication_id
	, coalesce('AS' + nullif(convert(varchar(max), trim(char(11) from c.company_id)), ''), NULL) as com_ext_id
	, coalesce('AS' + nullif(convert(varchar(max), c.contact_id), ''), NULL) as con_ext_id
	, coalesce('AS' + nullif(convert(varchar(max), ev._fk_job), ''), NULL) as job_ext_id
	, coalesce('AS' + nullif(convert(varchar(max), can.__pk), ''), NULL) as cand_ext_id
	, coalesce('[Events]' + char(10) + nullif(
		concat_ws(char(10)
			, coalesce('[Created by] ' + nullif(ev.created_by,''), NULL)
			, coalesce('[Company] ' + nullif(com.name, ''), NULL)
			, coalesce('[Contact] ' + nullif(concat_ws(' ', nullif(con.name_first, ''), nullif(con.name_last, '')), ''), NULL)
			, coalesce('[Job] ' + nullif(j.title, ''), NULL)
			, coalesce('[Candidate] ' + nullif(concat_ws(' ', nullif(can.name_first, ''), nullif(can.name_last, '')), ''), NULL)
			, coalesce('[Summary] ' + nullif(ev.summary,''), NULL)
			, coalesce('[Description] ' + char(10) + nullif(ev.description,''), NULL)
			, coalesce('[Colour code] ' + nullif(ev.colour_code,''), NULL)
			, coalesce('[Date start] ' + nullif(ev.date_start,''), NULL)
			, coalesce('[Date end] ' + nullif(ev.date_end,''), NULL)
		), ''), NULL) as comment_activities
	, coalesce(nullif(ev.stamp_created, ''), c.stamp_created) as stamp_created
	, 'comment' as category
	, -10 as user_account_id
	from communications c
	left join events ev on c._fk_event = ev.event_id
	left join (select * from [20191030_153350_contacts] where type = 'Contact') con on con.__pk = ev.contact_id
	left join (select * from [20191030_153350_contacts] where type = 'Candidate') can on can.__pk = ev._fk_candidate_list
	left join [20191030_153350_companies] com on com.__pk = trim(char(11) from c.company_id)
	left join [20191030_155620_jobs] j on j.__pk = c.job_id
	where c._fk_event > 0)

select *
from event_activities
where comment_activities is not NULL