--Communication | without email/event
select c.communication_id
, coalesce('AS' + nullif(convert(varchar(max), trim(char(11) from c.company_id)), ''), NULL) as com_ext_id
, coalesce('AS' + nullif(convert(varchar(max), c.contact_id), ''), NULL) as con_ext_id
, coalesce('AS' + nullif(convert(varchar(max), c.job_id), ''), NULL) as job_ext_id
, coalesce('[Communications]' + char(10) + nullif(
		concat_ws(char(10)
			, coalesce('[Company] ' + nullif(com.name, ''), NULL)
			, coalesce('[Contact] ' + nullif(concat_ws(' ', nullif(con.name_first, ''), nullif(con.name_last, '')), ''), NULL)
			, coalesce('[Job] ' + nullif(j.title, ''), NULL)
			, coalesce('[Consultant] ' + nullif(case 
					when c._fk_consultant = 1000 then 'simon@additionsolutions.co.uk'
					when c._fk_consultant = 1002 then 'brett@additionsolutions.co.uk'
					when c._fk_consultant = 1003 then 'mitchell@additionsolutions.co.uk'
					when c._fk_consultant = 1006 then 'james@additionsolutions.co.uk'
					when c._fk_consultant = 1008 then 'ben@additionsolutions.co.uk'
					when c._fk_consultant = 1009 then 'kayla@additionsolutions.co.uk'
					when c._fk_consultant = 1012 then 'ellie@additionsolutions.co.uk'
					when c._fk_consultant = 1013 then 'ben.c@additionsolutions.co.uk'
					when c._fk_consultant = 1014 then 'aimee@additionsolutions.co.uk'
					when c._fk_consultant = 1015 then 'anthony@additionsolutions.co.uk'
					when c._fk_consultant = 1017 then 'kirsty@additionsolutions.co.uk'
					when c._fk_consultant = 1018 then 'dominique@additionsolutions.co.uk'
					end, ''), NULL)
			, coalesce('[Email from] ' + nullif(replace(c.email_from_name, char(11), ', '),''), NULL)
			, coalesce('[Email recipients] ' + nullif(replace(c.email_recipient_names, char(11), ', '),''), NULL)
			, coalesce('[Communication subject] ' + nullif(c.communication_subject,''), NULL)
		), ''), NULL) as comment_activities
, c.stamp_created as stamp_created
, 'comment' as category
, -10 as user_account_id
from communications c
left join (select * from [20191030_153350_contacts] where type = 'Contact') con on con.__pk = c.contact_id
left join [20191030_153350_companies] com on com.__pk = trim(char(11) from c.company_id)
left join [20191030_155620_jobs] j on j.__pk = c.job_id
where 1=1
and _fk_email is NULL
and (_fk_event is NULL or _fk_event = 0)