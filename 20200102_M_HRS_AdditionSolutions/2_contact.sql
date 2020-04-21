with dup as (select __pk, email_one
	, row_number() over(partition by trim(' ' from lower(email_one)) order by __pk asc) as rn --distinct email if emails exist more than once
	, row_number() over(partition by __pk order by trim(' ' from email_one)) as contactrn --distinct if contacts may have more than 1 email
	from [20191030_153350_contacts]
	where email_one like '%_@_%.__%')

, users as (select __pk
		, ae_name_full as user_full_name
		, case when __pk = 1000 then 'simon@additionsolutions.co.uk'
			when __pk = 1002 then 'brett@additionsolutions.co.uk'
			when __pk = 1003 then 'mitchell@additionsolutions.co.uk'
			when __pk = 1006 then 'james@additionsolutions.co.uk'
			when __pk = 1008 then 'ben@additionsolutions.co.uk'
			when __pk = 1009 then 'kayla@additionsolutions.co.uk'
			when __pk = 1012 then 'ellie@additionsolutions.co.uk'
			when __pk = 1013 then 'ben.c@additionsolutions.co.uk'
			when __pk = 1014 then 'aimee@additionsolutions.co.uk'
			when __pk = 1015 then 'anthony@additionsolutions.co.uk'
			when __pk = 1017 then 'kirsty@additionsolutions.co.uk'
			when __pk = 1018 then 'dominique@additionsolutions.co.uk'
			end as user_email
		from [20191030_153350_consultants])

--MAIN SCRIPT
select concat('AS', c.__pk) as [contact-externalId]
	, iif(nullif(c._fk_company, '') is NULL or c._fk_company not in (select __pk from [20191030_153350_companies]), 'AS999999999'
			, concat('AS', c._fk_company)) as [contact-companyId]
	, c.name_first as [contact-firstName]
	, coalesce(nullif(name_last, ''), coalesce('Last name - ' + nullif(c.candidate_current_employer, ''), 'Last name')) as [contact-lastName]
	, iif(dup.rn > 1, concat(dup.rn, '_', dup.email_one), dup.email_one) as [contact-email]
	, c.email_two as personal_email --#Inject
	, candidate_cv_source --#Inject
	, phone_one as [contact-phone]
	, concat_ws(', ', nullif(phone_two, ''), nullif(phone_three, '')) as personal_phone --#Inject
	, current_position as [contact-jobTitle]
	, url_linkedin --#Inject
	, url_twitter --#Inject
	, url_facebook --#Inject
	, department --#Inject
	, u.user_email as [contact-owners]
	, concat_ws(char(10)
		, coalesce('[External ID] ' + convert(varchar(max), c.__pk), NULL)
		, coalesce('[Pref. Comms.] ' + char(10) + nullif(c.preferred_comms,''), NULL)
		, coalesce('[Department] ' + char(10) + nullif(c.department,''), NULL)
		, coalesce('[Contact] ' + nullif(case when c.ae_flag_one_contact = 1 then 'YES' else NULL end,''), NULL)
		, coalesce('[Contact type] ' + nullif(c.type,''), NULL)
		, coalesce('[Flagged by] ' + nullif(u2.user_full_name,''), NULL) --c.flagged_by
		, coalesce('[Hot by] ' + nullif(u3.user_full_name,''), NULL) --c.flagged_by_hot
		) as contact_note
from [20191030_153350_contacts] c
left join dup on dup.__pk = c.__pk
left join users u on u.__pk = c._fk_consultant --owners
left join users u2 on u2.__pk = c.flagged_by
left join users u3 on u3.__pk = c.flagged_by_hot
where c.type = 'Contact'