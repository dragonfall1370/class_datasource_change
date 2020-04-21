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

, addresses as (select _fk_contact
		, coalesce('[' + nullif([type], '') + '] ' + nullif(
			concat_ws(', '
			, coalesce(nullif([line_one], ''), NULL), coalesce(nullif([line_two], ''), NULL), coalesce(nullif([line_three], ''), NULL)
			, coalesce(nullif([city], ''), NULL), coalesce(nullif([county], ''), NULL), coalesce(nullif([postcode], ''), NULL)
			, coalesce(nullif([country], ''), NULL)
			), ''), NULL) as can_address
		, city as can_city
		, [county] as can_state
		, postcode as can_postcode
		, case when country = 'Malta' then 'MT'
			when country = 'United Kingdom' then 'GB'
			when country = 'Czech Republic' then 'CZ'
			when country = 'Denmark' then 'DK'
			when country = 'France' then 'FR'
			when country = 'India' then 'IN'
			when country = 'Ireland' then 'IR'
			when country = 'Netherlands' then 'NL'
			when country = 'Poland' then 'PL'
			when country = 'Portugal' then 'PT'
			when country = 'Singapore' then 'SG'
			when country = 'South Africa' then 'ZA'
			when country = 'Switzerland' then 'CH'
			when country = 'UK' then 'GB'
			else NULL end as can_country
		from [20191030_153215_addresses]
		where _fk_contact is not NULL)

, documents as (select _kf_contact as cand_id
		, concat_ws('_', __pk, name) as cand_doc
		, stamp_created
		from [20191030_163510_documents.xlsx]
		where _kf_contact is not NULL)

, cand_doc as (select cand_id
		, string_agg(cand_doc, ',') within group (order by stamp_created desc) as cand_doc
		from documents
		group by cand_id)

, flag_by as (select __pk as cand_id
		, cast(ceiling(nullif(value, '')) as bigint) as flag_by_id
		from [20191030_153350_contacts]
		cross apply string_split(flagged_by, char(11)))

, flag_by_users as (select f.cand_id
		, string_agg(u.user_email, ', ') as flag_by_users
		from flag_by f
		left join users u on u.__pk = f.flag_by_id
		where f.flag_by_id is not NULL
		group by f.cand_id)

--MAIN SCRIPT
select concat('AS', c.__pk) as [candidate-externalId]
	, coalesce(nullif(name_first, ''), concat('Firstname - ', c.__pk)) as [candidate-firstName]
	, coalesce(nullif(name_last, ''), coalesce('Last name - ' + nullif(c.candidate_current_employer, ''), 'Last name')) as [candidate-lastName]
--Email
	--, iif(dup.rn > 1, concat(dup.rn, '_', dup.email_one), dup.email_one) as [candidate-email]
	, case when dup.rn > 1 then concat(dup.rn, '_', dup.email_one)
		when dup.rn = 1 then dup.email_one
		else concat(c.__pk, '_candidate@noemail.com') end as [candidate-email]
	, concat_ws(',', nullif(c.email_two,''), nullif(c.ae_email_home,'')) as [candidate-workEmail]
	, candidate_cv_source --#Inject
--Phone
	, concat_ws(',', nullif(ae_phone_mobile, ''), nullif(phone_one, '')) as [candidate-phone]
	, nullif(phone_two, '') as [candidate-workPhone]
	, nullif(phone_three, '') as [candidate-homePhone]
--Address
	, a.can_address as [candidate-address]
	, a.can_city as [candidate-City]
	, a.can_state as [candidate-State]
	, a.can_postcode as [candidate-ZipCode]
	, a.can_country as [candidate-Country]
--Personal info
	, convert(datetime, c.date_created, 103) as reg_date
	, candidate_date_of_birth
	, try_parse(c.candidate_date_of_birth as date using 'en-GB') as [candidate-dob]
	, c.candidate_nationality --all NULL
	, replace(c.tags_summary, char(11), char(10)) as [candidate-skill]
	, c.url_linkedin as [candidate-linkedIn]
	, c.url_twitter --#Inject
	, c.url_facebook --#Inject
	, u.user_email as [contact-owners]
	, c.candidate_cv_source --#Inject
	, department --#Inject
--Candidate work history
	, coalesce('<p>' +
		nullif(concat_ws('<br/>'
			, coalesce('[Company] ' + nullif(c.candidate_current_employer, ''), NULL)
			, coalesce('[Current position] ' + nullif(c.current_position, ''), NULL)
			, coalesce('[Employment market] ' + nullif(convert(varchar(max), c.candidate_employment_market), ''), NULL)
			, coalesce('[Position sought] ' + nullif(convert(varchar(max), c.candidate_position_sought), ''), NULL)
		), '') + '</p>', NULL) as [candidate-workHistory]
	, current_position as [candidate-jobTitle1]
	, candidate_current_employer as [candidate-employer1]
	, c.candidate_salary_sought_low as [candidate-desiredSalary]
	, c.candidate_current_rates as monthly_salary --current rate
	, c.candidate_current_salary as [candidate-currentSalary]
--Education summary
	, concat_ws(char(10)
		, coalesce('[Education] ' + char(10) + nullif(replace(c.candidate_education, char(11), char(10)),''), NULL)
		, coalesce('[Qualifications] ' + char(10) + nullif(replace(c.candidate_qualifications, char(11), char(10)),''), NULL)
		) as [candidate-education]
--Brief
	, coalesce('<p>' +
		nullif(concat_ws('<br/>'
			, coalesce('[Candidate External ID] ' + nullif(convert(varchar(max), c.__pk),''), NULL)
			, coalesce('[Pref. Comms.] ' + nullif(c.preferred_comms,''), NULL)
			, coalesce('[Department] ' + nullif(c.department,''), NULL)
			, coalesce('[Contact] ' + nullif(case when c.ae_flag_one_contact = 1 then 'YES' else NULL end,''), NULL)
			, coalesce('[Contact type] ' + nullif(c.type,''), NULL)
			, coalesce('[Flagged by] ' + nullif(f.flag_by_users,''), NULL) --c.flagged_by
			, coalesce('[Hot by] ' + nullif(u3.user_full_name,''), NULL) --c.flagged_by_hot
			, coalesce('[Candidate status] ' + nullif(c.candidate_status,''), NULL)
			, coalesce('[Candidate progress] ' + nullif(c.candidate_progress,''), NULL)
			, coalesce('[Visa status] ' + nullif(convert(varchar(max), c.candidate_visa_status),''), NULL) --empty
			, coalesce('[Marital status] ' + nullif(convert(varchar(max), c.candidate_marital_status),''), NULL) --empty
			, coalesce('[Current notice period] ' + nullif(c.candidate_notice_period,''), NULL)
			, coalesce('[Current benefits] ' + nullif(convert(varchar(max), c.candidate_current_benefits),''), NULL)
			, coalesce('[Next availability] ' + nullif(c.candidate_next_availability,''), NULL)
			, coalesce('[Salary info] ' + nullif(convert(varchar(max), c.candidate_current_salary_info),''), NULL)
			, coalesce('[Preferred location] ' + nullif(convert(varchar(max), c.candidate_preferred_location),''), NULL)
			, coalesce('[Modified] ' + nullif(c.stamp_modified,''), NULL)
			, coalesce('[Indeed] ' + nullif(convert(varchar(max), c.url_indeed),''), NULL) --empty
			, coalesce('[Google+] ' + nullif(convert(varchar(max), c.url_google_plus),''), NULL)
			, coalesce('[Profile] ' + '<br/>' + nullif(c.candidate_profile,''), NULL)
			, coalesce('[Notes] ' + '<br/>' + nullif(c.notes,''), NULL)
		), '') + '</p>', NULL) as [candidate-note]
	, cd.cand_doc as [candidate-resume]
from [20191030_153350_contacts] c
left join dup on dup.__pk = c.__pk
left join users u on u.__pk = c._fk_consultant --owners
left join flag_by_users f on f.cand_id = c.__pk
left join users u3 on u3.__pk = c.flagged_by_hot --empty value
left join addresses a on a._fk_contact = c.__pk
left join cand_doc cd on cd.cand_id = c.__pk --documents
where c.type = 'Candidate'
--and c.__pk = 61730