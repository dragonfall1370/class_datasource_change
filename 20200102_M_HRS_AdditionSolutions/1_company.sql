with dup as (SELECT __pk, trim(name) as name
		, row_number() over(partition by trim(name) order by __pk asc) as rn 
		from [20191030_153350_companies] c
		where nullif(name, '') is not NULL)

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

, sites as (select _fk_company
		, string_agg(replace(site_name, char(11), char(10)), '; ') as sites
		from [20191030_160039_sites]
		group by _fk_company)

, documents as (select _kf_company_image as company_id
		, concat_ws('_', __pk, name) as company_doc
		, stamp_created
		from [20191030_163510_documents.xlsx]
		where _kf_company_image is not NULL

		UNION

		select _kf_company as company_id
		, concat_ws('_', __pk, name) as company_doc
		, stamp_created
		from [20191030_163510_documents.xlsx]
		where _kf_company is not NULL)

, company_doc as (select company_id
		, string_agg(company_doc, ',') within group (order by stamp_created desc) as company_doc
		from documents
		group by company_id)

, addresses as (select _fk_company
		, concat_ws(', '
			, coalesce(nullif([line_one], ''), NULL), coalesce(nullif([line_two], ''), NULL), coalesce(nullif([line_three], ''), NULL)
			, coalesce(nullif([city], ''), NULL), coalesce(nullif([county], ''), NULL), coalesce(nullif([postcode], ''), NULL)
			, coalesce(nullif([country], ''), NULL)
			) as com_address
		, city as com_city
		, [county] as com_state
		, postcode as com_postcode
		, case when country = 'Malta' then 'MT'
			when country = 'United Kingdom' then 'GB' else NULL end as com_country
		from [20191030_153215_addresses]
		where _fk_company is not NULL and _fk_site is NULL)

--MAIN SCRIPT
select 
concat('AS', c.__pk) as [company-externalId]
	, iif(dup.rn > 1, concat(dup.name, ' ', dup.rn), coalesce(dup.name, concat('No company name - ', c.__pk))) as [company-name]
	, c.phone_switchboard_one as [company-switchboard]
	, c.phone_switchboard_two as [company-phone]
	, c.phone_fax as [company-fax]
	, left(c.url_web, 100) as [company-website]
	, no_of_employees --inject
	, convert(datetime, stamp_created, 103) as reg_date
	, u.user_email as [company-owners]
--Company address
	, com_address as [company-locationAddress]
	, com_address as [company-locationName]
	, com_city as [company-locationCity]
	, com_state as [company-locationState]
	, com_postcode as [company-locationZipCode]
	, com_country as [company-locationCountry]
	, concat_ws(char(10)
		, coalesce('[External ID] ' + nullif(convert(varchar(max), c.__pk),''), NULL)
		, coalesce('[Nature of Business] ' + char(10) + nullif(replace(c.nature_of_business, char(11), char(10)),''), NULL)
		, coalesce('[Flagged by] ' + nullif(u2.user_full_name,''), NULL) --c.flagged_by
		, coalesce('[Hot by] ' + nullif(u3.user_full_name,''), NULL) --c.flagged_by_hot
		, coalesce('[Type] ' + char(10) + nullif(replace(c.type, char(11), char(10)),''), NULL)
		, coalesce('[Language] ' + nullif(c.language,''), NULL)
		, coalesce('[Sites] ' + nullif(s.sites,''), NULL)
		, coalesce('[Free agreed] ' + nullif(convert(varchar(max), c.ageed_fee),''), NULL)
		, coalesce('[Client job ref] ' + nullif(c.prefix_job_client_ref,''), NULL)
		--, coalesce('[LinkedIn] ' + nullif(c.url_linkedin,''), NULL) --empty
		--, coalesce('[Twitter] ' + nullif(c.url_twitter,''), NULL) --empty
		--, coalesce('[Facebook] ' + nullif(c.url_facebook,''), NULL) --empty
		, coalesce('[General notes] ' + char(10) + nullif(replace(c.notes, char(11), char(10)),''), NULL)
		) as [company-note]
	, cd.company_doc as [company-document]
from [20191030_153350_companies] c
left join sites s on s._fk_company = c.__pk
left join dup on dup.__pk = c.__pk
left join users u on u.__pk = c._fk_consultant
left join users u2 on u2.__pk = c.flagged_by
left join users u3 on u3.__pk = c.flagged_by_hot
left join company_doc cd on cd.company_id = c.__pk
left join (select * from addresses where nullif(com_address, '') is not NULL) as a on a._fk_company = c.__pk

UNION 

select 'AS999999999', 'Default company', NULL, NULL, NULL, NULL, NULL, NULL
, NULL, NULL, NULL, NULL, NULL, NULL, NULL, 'This is default company from data import', NULL