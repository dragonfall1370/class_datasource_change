--JOB links with same contacts but new companies
with dif_job as (select j.[__pk] as job_id
		, j.[_fk_company] as company_id
		, j.[_fk_contact] as contact_id
		, c._fk_company as contact_companyid
		from [20191030_155620_jobs] j
		left join (select * from [20191030_153350_contacts] where type = 'Contact') c on c.__pk = j.[_fk_contact]
		where j.[_fk_company] <> c._fk_company --different companies
		or j.[_fk_contact] is NULL)

--Different contact companies / default contact
, dif_contact as (select job_id
		, company_id
		, case when company_id is NULL or company_id not in (select __pk from [20191030_153350_companies]) then 'AS999999999'
			else concat('AS99999', company_id) end as dif_contact
		, 'Default contact' as contact_lname
		, 'Default contact for this company' as contact_note
		from dif_job
		--where company_id is not NULL --53 default contacts

		UNION ALL

		select __pk
		, _fk_company
		, case when _fk_company not in (select __pk from [20191030_153350_companies]) or _fk_company is NULL then 'AS999999999'
			else concat('AS99999', _fk_company) end as dif_contact
		, 'Default contact' as contact_lname
		, 'Default contact for this company' as contact_note
		from [20191030_155620_jobs]
		where _fk_contact not in (select __pk from [20191030_153350_contacts] where type = 'Contact')
		) --43 rows

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

, dup as (select __pk as job_id
		, row_number() over(partition by lower(title) order by title) as rn
		from [20191030_155620_jobs])

--DOCUMENTS
, documents as (select _fk_job as job_id
		, concat_ws('_', __pk, name) as job_doc
		, stamp_created
		from [20191030_163510_documents.xlsx]
		where _fk_job is not NULL)

, job_doc as (select job_id
		, string_agg(job_doc, ',') within group (order by stamp_created desc) as job_doc
		from documents
		group by job_id)

--MAIN SCRIPT
select concat('AS', j.__pk) as [position-externalId]
	, case when dup.rn > 1 then concat_ws(' - ', coalesce(nullif(j.title, ''), 'No job title'), j.__pk)
			else coalesce(nullif(j.title,''), concat('No job title - ', j.__pk)) end as [position-title]
	, j.[_fk_contact]
	, j._fk_company
	, case when j.__pk in (select job_id from dif_contact) then dc.dif_contact
			else concat('AS', j.[_fk_contact]) end as [position-contactId]
	, case when j.contract_type = 'Permanent' then upper('Permanent')
			when j.contract_type = 'Temp' then upper('Contract')
			else upper('Permanent') end as [position-type]
	, convert(date, j.date_job_opened, 103) as [position-startDate]
	, convert(date, j.date_job_closed, 103) as [position-endDate]
	, coalesce(nullif(j.vacancies,''), 1) as [position-headcount]
	, 'GBP' as [position-currency]
	, u.user_email as [company-owners]
--Job category
	, case when j.status in ('Open', 'Closed') then 1
		when j.status in ('Prospect') then 2
		else 1 end as job_category --#Inject
--Job description
	, coalesce('<p>' + 
		nullif(concat_ws('<br/>'
			, coalesce('[Job Benefits]' + '<br/>' + nullif(j.benefits, ''), NULL)
			, coalesce('[Car Included ?] ' + nullif(j.car, ''), NULL)
		), '') + '</p>', NULL) as [position-internalDescription]
	, coalesce('<p>' +
		nullif(concat_ws('<br/>'
			, coalesce('[Summary]' + '<br/>' + nullif(replace(j.summary, char(11), '<br/>'), ''), NULL)
			, coalesce('[Education]' + '<br/>' + nullif(replace(j.education, char(11), '<br/>'), ''), NULL)
			, coalesce('[Qualifications]' + '<br/>' + nullif(replace(j.qualifications, char(11), '<br/>'), ''), NULL)
			, coalesce('[Training]' + '<br/>' + nullif(replace(j.training, char(11), '<br/>'), ''), NULL)
			, coalesce('[Additional info]' + '<br/>' + nullif(replace(j.additional_info, char(11), '<br/>'), ''), NULL)
		), '') + '</p>', NULL) as [position-publicDescription]
--Notes
	, concat_ws(char(10)
		, coalesce('[Job External ID] ' + convert(varchar(10), j.__pk), NULL)
		, coalesce('[Employment Market] ' + nullif(j.employment_market, ''), NULL)
		, coalesce('[Bonus] ' + nullif(j.salary_bonus_exchanged, ''), NULL)
		, coalesce('[Date start contract] ' + nullif(j.date_start_contract, ''), NULL)
		, coalesce('[Date end contract] ' + nullif(j.date_end_contract, ''), NULL)
		, coalesce('[Flagged by] ' + nullif(u2.user_full_name,''), NULL) --c.flagged_by
		, coalesce('[Hot by] ' + nullif(u3.user_full_name,''), NULL) --c.flagged_by_hot
		, coalesce('[Int. Ref] ' + nullif(j.job_ref, ''), NULL)
		, coalesce('[Job Ref Client] ' + nullif(j.job_ref_client, ''), NULL)
		, coalesce('[Additional info]' + char(10) + nullif(replace(j.additional_info, char(11), char(10)), ''), NULL)
		) as [position-note]
	, jd.job_doc as [position-document]
from [20191030_155620_jobs] j
left join dup on dup.job_id = j.__pk
left join dif_contact dc on dc.job_id = j.__pk
left join users u on u.__pk = j._fk_consultant
left join users u2 on u2.__pk = j.flagged_by
left join users u3 on u3.__pk = j.flagged_by_hot
left join job_doc jd on jd.job_id = j.__pk
--where 1=1
--and j._fk_company in (1051, 1052)
--and j.__pk in (118)