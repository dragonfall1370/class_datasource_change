WITH --MAPPING: PRIVATE EMAIL IS USED AS PRIMARY EMAIL
private_email as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as privatememail
		from person_eaddress pe
		left join eaddress e on e.ideaddress = pe.ideaddress
		where pe.idpersoncommunicationtype = '3285c9df-8eb2-4b26-97ef-4a3dc7452aa9' --private email
		)

, primary_email as (select idperson, TRIM('' FROM (TRIM('''' FROM privatememail))) as privatememail
		--distinct email if emails exist more than once
		, row_number() over(partition by trim(' ' from lower(translate(privatememail, ':  ?!\/#$%^&*()<>{}[]',''))) order by idperson asc) as rn
		--distinct if contacts may have more than 1 email
		, row_number() over(partition by idperson order by trim(' ' from privatememail)) as contactrn
		from private_email
		where privatememail like '%_@_%.__%'
		)
--CHANGED TO DEFAULT EMAIL due to less values
, dup as (select idperson, TRIM('' FROM TRIM('''' FROM translate(defaultemail,':  ?!\/#$%^&*()<>{}[]',''))) as defaultemail
		--distinct email if emails exist more than once
		, row_number() over(partition by trim(' ' from lower(translate(defaultemail,':  ?!\/#$%^&*()<>{}[]',''))) order by idperson asc, createdon desc) as rn
		--distinct if contacts may have more than 1 email
		, row_number() over(partition by idperson order by trim(' ' from translate(defaultemail,':  ?!\/#$%^&*()<>{}[]',''))) as contactrn
		from personx
		where defaultemail like '%_@_%.__%'
		)
, split_relocate_location_list AS (
	SELECT 
	idperson, 
	s.relocate_location
	FROM personx px, UNNEST(string_to_array(px.idrelocatelocation_string_list, ',')) s(relocate_location)
)
, relocate_location AS (
	SELECT
	idperson,
	l.value relocate_location
	FROM split_relocate_location_list srll
	LEFT JOIN "location" l ON srll.relocate_location = l.idlocation
)
, cte_join_relocate_location_list AS (
	SELECT 
	idperson, 
	string_agg(relocate_location, ', ') relocate_location_list
	FROM relocate_location 
	GROUP BY idperson
)

--Linkedin | not having multiple linkedin
, linkedin as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as linkedin
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '22cfe759-44fb-4378-9c23-16b19fa00935' --linkedin
	and e.commvalue is not NULL
	)
--GatedTalent | not having multiple GatedTalent
, gatedtalent as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as gatedtalent
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '6b3fd179-fb26-4c9f-a22e-6eed1cc03aae' --GatedTalent
	and e.commvalue is not NULL
	)
--Company URL
, companyurl as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as companyurl
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = 'd3322ae4-01cf-4302-ac36-6e52504bcb77' --URL Company
	and e.commvalue is not NULL
	)
--Candidate URLs (may include Linkedin)
, candidateurl as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as candidateurl
	from person_eaddress pe
	left join eaddress e on e.ideaddress = pe.ideaddress
	where pe.idpersoncommunicationtype = '8c7d16c4-125f-498b-b932-5465373a782b' --URLs
	and e.commvalue is not NULL
	)
--Switchboard | not having multiple switchboard
, candidate_switchboard as (select pe.idperson, pe.idpersoncommunicationtype, e.commvalue as candidate_switchboard
		from person_eaddress pe
		left join eaddress e on e.ideaddress = pe.ideaddress
		where pe.idpersoncommunicationtype = 'dced2973-8162-4152-a75a-a7d7991d1577' --switchboard
		)

--Documents
, documents as (select c.idcandidate, c.idperson
		, d.newdocumentname
		, reverse(substring(reverse(d.newdocumentname), 1, position('.' in reverse(d.newdocumentname)))) as extension
		, d.originaldocumentname
		, d.createdon::timestamp as created
		from candidate c
		join entitydocument ed on ed.entityid = c.idperson
		join document d on d.iddocument = ed.iddocument
		where ed.entityid is not NULL
		and d.newdocumentname is not NULL)
		
, candidate_document as (select idperson
		, string_agg(newdocumentname, ',' order by created desc) as candidate_document
		from documents
		where extension in ('.doc','.docx','.pdf','.rtf','.xls','.xlsx','.htm', '.html', '.msg', '.txt')
		group by idperson)
--Contractors
, contact_contractor as (select c.idcontractor, c.idperson
		, c.minimumrequiredrate
		, c.idcurrency
		, cur.value
		, cur.currencyname
		, c.contractoravailabilitycomment
		, c.contractorpaymentcomment
		, concat_ws(chr(10)
			, coalesce('[Minimum required rate] ' || nullif(REPLACE(c.minimumrequiredrate, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Currency name] ' || nullif(REPLACE(cur.currencyname, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Contractor availability comment]' || chr(10) || nullif(REPLACE(c.contractoravailabilitycomment, '\x0d\x0a', ' '), ''), NULL)
			, coalesce('[Contractor payment comment] ' || chr(10) || nullif(REPLACE(c.contractorpaymentcomment, '\x0d\x0a', ' '), ''), NULL)
			) as contact_contractor
		from contractor c
		left join currency cur on cur.idcurrency = c.idcurrency --value | currencyname
		where coalesce(c.minimumrequiredrate, c.idcurrency, c.contractoravailabilitycomment, c.contractorpaymentcomment) is not NULL
		)
--Candidate work history
, company_cand as (select *
		, ROW_NUMBER() OVER(PARTITION BY cp.idperson 
				ORDER BY cp.sortorder ASC, cp.employmentfrom DESC, cp.isdefaultrole desc, cp.isactiveemployment desc) rn
		from company_person cp)

, work_history as (select px.idperson, px.idcompany, cp.idcompany, px.companyname
		, px.jobtitle, px.fromdate, cp.employmentfrom, px.todate, cp.employmentto
		, px.idpersonstatus_string, px.salary, cp.salary, cp.checkedon, cp.checkedby, cp.employmentassistant, cp.sortorder
		from personx px
		left join (select * from company_cand where rn = 1) cp on cp.idperson = px.idperson
		left join personstatus ps on ps.idpersonstatus = px.idpersonstatus_string
		where px.isdeleted = '0'
		--and px.idperson = 'cd2260b2-80b9-42e6-b2ee-4a5c542c87d8'
		--order by px.idperson
		)

--Selected users
, selected_user as (select iduser, idorganizationunit, title, firstname, lastname
		, fullname, replace(useremail, 'spenglerfox.eu', 'spenglerfox.com') as useremail, createdon
		from "user"
		where isdisabled = '0'
		and useremail ilike '%_@_%.__%'
		and (firstname not ilike '%partner%' and jobtitle not ilike '%partner%')
		)
		
--MAIN SCRIPT
, cte_candidate AS (SELECT c.idperson candidate_id
	, COALESCE(px.idcompany, '0') company_id
	, ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
--Basic information
	, COALESCE(TRIM(REPLACE(px.firstname, '\x0d\x0a', ' ')), concat('First name - ', p.personid)) first_name
	, TRIM(REPLACE(px.middlename, '\x0d\x0a', ' ')) middle_name
	, COALESCE(TRIM(REPLACE(px.lastname, '\x0d\x0a', ' ')), 'Last name') last_name
	, to_char(px.dateofbirth::DATE, 'YYYY-MM-DD') dob
	, px.knownas preffered_name --#inject
	, case t.value
		when 'Miss' then 'MISS'
		when 'Ms' then 'MS'
		when 'Dr' then 'DR'
		when 'Mr' then 'MR'
		when 'Mrs' then 'MRS'
		else NULL end gender_title
	, case when px.idgender_string = '2ac1b27a-e06c-4f42-9945-54a1d1866db2' then 'FEMALE'
		when px.idgender_string = '7cba7288-09cf-456d-8821-86ff7e521942' then 'MALE'
		else NULL end as gender --table | gender
	, case mrs.value
		when 'Married' then 2
		when 'Widowed' then 4
		when 'Partner' then 0
		when 'Seperated' then 5
		when 'Single' then 1
		when 'Divorced' then 3
		else 0 end as marital_stastus --#Inject
--Current Address
	, TRIM(replace(px.addressdefaultfull, '\x0d\x0a', ', ')) as address --location_name can be updated later
	, TRIM(replace(px.addressdefaultcity, '\x0d\x0a', ', ')) city
	, TRIM(replace(px.addressdefaultpostcode, '\x0d\x0a', ', ')) postal_code
	, TRIM(replace(cs.value, '\x0d\x0a', ', ')) state
	, case when co.abbreviation = 'UK' then 'GB'
				when co.abbreviation in ('Unknown', '--') THEN NULL
				when co.value = 'United States' then 'US'
				when co.value = 'Russian Federation' then 'RU'
				when co.value = 'Ireland' then 'IE'
				when co.value = 'Serbia' then 'RS'
				when co.value = 'United States of America' THEN 'US'
				when co.value = 'United Kingdom' THEN 'GB'
				when co.value = 'Canada' THEN 'CA'
				when co.value = 'New Zealand' THEN 'NZ'
				when co.value = 'Czech Republic' THEN 'CZ'
				when co.value = 'Germany' THEN 'DE'
				when co.value = 'Philippines' THEN 'PH'
				when co.value = 'Spain' THEN 'ES'
				when co.value = 'Australia' THEN 'AU'
				when co.value = 'United States of America' THEN 'US'
				when co.value = 'Republic of Ireland' THEN 'IE'
				when co.value = 'India' THEN 'IN'
				when co.value = 'Switzerland' THEN 'CH'
				when co.value = 'Nigeria' THEN 'NG'
				when co.value = 'Greece' THEN 'GR'
				when co.value = 'France' THEN 'FR'
				when co.value = 'Netherlands' THEN 'NL'
				when co.value in ('Unknown', '--') THEN NULL
				else co.abbreviation end as country --addressdefaultidcountry_string
--Communication info
	, TRIM('' FROM (TRIM('''' FROM px.emailwork))) work_email
	, px.mobileprivate primary_phone
	, px.directlinephone mobile_phone
	, px.phonehome home_phone --#Inject
	, px.mobilebusiness work_phone
	, CASE
		WHEN dup.rn > 1 THEN concat(dup.rn, '_', dup.defaultemail)
		ELSE coalesce(dup.defaultemail, concat(px.personid, '_candidate@noemail.com')) END email
	, u.useremail owner_email
	, lk.linkedin
	, cw.candidate_switchboard personal_phone --#inject
	, canu.candidateurl website --#inject
--Employment info
	, CASE
		WHEN pet.value = 'Permanent' THEN 'PERMANENT'
		WHEN pet.value = 'Flex' THEN 'CONTRACT'
		ELSE 'PERMANENT' END employment_type
	, px.salary current_salary
	, px.minimumrequiredrate contract_rate
	, case when curr.value = '---' then NULL
		else curr.value end currency
	, case when ut.value = 'Day' then 'DAYS' 
		when ut.value = 'Hour' then 'HOURS'
		else NULL end as contract_interval
	, px.jobtitle title1
	, px.companyname employer_org_name1
	, to_char(px.fromdate::DATE, 'YYYY-MM-DD') start_date1
	, to_char(px.todate::DATE, 'YYYY-MM-DD') end_date1
	, px.previousjobtitle title2
	, px.previouscompany employer_org_name2
	, to_char(px.previouscompanytodate::DATE, 'YYYY-MM-DD') end_date2
	, doc.candidate_document as resume
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
	LEFT JOIN linkedin lk on lk.idperson = p.idperson
	LEFT JOIN candidate_switchboard cw on cw.idperson = p.idperson
	LEFT JOIN candidateurl canu on canu.idperson = p.idperson
	LEFT JOIN dup on dup.idperson = p.idperson
	LEFT JOIN country co ON px.addressdefaultidcountry_string = co.idcountry
	LEFT JOIN preferredemploymenttype pet ON pet.idpreferredemploymenttype = px.idpreferredemploymenttype_string
	LEFT JOIN maritalstatus mrs on mrs.idmaritalstatus = px.idmaritalstatus_string
	LEFT JOIN countystate cs ON px.addressdefaultcountystate = cs.abbreviation
	LEFT JOIN (select * from contractor where idcurrency is not NULL) ctr on ctr.idperson = p.idperson
	LEFT JOIN currency curr on curr.idcurrency = ctr.idcurrency
	LEFT JOIN title t on t.idtitle = px.idtitle_string
	LEFT JOIN unittype ut ON ut.idunittype = px.idunittype_string
	LEFT JOIN "location" l ON l.idlocation = px.idlocation_string
	LEFT JOIN selected_user u ON px.iduser = u.iduser
	LEFT JOIN candidate_document doc on doc.idperson = p.idperson
	--where p.personid::bigint = 2079019
	--where p.personid::bigint between 1 and 300000 --87559
	--where p.personid::bigint between 300000 and 1100000 --99958
	--where p.personid::bigint between 1100000 and 1200000 --99988
	--where p.personid::bigint between 1200000 and 1300000 --99994
	--where p.personid::bigint between 1300000 and 1400000 --99987
	--where p.personid::bigint between 1400000 and 2000000 --42865
	--where p.personid::bigint between 2000000 and 2091358 --84396
)

SELECT
	candidate_id "candidate-externalId"
	, company_id
	, first_name "candidate-firstName"
	, middle_name "candidate-middleName"
	, last_name "candidate-lastName"
	, employment_type
	, dob "candidate-dob"
	, gender_title "candidate-title"
	, REPLACE(address, ',,', ',') "candidate-address"
	, city "candidate-city"
	, state "candidate-State"
	, country "candidate-Country"
	, postal_code "candidate-zipCode"
	, mobile_phone "candidate-mobile"
	, primary_phone "candidate-phone"
	, home_phone "candidate-homePhone"
	, work_phone "candidate-workPhone"
	, email "candidate-email"
	, work_email "candidate-workEmail"
	, owner_email "candidate-owners"
	, linkedin "candidate-linkedIn"
	, current_salary "candidate-currentSalary"
	, contract_rate "candidate-contractRate"
	, title1 "candidate-jobTitle1"
	, employer_org_name1 "candidate-employer1"
	, COALESCE(start_date1, '') "candidate-startDate1"
	, COALESCE(end_date1, '') "candidate-endDate1"
	, title2 "candidate-jobTitle2"
	, employer_org_name2 "candidate-employer2"
	, COALESCE(end_date2, '') "candidate-endDate2"
	, resume "candidate-resume"
FROM cte_candidate
WHERE rn = 1
--AND company_id IN (SELECT idcompany FROM company)
--limit 1000