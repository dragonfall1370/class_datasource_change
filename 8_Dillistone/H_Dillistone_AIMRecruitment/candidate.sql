WITH split_relocate_location_list AS (
	SELECT 
	id_person, 
	s.relocate_location
	FROM person_x px, UNNEST(string_to_array(px.id_relocate_location_string_list, ',')) s(relocate_location)
),
relocate_location AS (
	SELECT
	id_person,
	l.value relocate_location
	FROM split_relocate_location_list srll
	LEFT JOIN "location" l ON srll.relocate_location = l.id_location
),
cte_join_relocate_location_list AS (
	SELECT 
	id_person, 
	string_agg(relocate_location, ', ') relocate_location_list
	FROM relocate_location 
	GROUP BY id_person
),
cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	COALESCE(px.id_company, '0') company_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn,
	TRIM(px.first_name) first_name,
	TRIM(px.middle_name) middle_name,
	TRIM(px.last_name) last_name,
	to_char(px.date_of_birth::DATE, 'YYYY-MM-DD') dob,
	CASE
		WHEN RIGHT(TRIM(replace(px.address_private_full, '\x0d\x0a', ', ')),1) = ',' THEN LEFT(TRIM(replace(px.address_private_full, '\x0d\x0a', ', ')), CHAR_LENGTH(TRIM(replace(px.address_private_full, '\x0d\x0a', ', '))) - 1) 
		ELSE TRIM(replace(px.address_private_full, '\x0d\x0a', ', '))
	END address,
	px.address_private_city city,
	cs.value state,
	CASE WHEN regexp_match(px.address_private_country, '[0-9]+') IS NULL THEN 
		CASE
			WHEN px.address_private_country = 'United States of America' THEN 'US'
			WHEN px.address_private_country = 'United Kingdom' THEN 'GB'
			WHEN px.address_private_country = 'Canada' THEN 'CA'
			WHEN px.address_private_country = 'New Zealand' THEN 'NZ'
			WHEN px.address_private_country = 'Czech Republic' THEN 'CZ'
			WHEN px.address_private_country = 'Germany' THEN 'DE'
			WHEN px.address_private_country = 'Philippines' THEN 'PH'
			WHEN px.address_private_country = 'Spain' THEN 'ES'
			WHEN px.address_private_country = 'Australia' THEN 'AU'
			WHEN px.address_private_country = 'Republic of Ireland' THEN 'IE'
			WHEN px.address_private_country = 'India' THEN 'IN'
			WHEN px.address_private_country = 'Switzerland' THEN 'CH'
			WHEN px.address_private_country = 'Nigeria' THEN 'NG'
			WHEN px.address_private_country = 'Greece' THEN 'GR'
			WHEN px.address_private_country = 'France' THEN 'FR'
			WHEN px.address_private_country = 'Netherlands' THEN 'NL'
		END
	ELSE
		CASE
			WHEN co.value = 'United States of America' THEN 'US'
			WHEN co.value = 'United Kingdom' THEN 'GB'
			WHEN co.value = 'Canada' THEN 'CA'
			WHEN co.value = 'New Zealand' THEN 'NZ'
			WHEN co.value = 'Czech Republic' THEN 'CZ'
			WHEN co.value = 'Germany' THEN 'DE'
			WHEN co.value = 'Philippines' THEN 'PH'
			WHEN co.value = 'Spain' THEN 'ES'
			WHEN co.value = 'Australia' THEN 'AU'
			WHEN co.value = 'Republic of Ireland' THEN 'IE'
			WHEN co.value = 'India' THEN 'IN'
			WHEN co.value = 'Switzerland' THEN 'CH'
			WHEN co.value = 'Nigeria' THEN 'NG'
			WHEN co.value = 'Greece' THEN 'GR'
			WHEN co.value = 'France' THEN 'FR'
			WHEN co.value = 'Netherlands' THEN 'NL'
		END
	END country,
	px.address_private_post_code postal_code,
	px.mobile_private primary_phone,
	px.mobile_private mobile_phone,
	px.phone_home home_phone,
	px.direct_line_phone work_phone,
	CASE
		WHEN px.default_email IS NULL OR px.default_email NOT LIKE '%@%' THEN concat(p.person_id, '_', px.last_name, '@no_email.com')
		ELSE px.default_email
	END email,
	CASE
		WHEN px.default_email IS NOT NULL THEN ROW_NUMBER() OVER(PARTITION BY LOWER(px.default_email) ORDER BY px.created_on DESC) 
		ELSE 1
	END rn_email,
	px.email_work work_email,
	u.user_email owner_email,
	CASE
		WHEN pet.value = 'Permanent' THEN 'PERMANENT'
		WHEN pet.value = 'Flex' THEN 'CONTRACT'
		ELSE ''
	END employment_type,
	px.salary::int current_salary,
	px.minimum_required_rate::int contract_rate,
	px.job_title title1,
	px.company_name employer_org_name1,
	to_char(px.from_date::DATE, 'YYYY-MM-DD') start_date1,
	to_char(px.to_date::DATE, 'YYYY-MM-DD') end_date1,
	px.previous_job_title title2,
	px.previous_company employer_org_name2,
	to_char(px.previous_company_to_date::DATE, 'YYYY-MM-DD') end_date2,
	concat(
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_id::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate id: ', REPLACE(px.person_id::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.maiden_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Maiden name: ', REPLACE(px.maiden_name, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.date_of_birth, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Date of birth: ', REPLACE(px.date_of_birth, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.family, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Family: ', REPLACE(px.family, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Created on: ', REPLACE(px.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.alert_text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Alert text: ', REPLACE(px.alert_text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN px.is_off_limit = 0 THEN concat('Is off limit: No', E'\n') ELSE concat('Is off limit: Yes', E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(olt.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit type: ', REPLACE(olt.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_date_from, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit from: ', REPLACE(pol.off_limit_date_from, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_date_to, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit to: ', REPLACE(pol.off_limit_date_to, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_note, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Off limit note: ', REPLACE(pol.off_limit_note, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(prat.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person rating: ', REPLACE(prat.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perrt.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation type: ', REPLACE(perrt.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perr.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation created on: ', REPLACE(perr.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perr.created_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation created by: ', REPLACE(perr.created_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(perr.relation_description, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person relation description: ', REPLACE(perr.relation_description, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_reference, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate reference: ', REPLACE(px.person_reference, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.company_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Company name: ', REPLACE(px.company_name, '\x0d\x0a', ' '), E'\n') END, 
		CASE WHEN NULLIF(TRIM(REPLACE(pet.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Preferred employment type: ', REPLACE(pet.value, '\x0d\x0a', ' '), E'\n') END, 
		CASE WHEN NULLIF(TRIM(REPLACE(px.address_default_full, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Full default address: ', REPLACE(px.address_default_full, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(co.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default country: ', REPLACE(co.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.address_default_post_code, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default postcode: ', REPLACE(px.address_default_post_code, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.default_url, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Default URL: ', REPLACE(px.default_url, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.direct_line_phone, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Direct line phone: ', REPLACE(px.direct_line_phone, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.mobile_business, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Business mobile: ', REPLACE(px.mobile_business, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.mobile_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Other mobile: ', REPLACE(px.mobile_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.phone_home, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Home phone: ', REPLACE(px.phone_home, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.phone_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Other phone: ', REPLACE(px.phone_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.email_work, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Work Email: ', REPLACE(px.email_work, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.email_private, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Private Email: ', REPLACE(px.email_private, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.email_other, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Other email: ', REPLACE(px.email_other, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cur.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate unavailable reason: ', REPLACE(cur.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.international_value_string, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('International: ', REPLACE(px.international_value_string, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(l.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Location: ', REPLACE(l.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.nationality_value_string, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Nationality: ', REPLACE(px.nationality_value_string, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(po.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate origin: ', REPLACE(po.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ps.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate status: ', REPLACE(ps.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pr.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason: ', REPLACE(pr.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pst.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing status: ', REPLACE(pst.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.qualification_value_string, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Qualification: ', REPLACE(px.qualification_value_string, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(jrll.relocate_location_list, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Relocate location: ', REPLACE(jrll.relocate_location_list, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(r.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Relocate: ', REPLACE(r.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ut.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Unit type: ', REPLACE(ut.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.market_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Market rate: ', REPLACE(px.market_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.minimum_required_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Minimum required rate: ', REPLACE(px.minimum_required_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.owning_user, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Owning user: ', REPLACE(px.owning_user, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.package::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Package: ', REPLACE(px.package::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_company, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Previous company: ', REPLACE(px.previous_company, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_job_title, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Previous job_title: ', REPLACE(px.previous_job_title, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.salary::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Salary: ', REPLACE(px.salary::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(p.contact_subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contact subject: ', REPLACE(p.contact_subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(p.contacted_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contacted by: ', REPLACE(p.contacted_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(p.contacted_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contacted on: ', REPLACE(p.contacted_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.note, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate note: ', E'\n', REPLACE(px.note, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_comment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate comment: ', E'\n', REPLACE(px.person_comment, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.job_notes, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Job note: ', E'\n', REPLACE(REPLACE(px.job_notes, '\x0d\x0a', ' '), '\x0a', ' ')) END
	) note
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
	LEFT JOIN "user" u ON px.id_user = u.id_user
	LEFT JOIN country co ON px.address_private_country = co.id_country
	LEFT JOIN county_state cs ON px.address_default_county_state = cs.abbreviation AND cs.value <> 'Californian'
	LEFT JOIN contractor_unavailable_reason cur ON px.id_contractor_unavailable_reason_string = cur.id_contractor_unavailable_reason
	LEFT JOIN "location" l ON px.id_location_string = l.id_location
	LEFT JOIN person_origin po ON px.id_person_origin_string = po.id_person_origin
	LEFT JOIN person_status ps ON px.id_person_status_string = ps.id_person_status
	LEFT JOIN previous_candidate pc ON px.id_previous_candidate_string = pc.id_previous_candidate
	LEFT JOIN processing_reason pr ON px.id_processing_reason_string = pr.id_processing_reason
	LEFT JOIN processing_status pst ON px.id_processing_status_string = pst.id_processing_status
	LEFT JOIN relocate r ON px.id_relocate_string = r.id_relocate
	LEFT JOIN cte_join_relocate_location_list jrll ON px.id_person = jrll.id_person
	LEFT JOIN unit_type ut ON px.id_unit_type_string = ut.id_unit_type
	LEFT JOIN preferred_employment_type pet ON px.id_preferred_employment_type_string = pet.id_preferred_employment_type
	LEFT JOIN person_off_limit pol ON px.id_person = pol.id_person
	LEFT JOIN off_limit_type olt ON pol.id_off_limit_type = olt.id_off_limit_type
	LEFT JOIN person_relation perr ON px.id_person = perr.id_person
	LEFT JOIN person_relation_type perrt ON perr.id_person_relation_type = perrt.id_person_relation_type
	LEFT JOIN person_rating prat ON px.id_person_rating_string = prat.id_person_rating
)
SELECT
	candidate_id "candidate-externalId",
	company_id,
	first_name "candidate-firstName",
	middle_name "candidate-middleName",
	last_name "candidate-lastName",
	employment_type,
	dob "candidate-dob",
	REPLACE(address, ',,', ',') "candidate-address",
	city "candidate-city",
	state "candidate-State",
	country "candidate-Country",
	postal_code "candidate-zipCode",
	mobile_phone "candidate-mobile",
	primary_phone "candidate-phone",
	home_phone "candidate-homePhone",
	work_phone "candidate-workPhone",
	CASE
		WHEN rn_email <> 1 THEN OVERLAY(email PLACING concat('DUP_', CASE WHEN rn_email = 2 THEN '' ELSE rn_email::text END, '_') from 1 for 0)
		ELSE email
	END "candidate-email",
	work_email "candidate-workEmail",
	owner_email "candidate-owners",
	current_salary "candidate-currentSalary",
	contract_rate "candidate-contractRate",
	title1 "candidate-jobTitle1",
	employer_org_name1 "candidate-employer1",
	COALESCE(start_date1, '') "candidate-startDate1",
	COALESCE(end_date1, '') "candidate-endDate1",
	title2 "candidate-jobTitle2",
	employer_org_name2 "candidate-employer2",
	COALESCE(end_date2, '') "candidate-endDate2",
	note "candidate-note"
	-- resume "candidate-resume",
FROM cte_candidate
WHERE rn = 1
AND company_id IN (SELECT id_company FROM company)