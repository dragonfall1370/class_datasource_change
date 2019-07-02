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
cte_state AS (
	SELECT
		abbreviation,
		value
	FROM county_state
	WHERE value NOT IN ('Californian', 'Arkansasian', 'GeorgiaN')
),
cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn,
	TRIM(px.first_name) first_name,
	TRIM(px.middle_name) middle_name,
	TRIM(px.last_name) last_name,
	to_char(px.date_of_birth::DATE, 'YYYY-MM-DD') dob,
	CASE
		WHEN RIGHT(TRIM(replace(px.address_private_full, '\x0d\x0a', ', ')),1) = ',' THEN LEFT(TRIM(replace(px.address_private_full, '\x0d\x0a', ', ')), CHAR_LENGTH(TRIM(replace(px.address_private_full, '\x0d\x0a', ', '))) - 1) 
		ELSE REPLACE(REPLACE(TRIM(replace(px.address_private_full, '\x0d\x0a', ', ')), ',,', ','), ',   , ', '')
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
			WHEN px.address_private_country = 'Belgium' THEN 'BE'
			WHEN px.address_private_country = 'Switzerland' THEN 'CH'
			WHEN px.address_private_country = 'China' THEN 'CN'
			WHEN px.address_private_country = 'Portugal' THEN 'PT'
			WHEN px.address_private_country = 'Hong Kong' THEN 'HK'
			WHEN px.address_private_country = 'Ukraine' THEN 'UA'
			WHEN px.address_private_country = 'Finland' THEN 'FI'
			WHEN px.address_private_country = 'Chile' THEN 'CL'
			WHEN px.address_private_country = 'Israel' THEN 'IL'
			WHEN px.address_private_country = 'Lithuania' THEN 'LT'
			WHEN px.address_private_country = 'Russia' THEN 'RU'
			WHEN px.address_private_country = 'Denmark' THEN 'DK'
			WHEN px.address_private_country = 'Japan' THEN 'JP'
			WHEN px.address_private_country = 'France' THEN 'FR'
			WHEN px.address_private_country = 'Singapore' THEN 'SG'
			WHEN px.address_private_country = 'Bulgaria' THEN 'BG'
			WHEN px.address_private_country = 'Norway' THEN 'NO'
			WHEN px.address_private_country = 'United Arab Emirates' THEN 'AE'
			WHEN px.address_private_country = 'India' THEN 'IN'
			WHEN px.address_private_country = 'Malta' THEN 'MT'
			WHEN px.address_private_country = 'Sweden' THEN 'SE'
			WHEN px.address_private_country = 'Italy' THEN 'IT'
			WHEN px.address_private_country = 'Luxembourg' THEN 'LU'
			WHEN px.address_private_country = 'Poland' THEN 'PL'
			WHEN px.address_private_country = 'Republic of Ireland' THEN 'IE'
			WHEN px.address_private_country = 'Austria' THEN 'AT'
			WHEN px.address_private_country = 'Iceland' THEN 'IS'
			WHEN px.address_private_country = 'South Korea' THEN 'KR'
			WHEN px.address_private_country = 'Algeria' THEN 'DZ'
			WHEN px.address_private_country = 'Estonia' THEN 'EE'
			WHEN px.address_private_country = 'Taiwan' THEN 'TW'
			WHEN px.address_private_country = 'Mexico' THEN 'MX'
			WHEN px.address_private_country = 'Hungary' THEN 'HU'
			WHEN px.address_private_country = 'Brazil' THEN 'BR'
			WHEN px.address_private_country = 'Slovakia' THEN 'SK'
			WHEN px.address_private_country = 'Argentina' THEN 'AR'
			WHEN px.address_private_country = 'Netherlands' THEN 'NL'
			WHEN px.address_private_country = 'Saudi Arabia' THEN 'SA'
			WHEN px.address_private_country = 'Samoa' THEN 'WS'
			WHEN px.address_private_country = 'Turkey' THEN 'TR'
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
			WHEN co.value = 'Belgium' THEN 'BE'
			WHEN co.value = 'Switzerland' THEN 'CH'
			WHEN co.value = 'China' THEN 'CN'
			WHEN co.value = 'Portugal' THEN 'PT'
			WHEN co.value = 'Hong Kong' THEN 'HK'
			WHEN co.value = 'Ukraine' THEN 'UA'
			WHEN co.value = 'Finland' THEN 'FI'
			WHEN co.value = 'Chile' THEN 'CL'
			WHEN co.value = 'Israel' THEN 'IL'
			WHEN co.value = 'Lithuania' THEN 'LT'
			WHEN co.value = 'Russia' THEN 'RU'
			WHEN co.value = 'Denmark' THEN 'DK'
			WHEN co.value = 'Japan' THEN 'JP'
			WHEN co.value = 'France' THEN 'FR'
			WHEN co.value = 'Singapore' THEN 'SG'
			WHEN co.value = 'Bulgaria' THEN 'BG'
			WHEN co.value = 'Norway' THEN 'NO'
			WHEN co.value = 'United Arab Emirates' THEN 'AE'
			WHEN co.value = 'India' THEN 'IN'
			WHEN co.value = 'Malta' THEN 'MT'
			WHEN co.value = 'Sweden' THEN 'SE'
			WHEN co.value = 'Italy' THEN 'IT'
			WHEN co.value = 'Luxembourg' THEN 'LU'
			WHEN co.value = 'Poland' THEN 'PL'
			WHEN co.value = 'Republic of Ireland' THEN 'IE'
			WHEN co.value = 'Austria' THEN 'AT'
			WHEN co.value = 'Iceland' THEN 'IS'
			WHEN co.value = 'South Korea' THEN 'KR'
			WHEN co.value = 'Algeria' THEN 'DZ'
			WHEN co.value = 'Estonia' THEN 'EE'
			WHEN co.value = 'Taiwan' THEN 'TW'
			WHEN co.value = 'Mexico' THEN 'MX'
			WHEN co.value = 'Hungary' THEN 'HU'
			WHEN co.value = 'Brazil' THEN 'BR'
			WHEN co.value = 'Slovakia' THEN 'SK'
			WHEN co.value = 'Argentina' THEN 'AR'
			WHEN co.value = 'Netherlands' THEN 'NL'
			WHEN co.value = 'Saudi Arabia' THEN 'SA'
			WHEN co.value = 'Samoa' THEN 'WS'
			WHEN co.value = 'Turkey' THEN 'TR'
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
	px.salary::int current_salary,
-- 	px.minimum_required_rate::int contract_rate,
	px.job_title title1,
	px.company_name employer_org_name1,
	to_char(px.from_date::DATE, 'YYYY-MM-DD') start_date1,
	to_char(px.to_date::DATE, 'YYYY-MM-DD') end_date1,
	px.previous_job_title title2,
	px.previous_company employer_org_name2,
	to_char(px.previous_company_to_date::DATE, 'YYYY-MM-DD') end_date2,
	CONCAT_WS(
		E'\n',
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_id::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Candidate external id: ', REPLACE(px.person_id::text, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_reference, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Candidate reference: ', REPLACE(px.person_reference, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.date_of_birth, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Date of birth: ', REPLACE(px.date_of_birth, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.company_name, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Company name: ', REPLACE(px.company_name, '\x0d\x0a', ' ')) END, 
		CASE WHEN NULLIF(TRIM(REPLACE(pet.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Preferred employment type: ', REPLACE(pet.value, '\x0d\x0a', ' ')) END, 
		CASE WHEN NULLIF(TRIM(REPLACE(px.address_default_full, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Full default address: ', REPLACE(px.address_default_full, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(co.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Default country: ', REPLACE(co.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.address_default_post_code, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Default postcode: ', REPLACE(px.address_default_post_code, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.default_url, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Default URL: ', REPLACE(px.default_url, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.direct_line_phone, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Direct line phone: ', REPLACE(px.direct_line_phone, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.mobile_business, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Business mobile: ', REPLACE(px.mobile_business, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.mobile_other, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Other mobile: ', REPLACE(px.mobile_other, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.phone_home, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Home phone: ', REPLACE(px.phone_home, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.phone_other, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Other phone: ', REPLACE(px.phone_other, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.email_work, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Work Email: ', REPLACE(px.email_work, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.email_private, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Private Email: ', REPLACE(px.email_private, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.email_other, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Other email: ', REPLACE(px.email_other, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(l.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Location: ', REPLACE(l.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.nationality_value_string, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Nationality: ', REPLACE(px.nationality_value_string, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(po.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Candidate origin: ', REPLACE(po.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(ps.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Candidate status: ', REPLACE(ps.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_company, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Previous company: ', REPLACE(px.previous_company, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_job_title, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Previous job_title: ', REPLACE(px.previous_job_title, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.note, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Candidate note: ', E'\n', REPLACE(px.note, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.person_comment, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Candidate comment: ', E'\n', REPLACE(px.person_comment, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.job_notes, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Job note: ', E'\n', REPLACE(REPLACE(px.job_notes, '\x0d\x0a', ' '), '\x0a', ' ')) END
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.maiden_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Maiden name: ', REPLACE(px.maiden_name, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.family, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Family: ', REPLACE(px.family, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.created_on, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Created on: ', REPLACE(px.created_on, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.alert_text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Alert text: ', REPLACE(px.alert_text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN px.is_off_limit = 0 THEN concat('Is off limit: No', E'\n') ELSE concat('Is off limit: Yes', E'\n') END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(olt.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Off limit type: ', REPLACE(olt.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_date_from, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Off limit from: ', REPLACE(pol.off_limit_date_from, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_date_to, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Off limit to: ', REPLACE(pol.off_limit_date_to, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(pol.off_limit_note, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Off limit note: ', REPLACE(pol.off_limit_note, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(prat.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Person rating: ', REPLACE(prat.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(perrt.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Person relation type: ', REPLACE(perrt.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(perr.created_on, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Person relation created on: ', REPLACE(perr.created_on, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(perr.created_by, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Person relation created by: ', REPLACE(perr.created_by, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(perr.relation_description, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Person relation description: ', REPLACE(perr.relation_description, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(cur.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Candidate unavailable reason: ', REPLACE(cur.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.international_value_string, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('International: ', REPLACE(px.international_value_string, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(pr.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Processing reason: ', REPLACE(pr.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(pst.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Processing status: ', REPLACE(pst.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.qualification_value_string, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Qualification: ', REPLACE(px.qualification_value_string, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(jrll.relocate_location_list, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Relocate location: ', REPLACE(jrll.relocate_location_list, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(r.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Relocate: ', REPLACE(r.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(ut.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Unit type: ', REPLACE(ut.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.market_rate::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Market rate: ', REPLACE(px.market_rate::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.minimum_required_rate::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Minimum required rate: ', REPLACE(px.minimum_required_rate::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.owning_user, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Owning user: ', REPLACE(px.owning_user, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.package::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Package: ', REPLACE(px.package::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_company, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Previous company: ', REPLACE(px.previous_company, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.previous_job_title, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Previous job_title: ', REPLACE(px.previous_job_title, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(px.salary::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Salary: ', REPLACE(px.salary::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(p.contact_subject, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Contact subject: ', REPLACE(p.contact_subject, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(p.contacted_by, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Contacted by: ', REPLACE(p.contacted_by, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(p.contacted_on, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Contacted on: ', REPLACE(p.contacted_on, '\x0d\x0a', ' ')) END,

	) note
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
	LEFT JOIN "user" u ON px.id_user = u.id_user
	LEFT JOIN country co ON px.address_private_country = co.id_country
	LEFT JOIN cte_state cs ON px.address_default_county_state = cs.abbreviation
	LEFT JOIN "location" l ON px.id_location_string = l.id_location
	LEFT JOIN person_origin po ON px.id_person_origin_string = po.id_person_origin
	LEFT JOIN person_status ps ON px.id_person_status_string = ps.id_person_status
	LEFT JOIN previous_candidate pc ON px.id_previous_candidate_string = pc.id_previous_candidate
	LEFT JOIN preferred_employment_type pet ON px.id_preferred_employment_type_string = pet.id_preferred_employment_type
-- 	LEFT JOIN contractor_unavailable_reason cur ON px.id_contractor_unavailable_reason_string = cur.id_contractor_unavailable_reason
-- 	LEFT JOIN processing_reason pr ON px.id_processing_reason_string = pr.id_processing_reason
-- 	LEFT JOIN processing_status pst ON px.id_processing_status_string = pst.id_processing_status
-- 	LEFT JOIN relocate r ON px.id_relocate_string = r.id_relocate
-- 	LEFT JOIN cte_join_relocate_location_list jrll ON px.id_person = jrll.id_person
-- 	LEFT JOIN unit_type ut ON px.id_unit_type_string = ut.id_unit_type
-- 	LEFT JOIN person_off_limit pol ON px.id_person = pol.id_person
-- 	LEFT JOIN off_limit_type olt ON pol.id_off_limit_type = olt.id_off_limit_type
-- 	LEFT JOIN person_relation perr ON px.id_person = perr.id_person
-- 	LEFT JOIN person_relation_type perrt ON perr.id_person_relation_type = perrt.id_person_relation_type
-- 	LEFT JOIN person_rating prat ON px.id_person_rating_string = prat.id_person_rating
)
SELECT
candidate_id "candidate-externalId",
first_name "candidate-firstName",
middle_name "candidate-middleName",
last_name "candidate-lastName",
dob "candidate-dob",
CASE
	WHEN STRPOS(address, ',') = 1 THEN TRIM(SUBSTRING(address FROM 2 FOR LENGTH(address) - 1))
	ELSE TRIM(address)
END "candidate-address",
STRPOS(address, ','),
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
-- current_salary "candidate-currentSalary",
title1 "candidate-jobTitle1",
employer_org_name1 "candidate-employer1",
note "candidate-note"

-- contract_rate "candidate-contractRate",
-- COALESCE(start_date1, '') "candidate-startDate1",
-- COALESCE(end_date1, '') "candidate-endDate1",
-- title2 "candidate-jobTitle2",
-- employer_org_name2 "candidate-employer2",
-- COALESCE(end_date2, '') "candidate-endDate2",
-- resume "candidate-resume",
FROM cte_candidate
WHERE rn = 1
-- LIMIT 100