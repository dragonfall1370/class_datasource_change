WITH cte_candidate AS (
	SELECT
		c.record_type_id,
		c.contact_id candidate_id,
		COALESCE(c.first_name, 'Unknown') first_name,
		c.middle_name,
		COALESCE(c.last_name, 'Unkown') last_name,
		CASE
			WHEN c.salutation IN ('Mr.') THEN 'MR'
			WHEN c.salutation = 'Mrs.' THEN 'MRS'
			WHEN c.salutation = 'Miss.' THEN 'MISS'
			WHEN c.salutation = 'Ms.' THEN 'MS'
			WHEN c.salutation IN ('Doctor', 'Dr.', 'Dr') THEN 'DR'
			WHEN c.salutation IN ('M', 'ESQ.', 'II') THEN ''
			ELSE c.salutation
		END candidate_title,
		CASE 
			WHEN RIGHT(c.mailing_street, 1) = ',' OR RIGHT(c.mailing_street, 1) = '.' THEN LEFT(c.mailing_street, CHAR_LENGTH(c.mailing_street) - 1)
			ELSE REPLACE(c.mailing_street, ' .', '')
		END address,
		c.mailing_city,
		c.mailing_state,
		CASE
			WHEN c.mailing_country = 'Cnr Peter Place and Main Road Bryanston' THEN ''
			WHEN c.mailing_country IN ('Cape Town', 'AF', 'South Africa') THEN 'ZA'
			WHEN c.mailing_country = 'United Arab Emirates' THEN 'AE'
			WHEN c.mailing_country = 'Botswana' THEN 'BW'
			WHEN c.mailing_country IN ('USA', 'United States of America') THEN 'US'
			WHEN c.mailing_country = 'Tunisia' THEN 'TN'
			WHEN c.mailing_country = 'United Kingdom' THEN 'GB'
			WHEN c.mailing_country = 'Hong Kong' THEN 'HK'
			ELSE c.mailing_country
		END,
		CASE
			WHEN CHAR_LENGTH(c.mailing_postal_code) > 10 THEN regexp_replace(c.mailing_postal_code, '\D','','g')
			ELSE c.mailing_postal_code
		END postal_code,
		regexp_replace(c.phone, '[a-z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') phone,
		regexp_replace(c.mobile_phone, '[a-z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') primary_phone,
		regexp_replace(c.home_phone, '[a-z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') home_phone,
		CASE
			WHEN c.email IS NULL OR c.email NOT LIKE '%@%' THEN concat(c.contact_id, '@no_email.com')
			ELSE c.email
		END email,
		ROW_NUMBER() OVER(PARTITION BY c.email ORDER BY c.contact_id) rn_email,
		c.title,
		c.owner_id,
		u.email owner_email,
		to_char(c.birthdate::DATE, 'YYYY-MM-DD') dob,
		c.candidate_source, -- injection
		c.current_salary::int,
		c.desired_salary::int,
-- 		c.edu_degree_name1, --injection
-- 		c.edu_degree_name2, --injection
-- 		c.edu_degree_type1, --injection
-- 		c.edu_degree_type2, --injection
-- 		c.education_school1, --injection
-- 		c.education_school2, --injection
		c.employer1_title,
		c.employer2_title,
		c.employer3_title,
		c.employer_org_name1,
		c.employer_org_name2,
		c.employer_org_name3,
		c.experience_summary,
-- 		c.legacy_document_id resume,
		concat(
			CASE WHEN best_fit IS NULL THEN '' ELSE concat('Best fit: ', best_fit, E'\n') END,
			CASE WHEN executivesummary IS NULL THEN '' ELSE concat('Executive summary: ', executivesummary, E'\n') END,
			CASE WHEN jobs_applied_for IS NULL THEN '' ELSE concat('Job applied for: ', jobs_applied_for) END
		) note
	FROM contact c
	LEFT JOIN "user" u ON c.owner_id = u.user_id
	WHERE c.record_type_id = '01261000000gXaw'
	AND c.is_deleted = 0
)

SELECT
candidate_id "candidate-externalId",
first_name "contact-firstName",
middle_name "contact-middleName",
last_name "contact-lastName",
candidate_title "candidate-title",
address "candidate-address",
mailing_city "candidate-city",
mailing_state "candidate-State",
mailing_country "candidate-Country",
postal_code "candidate-zipCode",
primary_phone "candidate-mobile",
primary_phone "candidate-phone",
CASE 
	WHEN home_phone is not null AND phone is not null THEN concat(phone, ' / ', home_phone) 
	ELSE COALESCE(phone, home_phone)
END "candidate-homePhone",
CASE
	WHEN rn_email <> 1 THEN OVERLAY(email PLACING rn_email::text from strpos(email, '@') for 0)
	ELSE email
END "candidate-email",
owner_email "candidate-owners",
dob "candidate-dob",
current_salary "candidate-currentSalary",
desired_salary "candidate-desiredSalary",
title "candidate-jobTitle1",
employer2_title "candidate-jobTitle2",
employer3_title "candidate-jobTitle3",
employer_org_name1 "candidate-employer1",
employer_org_name2 "candidate-employer2",
employer_org_name3 "candidate-employer3",
experience_summary "candidate-workHistory",
-- resume "candidate-resume",
note "candidate-note"
FROM cte_candidate
-- where rn_email <> 1