WITH candidate_type AS (
	SELECT
	ROW_NUMBER() OVER(PARTITION BY pt.person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
	p.*,
	c.*,
	pt.status,
	pt.availability_confirmed,
	pt.status_reason
	FROM candidate c
	LEFT JOIN person_type pt ON c.person_type_ref = pt.person_type_ref
	LEFT JOIN person p ON pt.person_ref = p.person_ref
	LEFT JOIN lookup l ON pt.type = l.code
	WHERE l.code_type = '104'
	AND l.code IN ('A', 'C')
),
check_dup_email AS (
	SELECT ROW_NUMBER() OVER(PARTITION BY TRIM(LOWER(email_address)) ORDER BY person_ref) rn_email,
	*
	FROM candidate_type
	WHERE rn = 1
),
candidate_source AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '102'
),
candidate_address AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN main_address = 'Y' THEN 1 ELSE 2 END) AS rn
	FROM address
	WHERE person_ref IS NOT NULL
),
responsible_team AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '2'
),
candidate_status AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '105'
),
candidate_location AS (
	SELECT 
		person_ref,
		l.description,
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY sc.update_timestamp DESC) rn
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE person_ref IS NOT NULL
	AND sc.code_type = '1020'
	AND l.description IS NOT NULL
),
candidate_skill AS (
	SELECT person_ref,
	string_agg(l.description, ', ') skills
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE person_ref IS NOT NULL
	AND sc.code_type IN ('1015', '1030')
	GROUP BY person_ref
),
current_position AS (
	SELECT
		person_ref,
		p.displayname job_title,
		o.displayname company,
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn
	FROM position p
	LEFT JOIN lookup l ON p.record_status = l.code
	LEFT JOIN organisation o ON p.organisation_ref = o.organisation_ref
	WHERE code_type = '132'
	AND NULLIF(person_ref, '0') IS NOT NULL
),
candidate_owner AS (
	SELECT
		person_ref,
		type,
		s.email_address
	FROM person_type pt
	LEFT JOIN staff s ON pt.person_type_ref = s.person_type_ref
	WHERE pt.type LIKE '%Z%'
	AND s.email_address LIKE '%@%'
),
candidate_work_history AS (
	SELECT
		cde.person_ref candidate_id,
		string_agg(
			CONCAT_WS(
				E'\n',
				CASE WHEN p.displayname IS NOT NULL THEN CONCAT('Position: ', p.displayname) END,
				CASE WHEN o.displayname IS NOT NULL THEN CONCAT('Organisation: ', o.displayname) END,
				CASE WHEN p.start_date IS NOT NULL THEN CONCAT('Start date: ', p.start_date) END,
				CASE WHEN l.description IS NOT NULL THEN CONCAT('Status: ', l.description) END
			), E'\n-----------------------------------------------\n'
		) AS work_history
	FROM check_dup_email cde
	LEFT JOIN position p ON cde.person_ref = p.person_ref
	LEFT JOIN organisation o ON p.organisation_ref = o.organisation_ref
	LEFT JOIN lookup l ON p.record_status = l.code
	WHERE l.code_type = '132'
	AND COALESCE(p.displayname, o.displayname) IS NOT NULL
	GROUP BY candidate_id
)

------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
cde.person_ref AS "candidate-externalId",
COALESCE(cde.first_name, 'Unknown') AS "candidate-firstName",
COALESCE(cde.last_name, 'Unknown') AS "candidate-lastName",
CASE
	WHEN TRIM(cde.title) = 'Dr' THEN 'DR'
	WHEN TRIM(cde.title) = 'Miss' THEN 'MISS'
	WHEN TRIM(cde.title) = 'MIss' THEN 'MISS'
	WHEN TRIM(cde.title) = 'Mr' THEN 'MR'
	WHEN TRIM(cde.title) = 'Mrs' THEN 'MRS'
	WHEN TRIM(cde.title) = 'MRS' THEN 'MRS'
	WHEN TRIM(cde.title) = 'Ms' THEN 'MS'
	WHEN TRIM(cde.title) = 'MS' THEN 'MS'
END AS "candidate-title",
cde.salutation AS preferred_name,
to_char(cde.date_of_birth::DATE, 'YYYY-MM-DD') AS "candidate-dob",
pos.job_title "candidate-jobTitle1",
pos.company "candidate-employer1",
pos.company "candidate-company1",
cwh.work_history,
cs.description AS candidate_source,
a.post_town AS "candidate-city",
a.county_state AS "candidate-State",
a.zipcode AS "candidate-zipCode",
CASE
	WHEN a.country = 'United Kingdom' THEN 'GB'
	WHEN a.country = 'Ireland' THEN 'IE'
	WHEN a.country = 'New Zealand' THEN 'NZ'
	WHEN a.country = 'USA' THEN 'US'
END AS "candidate-Country",
CONCAT_WS(
	', ',
	CASE WHEN RIGHT(TRIM(a.address_line_1), 1) = ',' THEN LEFT(TRIM(a.address_line_1), LENGTH(TRIM(a.address_line_1)) - 1) ELSE TRIM(a.address_line_1) END, 
	CASE WHEN RIGHT(TRIM(a.address_line_2), 1) = ',' THEN LEFT(TRIM(a.address_line_2), LENGTH(TRIM(a.address_line_2)) - 1) ELSE TRIM(a.address_line_2) END,
	CASE WHEN RIGHT(TRIM(a.address_line_3), 1) = ',' THEN LEFT(TRIM(a.address_line_3), LENGTH(TRIM(a.address_line_3)) - 1) ELSE TRIM(a.address_line_3) END,
	a.post_town,
	a.county_state,
	a.country
) AS "candidate-address",
REGEXP_REPLACE(cde.mobile_telno, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') AS "candidate-phone",
REGEXP_REPLACE(cde.mobile_telno, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') AS "candidate-mobile",
REGEXP_REPLACE(cde.zc_day_telno, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') AS "candidate-workPhone",
CASE
	WHEN rn_email > 1 THEN OVERLAY(cde.email_address PLACING CONCAT('DUP_', CASE WHEN rn_email = 2 THEN '' ELSE rn_email::text END) from 1 for 0)
	ELSE cde.email_address 
END AS "candidate-email",
co.email_address AS "candidate-owners",
REGEXP_REPLACE(cde.package_required, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~£]','','g')::int AS "candidate-desiredSalary",
csk.skills "candidate-skills",
CONCAT_WS(
	E'\n',
	CASE WHEN cde.z_last_candidate_action IS NOT NULL THEN CONCAT('Last action: ', cde.z_last_candidate_action) END,
	CASE WHEN cl.description IS NOT NULL THEN CONCAT('Location: ', cl.description) END,
	CASE WHEN cde.part_time IS NOT NULL THEN CASE WHEN cde.part_time = 'Y' THEN 'Do not mailshot (check box): Yes' ELSE 'Do not mailshot (check box): No' END END,
	CASE WHEN rt.description IS NOT NULL THEN CONCAT('Responsible team: ', rt.description) END,
	CASE WHEN cde.cv_last_updated IS NOT NULL THEN CONCAT('CV last updated: ', cde.cv_last_updated) END,
	CASE WHEN cde.notice_period IS NOT NULL THEN CONCAT('Notice period: ', cde.notice_period) END,
	CASE WHEN cde.notice_period_mode IS NOT NULL THEN CONCAT('Notice period mode: ', cde.notice_period_mode) END,
	CASE WHEN cde.date_available IS NOT NULL THEN CONCAT('Available date: ', cde.date_available) END,
	CASE WHEN cde.seeking IS NOT NULL THEN CONCAT('Seeking: ', cde.seeking) END,
	CASE WHEN cde.income_required IS NOT NULL THEN CONCAT('Required income: ', cde.income_required) END,
	CASE WHEN cde.income_mode IS NOT NULL THEN CONCAT('Income mode: ', cde.income_mode) END,
	CASE WHEN cde.package_value_reqd IS NOT NULL THEN CONCAT('Package value: ', cde.package_value_reqd) END,
	CASE WHEN cst.description IS NOT NULL THEN CONCAT('Candidate status: ', cst.description) END,
	CASE WHEN cde.status_reason IS NOT NULL THEN CONCAT('Status reason: ', cde.status_reason) END,
	CASE WHEN cde.first_time IS NOT NULL THEN CASE WHEN cde.first_time = 'Y' THEN 'First time: Yes' ELSE 'First time: No' END END,
	CASE WHEN cde.part_time IS NOT NULL THEN CASE WHEN cde.part_time = 'Y' THEN 'Part time: Yes' ELSE 'Part time: No' END END,
	CASE WHEN cde.full_time IS NOT NULL THEN CASE WHEN cde.full_time = 'Y' THEN 'Full time: Yes' ELSE 'Full time: No' END END,
	CASE WHEN cde.notes IS NOT NULL THEN CONCAT('Note: ', cde.notes) END
	
) AS "candidate-note"

FROM check_dup_email cde
LEFT JOIN candidate_source cs ON cde.source = cs.code
LEFT JOIN candidate_address a ON cde.person_ref = a.person_ref AND a.rn = 1
LEFT JOIN candidate_owner co ON cde.responsible_user = co.person_ref
LEFT JOIN responsible_team rt ON cde.responsible_team = rt.code
LEFT JOIN candidate_status cst ON cde.status = cst.code
LEFT JOIN candidate_location cl ON cde.person_ref = cl.person_ref AND cl.rn = 1
LEFT JOIN candidate_skill csk ON cde.person_ref = csk.person_ref
LEFT JOIN current_position pos ON cde.person_ref = pos.person_ref AND pos.rn = 1
LEFT JOIN candidate_work_history cwh ON cde.person_ref = cwh.candidate_id
WHERE cde.person_ref = '14713'