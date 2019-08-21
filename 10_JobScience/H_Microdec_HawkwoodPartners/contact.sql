WITH contact_record_status AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		p.*
	FROM position p
	LEFT JOIN lookup l ON p.record_status = l.code
	WHERE code_type = '132'
),
current_contact AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY TRIM(email_address) ORDER BY create_timestamp DESC) rn_email
	FROM contact_record_status
	WHERE rn = 1
),
contact_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '131'
),
department AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '299'
),
contact_status AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '135'
),
responsible_team AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '2'
),
contact_location AS (
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
contact_work_address AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY c.person_ref ORDER BY CASE WHEN main_address = 'Y' THEN 1 ELSE 2 END) rn,
		c.person_ref contact_id,
		a.main_address,
		a.displayname location_name,
		CONCAT_WS(
			', ',
			CASE WHEN RIGHT(TRIM(a.address_line_1), 1) = ',' THEN LEFT(TRIM(a.address_line_1), LENGTH(TRIM(a.address_line_1)) - 1) ELSE TRIM(a.address_line_1) END, 
			CASE WHEN RIGHT(TRIM(a.address_line_2), 1) = ',' THEN LEFT(TRIM(a.address_line_2), LENGTH(TRIM(a.address_line_2)) - 1) ELSE TRIM(a.address_line_2) END,
			CASE WHEN RIGHT(TRIM(a.address_line_3), 1) = ',' THEN LEFT(TRIM(a.address_line_3), LENGTH(TRIM(a.address_line_3)) - 1) ELSE TRIM(a.address_line_3) END,
			a.post_town,
			a.county_state,
			a.zipcode,
			a.country
		) AS address,
		a.telephone_number
		
	FROM current_contact c
	LEFT JOIN address a ON c.person_ref = a.person_ref
	WHERE COALESCE(address_line_1, address_line_2, address_line_3, post_town, county_state, zipcode, country) IS NOT NULL
	ORDER BY contact_id,
					CASE
						WHEN main_address = 'Y' THEN 1
						ELSE 2
					END
),
contact_owner AS (
	SELECT
		person_ref,
		type,
		s.email_address
	FROM person_type pt
	LEFT JOIN staff s ON pt.person_type_ref = s.person_type_ref
	WHERE pt.type LIKE '%Z%'
	AND s.email_address LIKE '%@%'
)

------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
cc.person_ref "contact-externalId",
COALESCE(o.organisation_ref, '1') "contact-companyId",
COALESCE(p.first_name, 'Unknown') "contact-firstName",
COALESCE(p.last_name, 'Unknown') "contact-lastName",
CASE
	WHEN p.title = 'Mr' THEN 'Mr.'
	WHEN p.title = 'Miss' THEN 'Miss.'
	WHEN p.title = 'Ms' THEN 'Ms.'
	WHEN p.title = 'Mrs' THEN 'Mrs.'
END gender_title,
p.salutation preferred_name,
REGEXP_REPLACE(cc.telephone_number, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') "contact-phone",
REGEXP_REPLACE(COALESCE(cc.mobile_telno, p.mobile_telno), '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') mobile,
CASE
	WHEN cc.rn_email > 1 THEN OVERLAY(cc.email_address PLACING CONCAT('DUP_', CASE WHEN rn_email = 2 THEN '' ELSE rn_email::text END) from 1 for 0)
	ELSE cc.email_address
END "contact-email",
p.email_address personal_email,
cc.displayname "contact-jobTitle",
co.email_address "contact-owners",

CONCAT_WS(
	E'\n',
	CASE WHEN p.gender IS NOT NULL THEN CONCAT('Gender: ', CASE WHEN p.gender = 'M' THEN 'Male' WHEN p.gender = 'F' THEN 'Female' END) END,
	CASE WHEN ct.description IS NOT NULL THEN CONCAT('Contact type: ', ct.description) END,
	CASE WHEN cs.description IS NOT NULL THEN CONCAT('Contact status: ', cs.description) END,	
	CASE WHEN COALESCE(cwa.location_name, cl.description) IS NOT NULL THEN CONCAT('Contact location: ', COALESCE(cwa.location_name, cl.description)) END,
	CASE WHEN cwa.address IS NOT NULL THEN CONCAT('Contact address: ', cwa.address) END,
	CASE WHEN p.z_last_contact_action IS NOT NULL THEN CONCAT('Last action: ', p.z_last_contact_action) END,
	CASE WHEN d.description IS NOT NULL THEN CONCAT('Department: ', d.description) END,
	CASE WHEN p2.displayname IS NOT NULL THEN CONCAT('Manager: ', p2.displayname) END,
	CASE WHEN cc.start_date IS NOT NULL THEN CONCAT('Start date: ', cc.start_date) END,
	CASE WHEN cc.end_date IS NOT NULL THEN CONCAT('End date: ', cc.end_date) END,
	CASE WHEN rt.description IS NOT NULL THEN CONCAT('Responsible team: ', rt.description) END,
	CASE WHEN cc.notes IS NOT NULL THEN CONCAT('Note: ', cc.notes) END
) "contact-Note"

FROM current_contact cc
LEFT JOIN organisation o ON cc.organisation_ref = o.organisation_ref
JOIN person p ON cc.person_ref = p.person_ref
LEFT JOIN contact_owner co ON cc.responsible_user = co.person_ref
LEFT JOIN contact_type ct ON cc.type = ct.code
LEFT JOIN contact_location cl ON cc.person_ref = cl.person_ref AND cl.rn = 1
LEFT JOIN department d ON cc.department = d.code
LEFT JOIN person p2 ON cc.manager_person_ref = p2.person_ref
LEFT JOIN contact_status cs ON cc.contact_status = cs.code
LEFT JOIN responsible_team rt ON cc.responsible_team = rt.code
LEFT JOIN contact_work_address cwa ON cc.person_ref = cwa.contact_id AND cwa.rn = 1