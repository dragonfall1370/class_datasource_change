WITH contact_record_status AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		CASE
			WHEN email_address NOT LIKE '%@%' THEN NULL
			WHEN STRPOS(email_address, '''') = 1 AND RIGHT(email_address, 1) = '''' THEN LEFT(RIGHT(email_address, LENGTH(email_address) - 1), LENGTH(RIGHT(email_address, LENGTH(email_address) - 1)) - 1)
			ELSE email_address
		END AS contact_email,
		p.*
	FROM position p
	LEFT JOIN lookup l ON p.record_status = l.code
	WHERE code_type = '132'
	AND NULLIF(person_ref, '0') IS NOT NULL
),
current_contact AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY TRIM(LOWER(contact_email)) ORDER BY CASE WHEN contact_status = '1' THEN 1 ELSE 2 END) rn_email
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
contact_owner AS (
	SELECT
		person_ref,
		type,
		s.email_address
	FROM person_type pt
	LEFT JOIN staff s ON pt.person_type_ref = s.person_type_ref
	WHERE pt.type LIKE '%Z%'
	AND s.email_address LIKE '%@%'
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
contact_languages AS (
	SELECT person_ref,
	string_agg(l.description, ', ') languages
	FROM search_code sc
	LEFT JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE person_ref IS NOT NULL
	AND sc.code_type = '1030'
	GROUP BY person_ref
),
client_rating AS (
	SELECT
		person_ref,
		string_agg(l.description, ', ') rating
	FROM search_code sc
	JOIN lookup l ON sc.code_type = l.code_type AND sc.code = l.code
	WHERE sc.code_type = '1040'
	AND person_ref IS NOT NULL
	GROUP BY person_ref
)

------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
cc.person_ref AS "contact-externalId",
COALESCE(o.organisation_ref, '1') AS "contact-companyId",
COALESCE(p.first_name, 'Unknown') AS "contact-firstName",
COALESCE(p.last_name, 'Unknown') AS "contact-lastName",
p.salutation AS preferred_name,
REGEXP_REPLACE(cc.telephone_number, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') AS "contact-phone",
CONCAT_WS( ', ', REGEXP_REPLACE(cc.mobile_telno, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g'), REGEXP_REPLACE(p.mobile_telno, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g')) AS mobile,
CASE
	WHEN cc.rn_email > 1 THEN OVERLAY(cc.contact_email PLACING CONCAT('DUP_', CASE WHEN rn_email = 2 THEN '' ELSE (rn_email - 1)::text END) from 1 for 0)
	ELSE cc.contact_email
END AS "contact-email",
CASE
	WHEN STRPOS(p.email_address, '''') = 1 AND RIGHT(p.email_address, 1) = '''' THEN LEFT(RIGHT(p.email_address, LENGTH(p.email_address) - 1), LENGTH(RIGHT(p.email_address, LENGTH(p.email_address) - 1)) - 1)
	ELSE p.email_address
END AS personal_email,
cc.displayname "contact-jobTitle",
co.email_address "contact-owners",
CONCAT_WS(
	E'\n',
	CASE WHEN p.title IS NOT NULL THEN CONCAT('Title: ', CASE	
																													WHEN TRIM(p.title) = 'MRS' THEN 'Mrs'
																													WHEN TRIM(p.title) = 'MS' THEN 'Ms'
																													WHEN TRIM(p.title) = 'MIss' THEN 'Miss'
																													ELSE TRIM(p.title)
																											 END) END,
	CASE WHEN p.gender IS NOT NULL THEN CONCAT('Gender: ', CASE WHEN p.gender = 'M' THEN 'Male' WHEN p.gender = 'F' THEN 'Female' END) END,
	CASE WHEN COALESCE(cwa.location_name, cl.description) IS NOT NULL THEN CONCAT('Contact personal location: ', COALESCE(cwa.location_name, cl.description)) END,
	CASE WHEN cwa.address IS NOT NULL THEN CONCAT('Contact personal address: ', cwa.address) END,
	CASE WHEN ad.displayname IS NOT NULL THEN CONCAT('Contact work location: ', ad.displayname) END,
	CASE WHEN COALESCE(ad.address_line_1, ad.address_line_2, ad.address_line_3, ad.post_town, ad.county_state, ad.zipcode, ad.country) IS NOT NULL THEN
	CONCAT('Contact work address: ',
		CONCAT_WS(
				', ',
				CASE WHEN RIGHT(TRIM(ad.address_line_1), 1) = ',' THEN LEFT(TRIM(ad.address_line_1), LENGTH(TRIM(ad.address_line_1)) - 1) ELSE TRIM(ad.address_line_1) END, 
				CASE WHEN RIGHT(TRIM(ad.address_line_2), 1) = ',' THEN LEFT(TRIM(ad.address_line_2), LENGTH(TRIM(ad.address_line_2)) - 1) ELSE TRIM(ad.address_line_2) END,
				CASE WHEN RIGHT(TRIM(ad.address_line_3), 1) = ',' THEN LEFT(TRIM(ad.address_line_3), LENGTH(TRIM(ad.address_line_3)) - 1) ELSE TRIM(ad.address_line_3) END,
				ad.post_town,
				ad.county_state,
				ad.zipcode,
				ad.country
			)
	) END,
	CASE WHEN ct.description IS NOT NULL THEN CONCAT('Contact type: ', ct.description) END,
	CASE WHEN cs.description IS NOT NULL THEN CONCAT('Contact status: ', cs.description) END,
	CASE WHEN cr.rating IS NOT NULL THEN CONCAT('Client ratings: ', cr.rating) END,	
	CASE WHEN p.z_last_contact_action IS NOT NULL THEN CONCAT('Last action: ', p.z_last_contact_action) END,
	CASE WHEN d.description IS NOT NULL THEN CONCAT('Department: ', d.description) END,
	CASE WHEN p2.displayname IS NOT NULL THEN CONCAT('Manager: ', p2.displayname) END,
	CASE WHEN cc.start_date IS NOT NULL THEN CONCAT('Start date: ', cc.start_date) END,
	CASE WHEN cc.end_date IS NOT NULL THEN CONCAT('End date: ', cc.end_date) END,
	CASE WHEN rt.description IS NOT NULL THEN CONCAT('Responsible team: ', rt.description) END,
	CASE WHEN cla.languages IS NOT NULL THEN CONCAT('Languages: ', cla.languages) END,
	CASE WHEN cc.notes IS NOT NULL THEN CONCAT('Note: ', cc.notes) END
) "contact-Note"

FROM current_contact cc
JOIN person p ON cc.person_ref = p.person_ref
LEFT JOIN organisation o ON cc.organisation_ref = o.organisation_ref
LEFT JOIN contact_work_address cwa ON cc.person_ref = cwa.contact_id AND cwa.rn = 1
LEFT JOIN contact_location cl ON cc.person_ref = cl.person_ref AND cl.rn = 1
LEFT JOIN contact_owner co ON cc.responsible_user = co.person_ref
LEFT JOIN contact_type ct ON cc.type = ct.code
LEFT JOIN department d ON cc.department = d.code
LEFT JOIN person p2 ON cc.manager_person_ref = p2.person_ref
LEFT JOIN contact_status cs ON cc.contact_status = cs.code
LEFT JOIN responsible_team rt ON cc.responsible_team = rt.code
LEFT JOIN contact_languages cla ON cc.person_ref = cla.person_ref
LEFT JOIN address ad ON cc.address_ref = ad.address_ref
LEFT JOIN client_rating cr ON cc.person_ref = cr.person_ref
WHERE o.record_status IN ('L', 'D', 'Z')