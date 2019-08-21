WITH main_company AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY TRIM(LOWER(name)) ORDER BY create_timestamp) rn
	FROM organisation
),
company_address AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY organisation_ref ORDER BY CASE WHEN main_address = 'Y' THEN 1 ELSE 2 END) AS rn
	FROM address
	WHERE organisation_ref IS NOT NULL
),
responsible_team AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '2'
),
organisation_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '110'
),
parent AS (
	SELECT DISTINCT organisation_ref parent_id,
	name
	FROM organisation
),
organisation_source AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '108'
),
company_owner AS (
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
c.organisation_ref AS "company-externalId",
COALESCE(
	CASE
		WHEN c.rn = 1 THEN c.name
		ELSE CONCAT(c.displayname, ' ', c.rn)
	END, 
'No Company Name') AS "company-name",
CASE
	WHEN c.web_site_url IN ('no website', 'No website') THEN NULL
	WHEN STRPOS(c.web_site_url, 'http') = 0 THEN CASE
																									WHEN STRPOS(c.web_site_url, 'www') = 0 THEN CONCAT('http://www.', c.web_site_url)
																									ELSE CONCAT('http://', c.web_site_url)
																								END
	ELSE c.web_site_url
END "company-website",
REGEXP_REPLACE(a.telephone_number, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') "company-phone",
REGEXP_REPLACE(a.telephone_number, '[a-zA-Z!#$%&*,-./:;<=>?@[\]^_`{|}~]','','g') "company-switchBoard",
co.email_address AS "company-owners",
COALESCE(
	CASE WHEN RIGHT(TRIM(a.address_line_1), 1) = ',' THEN LEFT(TRIM(a.address_line_1), LENGTH(TRIM(a.address_line_1)) - 1) ELSE TRIM(a.address_line_1) END, 
	CASE WHEN RIGHT(TRIM(a.address_line_2), 1) = ',' THEN LEFT(TRIM(a.address_line_2), LENGTH(TRIM(a.address_line_2)) - 1) ELSE TRIM(a.address_line_2) END,
	CASE WHEN RIGHT(TRIM(a.address_line_3), 1) = ',' THEN LEFT(TRIM(a.address_line_3), LENGTH(TRIM(a.address_line_3)) - 1) ELSE TRIM(a.address_line_3) END
) "company-locationName",
a.post_town "company-locationCity",
a.county_state "company-locationState",
a.zipcode "company-locationZipCode",
CASE
	WHEN a.country = 'United Kingdom' THEN 'GB'
	WHEN a.country = 'Ireland' THEN 'IE'
	WHEN a.country = 'New Zealand' THEN 'NZ'
END AS "company-locationCountry",
CONCAT_WS(
	', ',
	CASE WHEN RIGHT(TRIM(a.address_line_1), 1) = ',' THEN LEFT(TRIM(a.address_line_1), LENGTH(TRIM(a.address_line_1)) - 1) ELSE TRIM(a.address_line_1) END, 
	CASE WHEN RIGHT(TRIM(a.address_line_2), 1) = ',' THEN LEFT(TRIM(a.address_line_2), LENGTH(TRIM(a.address_line_2)) - 1) ELSE TRIM(a.address_line_2) END,
	CASE WHEN RIGHT(TRIM(a.address_line_3), 1) = ',' THEN LEFT(TRIM(a.address_line_3), LENGTH(TRIM(a.address_line_3)) - 1) ELSE TRIM(a.address_line_3) END,
	a.post_town,
	a.county_state,
	a.country
) AS "company-locationAddress",
CASE
	WHEN main_address = 'Y' THEN CONCAT_WS(', ',
																					CASE WHEN RIGHT(TRIM(a.address_line_1), 1) = ',' THEN LEFT(TRIM(a.address_line_1), LENGTH(TRIM(a.address_line_1)) - 1) ELSE TRIM(a.address_line_1) END, 
																					CASE WHEN RIGHT(TRIM(a.address_line_2), 1) = ',' THEN LEFT(TRIM(a.address_line_2), LENGTH(TRIM(a.address_line_2)) - 1) ELSE TRIM(a.address_line_2) END,
																					CASE WHEN RIGHT(TRIM(a.address_line_3), 1) = ',' THEN LEFT(TRIM(a.address_line_3), LENGTH(TRIM(a.address_line_3)) - 1) ELSE TRIM(a.address_line_3) END,
																					a.post_town,
																					a.county_state,
																					a.country
																				)
END "company-headQuarter",
-- l.website_url "company-linkedin",
CONCAT_WS(
	E'\n',
	CASE WHEN c.z_last_action IS NOT NULL THEN CONCAT('Last action: ', c.z_last_action) END,
	CASE WHEN pa.name IS NOT NULL THEN CONCAT('Parent: ', pa.name) END,
	CASE WHEN os.description IS NOT NULL THEN CONCAT('Source: ', os.description) END,
	CASE WHEN ot.description IS NOT NULL THEN CONCAT('Organisation type: ', ot.description) END
-- 	CASE WHEN rt.description IS NOT NULL THEN CONCAT('Responsible team: ', rt.description) END
-- 	CASE WHEN c.company_reg_no IS NOT NULL THEN CONCAT('Company registration number: ', c.company_reg_no) END,
) AS "company-note"

FROM main_company c
LEFT JOIN parent pa ON c.parent_organ_ref = pa.parent_id
LEFT JOIN company_owner co ON c.responsible_user = co.person_ref
LEFT JOIN company_address a ON c.organisation_ref = a.organisation_ref AND a.rn = 1
-- LEFT JOIN linkedin_url l ON c.organisation_ref = l.parent_object_ref
LEFT JOIN responsible_team rt ON c.responsible_team = rt.code
LEFT JOIN organisation_type ot ON c.type = ot.code
LEFT JOIN organisation_source os ON c.source = os.code
