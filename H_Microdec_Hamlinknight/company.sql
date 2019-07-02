WITH main_company AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY LOWER(TRIM(name)) ORDER BY create_timestamp) rn
	FROM organisation
),
company_phone AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY organisation_ref ORDER BY CASE WHEN main_address = 'Y' THEN 1 ELSE 2 END) AS rn
	FROM address
),
linkedin_url AS (
	SELECT
		parent_object_ref,
		website_url
	FROM linksite
	WHERE parent_object_name = 'organisation'
	AND STRPOS(website_url, 'linkedin') > 0
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
c.create_timestamp::TIMESTAMP,
COALESCE(
	CASE
		WHEN c.rn = 1 THEN TRIM(c.name)
		ELSE CONCAT(TRIM(c.displayname), ' ', c.rn)
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
-- p.person_ref,
-- p.email_address ,
co.email_address AS "company-owners",
l.website_url "company-linkedin",
CONCAT_WS(
	E'\n',
	CASE WHEN c.displayname IS NOT NULL THEN CONCAT('Also known as: ', c.displayname) END,
	CASE WHEN c.company_reg_no IS NOT NULL THEN CONCAT('Company registration number: ', c.company_reg_no) END,
	CASE WHEN ot.description IS NOT NULL THEN CONCAT('Organisation type: ', ot.description) END,
	CASE WHEN rt.description IS NOT NULL THEN CONCAT('Responsible team: ', rt.description) END
) AS note

FROM main_company c
-- LEFT JOIN (SELECT person_ref, email_address FROM person WHERE email_address LIKE '%hamlinknight%') p ON c.responsible_user = p.person_ref
LEFT JOIN company_owner co ON c.responsible_user = co.person_ref
LEFT JOIN company_phone a ON c.organisation_ref = a.organisation_ref AND a.rn = 1
LEFT JOIN linkedin_url l ON c.organisation_ref = l.parent_object_ref
LEFT JOIN responsible_team rt ON c.responsible_team = rt.code
LEFT JOIN organisation_type ot ON c.type = ot.code
WHERE c.responsible_user IN ('88610', '233571', '295404', '304691')