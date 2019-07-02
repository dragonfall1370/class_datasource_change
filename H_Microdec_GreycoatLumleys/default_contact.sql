WITH current_contact AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY pos.person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		pos.*
	FROM position pos
	LEFT JOIN (SELECT code, description FROM lookup WHERE code_type = '132') l ON pos.record_status = l.code
	JOIN person p ON pos.person_ref = p.person_ref
),
contact_company AS (
	SELECT
		org.organisation_ref AS company_id,
		cc.person_ref AS contact_id,
		l.description,
		ROW_NUMBER() OVER(PARTITION BY org.organisation_ref ORDER BY cc.contact_status::INT ASC) rn
	FROM organisation org
	LEFT JOIN current_contact cc ON cc.organisation_ref = org.organisation_ref
	LEFT JOIN (SELECT code, description FROM lookup WHERE code_type = '135') l ON cc.contact_status = l.code
	WHERE cc.rn = 1
)
SELECT count(*) FROM contact_company WHERE rn = 1
,
default_contact AS (
	SELECT o.organisation_ref AS company_id,
	ROW_NUMBER() OVER(PARTITION BY o.organisation_ref ORDER BY o.organisation_ref) rn
	FROM opportunity o
	LEFT JOIN contact_company cc ON o.organisation_ref = cc.company_id AND rn = 1
	WHERE cc.contact_id IS NULL
)
SELECT count(*) FROM default_contact
,
distinct_default_contact AS (
	SELECT 
		company_id
	FROM default_contact
	ORDER BY company_id::INT
),
generate_default_contact AS (
	SELECT
		company_id,
		ROW_NUMBER() OVER() AS contact_id
	FROM distinct_default_contact
)

------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
company_id AS "contact-companyId",
contact_id AS "contact-externalId",
'Default' "contact-firstName",
'Default' "contact-lastName"
FROM generate_default_contact