WITH current_contact AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY pos.person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		pos.*
	FROM position pos
	LEFT JOIN lookup l ON pos.record_status = l.code
	JOIN person p ON pos.person_ref = p.person_ref
	WHERE code_type = '132'
	ORDER BY pos.person_ref
),
contact_company AS (
	SELECT
		org.organisation_ref AS company_id,
		cc.person_ref AS contact_id,
		l.description,
		ROW_NUMBER() OVER(PARTITION BY org.organisation_ref ORDER BY l.code::int ASC) rn
	FROM organisation org
	LEFT JOIN current_contact cc ON cc.organisation_ref = org.organisation_ref
	LEFT JOIN lookup l ON cc.contact_status = l.code
	WHERE code_type = '135'
	AND cc.rn = 1
),
default_contact AS (
	SELECT
		o.organisation_ref company_id,
		cc.contact_id,
		ROW_NUMBER() OVER(PARTITION BY contact_id ORDER BY company_id) rn
	FROM opportunity o
	LEFT JOIN contact_company cc ON o.organisation_ref = cc.company_id
	WHERE cc.contact_id IS NULL
)
------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
company_id AS "contact-companyId",
MAX(COALESCE(contact_id, rn::text)) AS "contact-externalId",
'Default' "contact-firstName",
'Default' "contact-lastName"
FROM default_contact
GROUP BY company_id