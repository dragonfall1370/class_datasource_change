WITH contact_record_status AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		p.*
	FROM position p
	LEFT JOIN lookup l ON p.record_status = l.code
	WHERE code_type = '132'
	AND NULLIF(person_ref, '0') IS NOT NULL
),
current_contact AS (
	SELECT *
	FROM contact_record_status
	WHERE rn = 1
), 
contact_industry AS (
	SELECT
		sc.person_ref contact_id,
		sc.code industry_code,
		ROW_NUMBER() OVER(PARTITION BY sc.person_ref, sc.code ORDER BY sc.person_ref) rn
	FROM search_code sc
	JOIN current_contact cc ON sc.person_ref = cc.person_ref
	WHERE sc.person_ref IS NOT NULL
	AND sc.code_type = '1005'
),
industry AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '1005'
)
SELECT
	ci.contact_id,
	TRIM(i.description) industry,
	CURRENT_TIMESTAMP insert_timestamp
FROM contact_industry ci
JOIN industry i ON ci.industry_code = i.code
WHERE ci.rn = 1
ORDER BY ci.contact_id