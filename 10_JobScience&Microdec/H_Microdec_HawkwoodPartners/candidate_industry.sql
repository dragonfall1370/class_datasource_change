WITH candidate_type AS (
	SELECT
	ROW_NUMBER() OVER(PARTITION BY pt.person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
	p.*,
	c.*,
	pt.status,
	CASE
		WHEN pt.type = 'C' THEN 'PERMANENT'
		WHEN pt.type = 'A' THEN 'TEMPORARY'
	END AS candidate_type,
	pt.availability_confirmed,
	pt.status_reason
	FROM candidate c
	LEFT JOIN person_type pt ON c.person_type_ref = pt.person_type_ref
	JOIN person p ON pt.person_ref = p.person_ref
	LEFT JOIN lookup l ON pt.type = l.code
	WHERE l.code_type = '104'
	AND l.code IN ('A', 'C')
),
candidate_industry AS (
	SELECT
		sc.person_ref candidate_id,
		sc.code industry_code,
		ROW_NUMBER() OVER(PARTITION BY sc.person_ref, sc.code ORDER BY sc.person_ref) rn
	FROM search_code sc
	JOIN candidate_type ct ON sc.person_ref = ct.person_ref AND rn = 1
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
	ci.candidate_id,
	i.description industry,
	CURRENT_TIMESTAMP insert_timestamp
FROM candidate_industry ci
JOIN industry i ON ci.industry_code = i.code
WHERE ci.rn = 1
ORDER BY ci.candidate_id