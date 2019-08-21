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
job_type AS (
	SELECT
		code,
		description AS job_type
	FROM lookup
	WHERE code_type = '1010'
),
skills AS (
	SELECT
		code,
		description AS skills
	FROM lookup
	WHERE code_type = '1015'
),
qualifications AS (
	SELECT
		code,
		description AS qualifications
	FROM lookup
	WHERE code_type = '1025'
),
candidate_fe AS (
	SELECT
		sc.person_ref candidate_id,
		sc.code_type,
		ROW_NUMBER() OVER(PARTITION BY sc.person_ref, sc.code ORDER BY sc.person_ref) rn,
		jt.job_type sub_fe
	FROM search_code sc
	JOIN job_type jt ON sc.code = jt.code
	JOIN candidate_type c ON sc.person_ref = c.person_ref AND c.rn = 1
	WHERE sc.person_ref IS NOT NULL
		AND sc.code_type = '1010'
		
	UNION ALL
	
	SELECT
		sc.person_ref candidate_id,
		sc.code_type,
		ROW_NUMBER() OVER(PARTITION BY sc.person_ref, sc.code ORDER BY sc.person_ref) rn,
		s.skills sub_fe
	FROM search_code sc
	JOIN skills s ON sc.code = s.code
	JOIN candidate_type c ON sc.person_ref = c.person_ref AND c.rn = 1
	WHERE sc.person_ref IS NOT NULL
		AND sc.code_type = '1015'
		
		UNION ALL
		
	SELECT
		sc.person_ref candidate_id,
		sc.code_type,
		ROW_NUMBER() OVER(PARTITION BY sc.person_ref, sc.code ORDER BY sc.person_ref) rn,
		q.qualifications sub_fe
	FROM search_code sc
	JOIN qualifications q ON sc.code = q.code
	JOIN candidate_type c ON sc.person_ref = c.person_ref AND c.rn = 1
	WHERE sc.person_ref IS NOT NULL
		AND sc.code_type = '1025'
),
candidate_fe_sub_fe AS (
SELECT
	candidate_id,
	CASE
		WHEN code_type = '1010' THEN 'Job Type'
		WHEN code_type = '1015' THEN 'Skills'
		WHEN code_type = '1025' THEN 'Qualifications'
	END fe,
	sub_fe,
	CURRENT_TIMESTAMP insert_timestamp
FROM candidate_fe
WHERE rn = 1
AND sub_fe IS NOT NULL
)

SELECT *
FROM candidate_fe_sub_fe
-- WHERE NULLIF(sub_fe, '') IS NOT NULL