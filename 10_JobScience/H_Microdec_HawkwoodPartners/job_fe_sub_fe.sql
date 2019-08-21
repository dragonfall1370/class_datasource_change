WITH job_type AS (
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
job_fe AS (
	SELECT
		sc.opportunity_ref job_id,
		sc.code_type,
		ROW_NUMBER() OVER(PARTITION BY sc.opportunity_ref, sc.code ORDER BY sc.opportunity_ref) rn,
		jt.job_type sub_fe
	FROM search_code sc
	JOIN job_type jt ON sc.code = jt.code
	JOIN opportunity o ON sc.opportunity_ref = o.opportunity_ref
	WHERE sc.opportunity_ref IS NOT NULL
		AND sc.code_type = '1010'
		
	UNION ALL
	
	SELECT
		sc.opportunity_ref job_id,
		sc.code_type,
		ROW_NUMBER() OVER(PARTITION BY sc.opportunity_ref, sc.code ORDER BY sc.opportunity_ref) rn,
		jt.job_type sub_fe
	FROM search_code sc
	JOIN job_type jt ON sc.code = jt.code
	JOIN opportunity o ON sc.opportunity_ref = o.opportunity_ref
	WHERE sc.opportunity_ref IS NOT NULL
		AND sc.code_type = '1015'
),
job_fe_sub_fe AS (
SELECT
	job_id,
	CASE
		WHEN code_type = '1010' THEN 'Job Type'
		WHEN code_type = '1015' THEN 'Skills'
-- 		WHEN code_type = '1025' THEN 'Qualifications'
	END fe,
	sub_fe,
	CURRENT_TIMESTAMP insert_timestamp
FROM job_fe
WHERE rn = 1
AND sub_fe IS NOT NULL
)

SELECT *
FROM job_fe_sub_fe
-- WHERE NULLIF(sub_fe, '') IS NOT NULL