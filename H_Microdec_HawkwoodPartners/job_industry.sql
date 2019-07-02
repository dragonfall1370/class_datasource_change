WITH job_industry AS (
	SELECT
		sc.opportunity_ref job_id,
		sc.code industry_code,
		ROW_NUMBER() OVER(PARTITION BY sc.opportunity_ref ORDER BY sc.search_code_ref) rn
	FROM search_code sc
	JOIN opportunity o ON sc.opportunity_ref = o.opportunity_ref
	WHERE sc.opportunity_ref IS NOT NULL
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
	ci.job_id,
	TRIM(i.description)	industry,
	CURRENT_TIMESTAMP insert_timestamp
FROM job_industry ci
JOIN industry i ON ci.industry_code = i.code
WHERE ci.rn = 1
ORDER BY ci.job_id