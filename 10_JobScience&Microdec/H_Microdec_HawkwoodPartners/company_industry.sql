WITH company_industry AS (
	SELECT
		sc.organisation_ref company_id,
		sc.code industry_code,
		ROW_NUMBER() OVER(PARTITION BY sc.organisation_ref, sc.code ORDER BY sc.organisation_ref) rn
	FROM search_code sc
	JOIN organisation o ON sc.organisation_ref = o.organisation_ref
	WHERE sc.organisation_ref IS NOT NULL
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
	ci.company_id,
	TRIM(i.description) industry,
	CURRENT_TIMESTAMP insert_timestamp
FROM company_industry ci
JOIN industry i ON ci.industry_code = i.code
WHERE ci.rn = 1
ORDER BY ci.company_id