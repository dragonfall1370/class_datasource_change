WITH candidate AS (
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
languages AS (
	SELECT
		person_ref,
		CASE
			WHEN l.description = 'Arabic' THEN 'ar'
			WHEN l.description = 'Chinese' THEN 'zh_HK'
			WHEN l.description = 'Czech' THEN 'cs'
			WHEN l.description = 'Dutch' THEN 'nl'
			WHEN l.description = 'French' THEN 'fr'
			WHEN l.description = 'German' THEN 'de'
			WHEN l.description = 'Greek' THEN 'el'
			WHEN l.description = 'Hungarian' THEN 'hu'
			WHEN l.description = 'Italian' THEN 'it'
			WHEN l.description = 'Japanese' THEN 'ja'
			WHEN l.description = 'Mandarin' THEN 'zh'
			WHEN l.description = 'Polish' THEN 'pl'
			WHEN l.description = 'Portuguese' THEN 'pt'
			WHEN l.description = 'Russian' THEN 'ru'
			WHEN l.description = 'Spanish' THEN 'es'
			WHEN l.description = 'Swedish' THEN 'sv'
			WHEN l.description = 'Hindi' THEN 'hi'
		END AS languages
	FROM search_code sc
	JOIN (SELECT code, description FROM lookup WHERE code_type = '1030') l ON sc.code = l.code
	WHERE sc.code_type = '1030'
	AND person_ref IS NOT NULL
),
candidate_language AS (
SELECT
	c.person_ref candidate_id,
	CONCAT('{"languageCode":"', languages, '","level":""}') languages 
FROM candidate c
JOIN languages l ON c.person_ref = l.person_ref
WHERE c.rn = 1
)
SELECT
	candidate_id,
	CONCAT('[', string_agg(languages, ','), ']') languages
FROM candidate_language
GROUP BY candidate_id