WITH cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn,
	REPLACE(px.job_title, '\x0d\x0a', '') title1,
	REPLACE(px.company_name, '\x0d\x0a', '') employer_org_name1,
	to_char(px.from_date::DATE, 'YYYY-MM-DD') start_date1,
	to_char(px.to_date::DATE, 'YYYY-MM-DD') end_date1,
	REPLACE(px.previous_job_title, '\x0d\x0a', '') title2,
	REPLACE(px.previous_company, '\x0d\x0a', '') employer_org_name2,
	to_char(px.previous_company_to_date::DATE, 'YYYY-MM-DD') end_date2
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
),
convert_to_json AS (
SELECT
candidate_id,
concat(
	CASE
		WHEN title1 IS NOT NULL OR employer_org_name1 IS NOT NULL OR start_date1 IS NOT NULL OR end_date1 IS NOT NULL THEN
		concat('[', json_build_object('jobTitle', COALESCE(title1, ''), 'currentEmployer', COALESCE(employer_org_name1, ''), 'dateRangeFrom', COALESCE(start_date1, ''), 'dateRangeTo', COALESCE(end_date1, '')), 
					CASE WHEN title2 IS NOT NULL OR employer_org_name2 IS NOT NULL OR end_date2 IS NOT NULL THEN ',' END)
		WHEN (title1 IS NULL AND employer_org_name1 IS NULL AND start_date1 IS NULL AND end_date1 IS NULL) AND (title2 IS NOT NULL OR employer_org_name2 IS NOT NULL OR end_date2 IS NOT NULL) THEN '['
	END,
	CASE
		WHEN title2 IS NOT NULL OR employer_org_name2 IS NOT NULL OR end_date2 IS NOT NULL THEN
		concat(json_build_object('jobTitle', COALESCE(title2, ''), 'currentEmployer', COALESCE(employer_org_name2, ''), 'dateRangeFrom', '', 'dateRangeTo', COALESCE(end_date2, '')), ']') 
		WHEN (title2 IS NULL AND employer_org_name2 IS NULL AND end_date2 IS NULL) AND (title1 IS NOT NULL OR employer_org_name1 IS NOT NULL OR start_date1 IS NOT NULL OR end_date1 IS NOT NULL) THEN ']'
	END
) AS data
FROM cte_candidate
WHERE rn = 1
)
SELECT *
FROM convert_to_json
WHERE data <> ''