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
check_dup_email AS (
	SELECT ROW_NUMBER() OVER(PARTITION BY TRIM(LOWER(email_address)) ORDER BY person_ref) rn_email,
	*
	FROM candidate_type
	WHERE rn = 1
	AND person_ref IS NOT NULL
),
candidate_status AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '105'
),
income_mode AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '7'
)

------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
cde.person_ref AS candidate_id,
candidate_type,
im.description income_mode,
CASE
	WHEN im.description IN ('per hour', 'per week') THEN cde.income_required::INT
END desire_contract_rate,
CASE
	WHEN im.description = 'per month' THEN cde.income_required::INT * 12
	WHEN im.description = 'per annum' THEN cde.income_required::INT
END desire_salary,
CASE
	WHEN im.description = 'per month' THEN cde.income_required::INT
END month_salary,
CASE
	WHEN im.description = 'per month' THEN 2
	WHEN im.description = 'per annum' THEN 1
END salary_type

FROM check_dup_email cde
LEFT JOIN income_mode im ON cde.income_mode = im.code
WHERE NULLIF(cde.income_required, '0') IS NOT NULL