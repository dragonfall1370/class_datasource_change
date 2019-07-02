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
)

------------------------------------------------- Main query --------------------------------------------------------------------------

SELECT
cst.code,
cst.description,
cde.person_ref AS candidate_id,
cde.first_name,
cde.last_name,
1 explicit_consent,
3 exercise_right,
1 request_through,
CURRENT_TIMESTAMP request_through_date,
5 obtained_through,
CURRENT_TIMESTAMP obtained_through_date,
1 expire,
'2024-05-01 00:00:00'::TIMESTAMP expire_date,
-10 obtained_by,
1 portal_status,
CURRENT_TIMESTAMP insert_timestamp

FROM check_dup_email cde
JOIN candidate_status cst ON cde.status = cst.code
-- WHERE cst.code IN ('A1', 'I1')
WHERE cst.code = 'A1'