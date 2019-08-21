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
talent_pool AS (
	SELECT 
		object_ref AS  candidate_id,
		CASE
			WHEN savelist_ref IN ('115814', '112854') THEN 'thuntley@hamlinknight.co.uk'
			WHEN savelist_ref IN ('113916', '116598') THEN 'pberry@hamlinknight.co.uk'
			WHEN savelist_ref IN ('116735') THEN 'charlotte.clarke@hamlinknight.co.uk'
			WHEN savelist_ref IN ('114125', '115874', '114210', '116469', '116729') THEN 'cbarnes@hamlinknight.co.uk'
			WHEN savelist_ref IN ('112842') THEN 'suky.rahim@hamlinknight.co.uk'
		END AS consultant,
		CASE
			WHEN savelist_ref = '115814' THEN 'IMMEDIATELY AVAILABLES'
			WHEN savelist_ref = '112854' THEN 'PERM CANDIDATES -TAMS'
			WHEN savelist_ref = '113916' THEN 'PERM CANDIDATES PB'
			WHEN savelist_ref = '116598' THEN 'BEZZA TEMPS'
			WHEN savelist_ref = '116735' THEN 'Charlotte Candidates'
			WHEN savelist_ref = '114125' THEN 'Available Temps'
			WHEN savelist_ref = '115874' THEN 'Nat''s Candidates'
			WHEN savelist_ref = '114210' THEN 'Available Perm'
			WHEN savelist_ref = '116469' THEN 'Person Finder (Quick search results)'
			WHEN savelist_ref = '116729' THEN 'NS Candidates'
			WHEN savelist_ref = '112842' THEN 'temp log candidates'
		END AS group_name
	FROM savelist_entry
	WHERE savelist_ref IN ( '115814', '112854', '113916', '116598', '116735', '114125', '115874', '114210', '116469', '116729', '112842')
)
------------------------------------------------- Main query --------------------------------------------------------------------------
SELECT
cde.person_ref AS candidate_id,
tp.consultant,
tp.group_name,
CURRENT_TIMESTAMP insert_timestamp
FROM check_dup_email cde
JOIN candidate_status cst ON cde.status = cst.code
JOIN talent_pool tp ON cde.person_ref = tp.candidate_id
WHERE cst.code IN ('A1', 'I1')
