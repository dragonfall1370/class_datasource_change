WITH contact_record_status AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		p.*
	FROM position p
	LEFT JOIN lookup l ON p.record_status = l.code
	WHERE code_type = '132'
),
current_contact AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY TRIM(email_address) ORDER BY create_timestamp DESC) rn_email
	FROM contact_record_status
	WHERE rn = 1
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
contact_fe AS (
	SELECT
		sc.person_ref contact_id,
		sc.code_type,
		ROW_NUMBER() OVER(PARTITION BY sc.position_ref, sc.code ORDER BY sc.position_ref) rn,
		jt.job_type sub_fe
	FROM search_code sc
	JOIN job_type jt ON sc.code = jt.code
	JOIN current_contact cc ON sc.position_ref = cc.position_ref
	WHERE sc.position_ref IS NOT NULL
		AND sc.code_type = '1010'
		
	UNION ALL
	
	SELECT
		sc.person_ref contact_id,
		sc.code_type,
		ROW_NUMBER() OVER(PARTITION BY sc.position_ref, sc.code ORDER BY sc.person_ref) rn,
		s.skills sub_fe
	FROM search_code sc
	JOIN skills s ON sc.code = s.code
	JOIN current_contact cc ON sc.position_ref = cc.position_ref
	WHERE sc.position_ref IS NOT NULL
		AND sc.code_type = '1015'
),
contact_fe_sub_fe AS (
SELECT
	contact_id,
	CASE
		WHEN code_type = '1010' THEN 'Job Type'
		WHEN code_type = '1015' THEN 'Skills'
-- 		WHEN code_type = '1025' THEN 'Qualifications'
	END fe,
	sub_fe,
	CURRENT_TIMESTAMP insert_timestamp
FROM contact_fe
WHERE rn = 1
AND sub_fe IS NOT NULL
)

SELECT *
FROM contact_fe_sub_fe
-- WHERE NULLIF(sub_fe, '') IS NOT NULL