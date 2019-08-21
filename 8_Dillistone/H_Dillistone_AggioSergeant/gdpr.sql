WITH cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn,
	concat(
		CASE WHEN NULLIF(TRIM(REPLACE(cl.processing_reason_value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason: ', REPLACE(cl.processing_reason_value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(pst.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing status: ', REPLACE(pst.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cl.person_title, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Person title: ', REPLACE(cl.person_title, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cl.first_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('First name: ', REPLACE(cl.first_name, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cl.last_name, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Last name: ', REPLACE(cl.last_name, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cl.created_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Created by: ', REPLACE(cl.created_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cl.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Created on: ', REPLACE(cl.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.processing_reason_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason on: ', REPLACE(px.processing_reason_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(px.processing_reason_log, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Processing reason log: ', REPLACE(px.processing_reason_log, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(cl.reason_log, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Reason log: ', E'\n', REPLACE(REPLACE(cl.reason_log, '\x0d\x0a', ' '), '\x0a', ' ')) END
	) gdpr_note
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
	LEFT JOIN processing_status pst ON px.id_processing_status_string = pst.id_processing_status
	LEFT JOIN compliance_log cl ON px.id_person = cl.id_person
)
SELECT
candidate_id,
gdpr_note
FROM cte_candidate
WHERE rn = 1
AND gdpr_note <> ''
-- LIMIT 100