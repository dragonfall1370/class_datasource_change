WITH split_relocate_location_list AS (
	SELECT 
	id_person, 
	s.relocate_location
	FROM person_x px, UNNEST(string_to_array(px.id_relocate_location_string_list, ',')) s(relocate_location)
),
relocate_location AS (
	SELECT
	id_person,
	l.value relocate_location
	FROM split_relocate_location_list srll
	LEFT JOIN "location" l ON srll.relocate_location = l.id_location
),
cte_join_relocate_location_list AS (
	SELECT 
	id_person, 
	string_agg(relocate_location, ', ') relocate_location_list
	FROM relocate_location 
	GROUP BY id_person
),
cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn,
	concat(
		CASE WHEN NULLIF(TRIM(REPLACE(e.education_establishment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Education establishment: ', REPLACE(e.education_establishment, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(e.education_subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Education subject: ', REPLACE(e.education_subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(q.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Qualification: ', REPLACE(q.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(e.education_from, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Education from: ', REPLACE(e.education_from, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(e.education_to, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Education to: ', REPLACE(e.education_to, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(e.checked_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Checked by: ', REPLACE(e.checked_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(e.checked_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Checked on: ', REPLACE(e.checked_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(e.notes, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Notes: ', E'\n', REPLACE(REPLACE(e.notes, '\x0d\x0a', ' '), '\x0a', ' ')) END
	) edu_summary
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
	LEFT JOIN education e ON c.id_person = e.id_person
	LEFT JOIN qualification q ON e.id_qualification = q.id_qualification
)
SELECT
candidate_id,
edu_summary
FROM cte_candidate
WHERE rn = 1
AND edu_summary <> ''
-- LIMIT 100