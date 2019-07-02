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
	CASE
		WHEN pet.value = 'Permanent' THEN '[{"desiredJobTypeId":"1"}]' --permanent
		WHEN pet.value = 'Flex' THEN '[{"desiredJobTypeId":"2"}]' --contract
		WHEN pet.value = 'Permanent/ Flex'THEN '[{"desiredJobTypeId":"1"},{"desiredJobTypeId":"2"}]'
		ELSE '[{"desiredJobTypeId":"0"}]'
	END json_employment_type
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
	LEFT JOIN preferred_employment_type pet ON px.id_preferred_employment_type_string = pet.id_preferred_employment_type
)
SELECT
candidate_id,
json_employment_type
FROM cte_candidate
WHERE rn = 1
-- LIMIT 100