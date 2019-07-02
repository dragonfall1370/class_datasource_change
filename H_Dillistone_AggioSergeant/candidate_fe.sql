WITH split_fe AS (
	SELECT
	id_person person_id,
	LOWER(TRIM(s.functional_expertise_id)) functional_expertise_id
	FROM person_x px, UNNEST(string_to_array(px.id_job_function_string_list, ',')) s(functional_expertise_id)
),
cte_functional_expertise AS (
	SELECT
	person_id candidate_id,
	jf.value functional_expertise
	FROM split_fe sf
	LEFT JOIN job_function jf ON sf.functional_expertise_id = jf.id_job_function
)
SELECT *,
CURRENT_TIMESTAMP insert_timestamp
FROM cte_functional_expertise