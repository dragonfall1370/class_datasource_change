WITH split_candidate_fe AS (
	SELECT
	id_person person_id,
	LOWER(TRIM(s.functional_expertise_id)) functional_expertise_id
	FROM person_x px, UNNEST(string_to_array(px.id_job_function_string_list, ',')) s(functional_expertise_id)
),
candidate_fe AS (
	SELECT
	person_id,
	jf.value functional_expertise
	FROM split_candidate_fe scf
	LEFT JOIN job_function jf ON scf.functional_expertise_id = jf.id_job_function
),
job_fe AS (
	SELECT 
	a.id_assignment job_id,
	jf.value functional_expertise
	FROM "assignment" a
	JOIN assignment_code ac ON a.id_assignment = ac.id_assignment
	JOIN job_function jf ON ac.code_id = jf.id_job_function
),
distinct_fe AS (
SELECT DISTINCT functional_expertise 
FROM candidate_fe
UNION
SELECT DISTINCT functional_expertise
FROM job_fe
)
SELECT ROW_NUMBER() OVER() id,
*,
CURRENT_TIMESTAMP insert_timestamp
FROM distinct_fe