WITH split_qualification AS (
	SELECT
	id_person person_id,
	LOWER(TRIM(s.qualification_id)) qualification_id
	FROM person_x px, UNNEST(string_to_array(px.id_qualification_string_list, ',')) s(qualification_id)
),
cte_qualification AS (
	SELECT
	person_id candidate_id,
	q.value skill
	FROM split_qualification sq
	LEFT JOIN qualification q ON sq.qualification_id = q.id_qualification
),
split_language AS (
	SELECT
	id_person person_id,
	LOWER(TRIM(s.language_id)) language_id
	FROM person_x px, UNNEST(string_to_array(px.id_language_string_list, ',')) s(language_id)
),
cte_language AS (
	SELECT
	person_id candidate_id,
	l.value skill
	FROM split_language sl
	LEFT JOIN language l ON sl.language_id = l.id_language
),
total_skills AS (
SELECT 
	candidate_id,
	skill
FROM cte_qualification
UNION
SELECT
	candidate_id,
	skill
FROM cte_language
)
SELECT
	candidate_id,
	string_agg(skill, ', ') skill
FROM total_skills
GROUP BY candidate_id