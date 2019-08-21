WITH cte_industry_id AS (
	SELECT 
	id_person person_id, 
	TRIM(s.industry) industry_id
	FROM person_x px, UNNEST(string_to_array(px.id_industry_string_list, ',')) s(industry)
),
cte_industry AS (
	SELECT
	person_id candidate_id,
	i.value industry
	FROM cte_industry_id ii
	JOIN industry i ON ii.industry_id = i.id_industry
)

SELECT *,
CURRENT_TIMESTAMP insert_timestamp
FROM cte_industry