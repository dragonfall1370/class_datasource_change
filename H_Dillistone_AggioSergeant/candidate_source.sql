WITH cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn,
	po.value source
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
	LEFT JOIN person_origin po ON c.id_person_origin = po.id_person_origin
)
SELECT
candidate_id,
source
FROM cte_candidate
WHERE rn = 1
AND source <> ''
-- LIMIT 100