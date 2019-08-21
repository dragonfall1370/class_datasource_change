WITH cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
)

SELECT 
cc.*,
UPPER(d.new_document_name) candidate_document,
'CANDIDATE' entity_type,
'resume' document_type

FROM cte_candidate cc
LEFT JOIN entity_document ed ON cc.candidate_id = ed.entity_id
LEFT JOIN "document" d ON ed.id_document = d.id_document
WHERE rn = 1
AND d.new_document_name IS NOT NULL