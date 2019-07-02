WITH cte_job AS (
	SELECT
	a.id_assignment job_id,
	ROW_NUMBER() OVER(PARTITION BY a.id_assignment ORDER BY ac.contacted_on ASC) rn
	FROM assignment_contact ac
	JOIN "assignment" a ON ac.id_assignment = a.id_assignment AND a.is_deleted = 0
	WHERE a.is_deleted = 0
)

SELECT
job_id,
UPPER(d.new_document_name) job_document,
'POSITION' entity_type,
'documents' document_type
FROM cte_job j
LEFT JOIN entity_document ed ON j.job_id = ed.entity_id
LEFT JOIN "document" d ON ed.id_document = d.id_document
WHERE rn = 1
AND d.new_document_name IS NOT NULL