WITH cte_contact AS (
	SELECT
	cp.id_person contact_id,
	ROW_NUMBER() OVER(PARTITION BY cp.id_person ORDER BY cp.sort_order ASC, cp.employment_from DESC, cp.is_default_role) rn
	FROM company_person cp
)

SELECT 
cc.*,
UPPER(d.new_document_name) contact_document,
'CONTACT' entity_type,
'documents' document_type

FROM cte_contact cc
LEFT JOIN entity_document ed ON cc.contact_id = ed.entity_id
LEFT JOIN "document" d ON ed.id_document = d.id_document
WHERE rn = 1
AND d.new_document_name IS NOT NULL