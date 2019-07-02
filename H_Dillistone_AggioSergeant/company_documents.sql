WITH cte_company AS (
	SELECT
	c.id_company company_id,
	UPPER(d.new_document_name) company_document
	
	FROM company c
	JOIN entity_document ed ON c.id_company = ed.entity_id
	JOIN "document" d ON ed.id_document = d.id_document
)

SELECT 
*,
'COMPANY' entity_type,
'legal_document' document_type

FROM cte_company
WHERE company_document IS NOT NULL