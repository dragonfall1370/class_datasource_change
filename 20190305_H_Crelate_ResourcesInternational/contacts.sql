WITH link_company_contact AS (
	SELECT
	c.contact_id,
	ROW_NUMBER() over(PARTITION by c.contact_id ORDER BY a.account_id) rn,
	a.account_id
	FROM contacts c
	LEFT JOIN (select distinct target_entity_id contact_id, where_id account_id FROM timeline_items) l ON c.contact_id = l.contact_id
	LEFT JOIN accounts a on l.account_id = a.account_id
	WHERE c.record_type in ('Lead Contact', 'Sales/Client Contact', 'Candidate, Sales/Client Contact')
),
contact as (
SELECT 
c.contact_id,
CASE	WHEN lcc.account_id IS NULL THEN '1'
	ELSE lcc.account_id
END account_id,
c.first_name,
COALESCE(c.middle_name, '') middle_name,
c.last_name,
pn.value phone,
e.value email,
ROW_NUMBER() OVER(PARTITION BY c.last_name, c.first_name ORDER BY c.contact_id) rn,
CONCAT(c.primary_document_attachment_id, '_', COALESCE(a.file_name, 'No file') ) document_name,
COALESCE(strip_tags(c.description), '') note
-- -- Remove everything from brief except description as customer's requirement
-- concat(
-- 	CASE 
-- 		WHEN c.created_by_id IS NULL THEN '' ELSE concat('Created by: ', u.full_name) 
-- 	END,
-- 	CASE 
-- 		WHEN c.created_on IS NULL THEN '' ELSE concat(E'\n', 'Created on: ', to_char(c.created_on::DATE, 'YYYY-MM-DD')) 
-- 	END,
-- 	CASE 
-- 		WHEN c.salary IS NULL THEN '' ELSE concat(E'\n', 'Salary: ', c.salary) 
-- 	END,
-- 	CASE 
-- 		WHEN c.contact_source_id IS NULL THEN '' ELSE concat(E'\n', 'Contact source: ', cs.name) 
-- 	END,
-- 	CASE 
-- 		WHEN c.last_activity_regarding_id IS NULL THEN '' ELSE concat(E'\n', 'Last activity: ', j.name) 
-- 	END,
-- 	CASE 
-- 		WHEN c.last_activity_date IS NULL THEN '' ELSE concat(E'\n', 'Last activity on: ', to_char(c.last_activity_date::DATE, 'YYYY-MM-DD')) 
-- 	END,
-- 	CASE 
-- 		WHEN c.contact_status_id IS NULL THEN '' ELSE concat(E'\n', 'Contact status: ', c.contact_status_id) 
-- 	END,
-- 	CASE 
-- 		WHEN c.record_type IS NULL THEN '' ELSE concat(E'\n', 'Record type: ', c.record_type) 
-- 	END,
-- 	CASE 
-- 		WHEN strip_tags(c.description) IS NULL THEN '' ELSE concat(E'\n', 'Description: ', strip_tags(c.description)) 
-- 	END
-- ) note

FROM contacts c
LEFT JOIN attachments a ON c.primary_document_attachment_id = a.attachment_id
LEFT JOIN link_company_contact lcc ON c.contact_id = lcc.contact_id
LEFT JOIN phone_numbers pn ON c.contact_id = pn.target_entity_id AND pn.is_primary = 't'
LEFT JOIN email_addresses e ON c.contact_id = e.target_entity_id AND e.is_primary = 't'
-- LEFT JOIN users u ON c.created_by_id = u.user_id
-- LEFT JOIN contact_sources cs ON c.contact_source_id = cs.contact_source_id
-- LEFT JOIN jobs j ON c.last_activity_regarding_id = j.job_id
WHERE c.record_type IN ('Lead Contact', 'Sales/Client Contact', 'Candidate, Sales/Client Contact')
AND lcc.rn = 1
)

SELECT
contact_id "contact-externalId",
account_id "contact-companyId",
first_name "contact-firstName",
middle_name "contact-middleName",
CASE
	WHEN rn <> 1 THEN concat(last_name, ' ', rn) 
	ELSE last_name
END "contact-lastName",
phone "contact-phone",
email "contact-email",
document_name "contact-documentName",
note "contact-Note"
FROM contact
