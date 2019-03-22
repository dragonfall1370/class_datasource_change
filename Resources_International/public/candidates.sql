WITH emails AS (
	SELECT
	c.contact_id,
	e.value email
	FROM contacts c
	LEFT JOIN email_addresses e ON c.contact_id = e.target_entity_id
	WHERE e.is_primary = 't'
),
candidate AS (
SELECT
c.contact_id contact_id,
coalesce(replace(c.first_name,'?',''), 'Unknown') first_name,
COALESCE(c.middle_name, '') middle_name,
coalesce(replace(c.last_name,'?',''), 'Unknown') last_name,
ROW_NUMBER() OVER(PARTITION BY c.first_name, c.last_name ORDER BY c.contact_id) rn_name,
COALESCE(salary, 0) salary,
CONCAT(
c.primary_document_attachment_id, '_',
COALESCE(a.file_name, 'No file') 
) resume,
COALESCE(nick_name, '') nick_name,
CASE
	WHEN e.email LIKE '%@%' THEN e.email
	WHEN e.email IS NULL OR e.email NOT LIKE '%@%' THEN concat(c.contact_id, '@no_email.com')
END email,
pn.value phone,
ROW_NUMBER() OVER(PARTITION BY e.email ORDER BY c.contact_id) rn_email,
COALESCE(strip_tags(description), '') note
FROM contacts c
LEFT JOIN emails e ON c.contact_id = e.contact_id
LEFT JOIN attachments a ON c.primary_document_attachment_id = a.attachment_id
LEFT JOIN contact_sources cs ON c.contact_source_id = cs.contact_source_id
LEFT JOIN phone_numbers pn ON c.contact_id = pn.target_entity_id AND pn.is_primary = 't'
WHERE c.record_type IN ('Candidate', 'Candidate, Sales/Client Contact')
)

SELECT
contact_id "candidate-externalId",
first_name "candidate-firstName",
middle_name "candidate-middleName",
CASE 
	WHEN rn_name <> 1 THEN concat(last_name, ' ', rn_name) 
	ELSE last_name
END "candidate-Lastname",
-- rn_name,
salary "candidate-currentSalary",
resume "candidate-resume",
nick_name "candidate-preferredName",
CASE
	WHEN rn_email <> 1 THEN OVERLAY(email PLACING rn_email::text from strpos(email, '@') for 0)
	ELSE email
END "candidate-email",
-- rn_email,
phone "candidate-phone",
note "candidate-note"

FROM candidate