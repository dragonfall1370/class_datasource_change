WITH contact_record_status AS (
	SELECT
		ROW_NUMBER() OVER(PARTITION BY person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
		CASE
			WHEN email_address NOT LIKE '%@%' THEN NULL
			WHEN STRPOS(email_address, '''') = 1 AND RIGHT(email_address, 1) = '''' THEN LEFT(RIGHT(email_address, LENGTH(email_address) - 1), LENGTH(RIGHT(email_address, LENGTH(email_address) - 1)) - 1)
			ELSE email_address
		END AS contact_email,
		p.*
	FROM position p
	LEFT JOIN lookup l ON p.record_status = l.code
	WHERE code_type = '132'
	AND NULLIF(person_ref, '0') IS NOT NULL
),
current_contact AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY TRIM(LOWER(contact_email)) ORDER BY CASE WHEN contact_status = '1' THEN 1 ELSE 2 END) rn_email
	FROM contact_record_status
	WHERE rn = 1
)
SELECT
parent_object_ref contact_id,
CONCAT(parent_object_ref, '.', l.linkfile_ref, '.', file_extension) document_name,
file_extension,
parent_object_name,
l.displayname,
'CONTACT' entity_type,
'documents' document_type
FROM linkfile l
JOIN current_contact cc ON l.parent_object_ref = cc.person_ref
JOIN person p ON cc.person_ref = p.person_ref
WHERE parent_object_name = 'contact'