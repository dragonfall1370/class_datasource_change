WITH activity AS(
SELECT
c.contact_id candidate_id,
e.created_on::timestamp date_added,
CONCAT(
CASE
	WHEN "from" IS NULL THEN ''
	ELSE concat('From: ', "from", E'\n')
END,
CASE
	WHEN "to" IS NULL THEN ''
	ELSE concat('To: ', "to", E'\n')
END,
CASE
	WHEN "c_c" IS NULL THEN ''
	ELSE concat('cc: ', "c_c", E'\n')
END,
CASE
	WHEN subject IS NULL THEN ''
	ELSE concat('Subject: ', subject, E'\n')
END,
CASE
	WHEN verb_id IS NULL THEN ''
	ELSE concat('Verbid: ', verb_id, E'\n')
END,
CASE
	WHEN "AttachmentId_Type" IS NULL THEN ''
	ELSE concat('Attachment type: ', "AttachmentId_Type", E'\n')
END,
CASE
	WHEN display IS NULL THEN ''
	ELSE concat('Content: ', E'\n', CASE
																		WHEN verb_id = 'Merge' THEN regexp_replace(REPLACE(REPLACE(REPLACE(display, '[', ''),'"', ''), ']', ''), E'[\\n\\r]+', ' ', 'g')
																		ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(regexp_replace(REPLACE(REPLACE(REPLACE(REPLACE(display, '["', ''), ' ["', ''), '"]', ''),'\r\n', chr(13)), E'<[^>]*>', '', 'gi'), '&nbsp;', ' '), '&ndash;', '-'), '&rsquo;', ''''), '&lt;', '<'), '&gt;', '>')
																	END)
END
) "content"
FROM experiences e
LEFT JOIN contacts c ON e.parent_id = c.contact_id
WHERE "ParentId_Type" = 'Contacts'
AND c.record_type IN ('Candidate', 'Candidate, Sales/Client Contact')
)

-- select count(*) from activity

SELECT
candidate_id,
cast('-10' as int) as user_account_id,
date_added,
'comment' as category,
'candidate' as type,
"content"
FROM activity
ORDER BY date_added DESC
-- limit 100