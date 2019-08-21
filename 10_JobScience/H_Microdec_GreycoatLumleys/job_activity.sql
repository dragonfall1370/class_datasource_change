WITH event_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '123'
)
SELECT
e.opportunity_ref job_id,
et.description activity_type,
CONCAT_WS(
	E'\n',
	CASE WHEN NULLIF(et.description, '') IS NOT NULL THEN CONCAT('Event type: ', et.description) END,
	CONCAT('Author: ', p.last_name, ' ', p.first_name),
	CASE WHEN NULLIF(p.email_address, '') IS NOT NULL THEN CONCAT('Email: ', p.email_address) END,
	CASE WHEN NULLIF(e.subject, '') IS NOT NULL THEN CONCAT('Subject: ', e.subject) END,
	CASE WHEN NULLIF(e.notes, '') IS NOT NULL THEN CONCAT('Activity notes: ', E'\n', e.notes) END
) AS content,
CONCAT(e.event_date, ' ', e.event_time)::TIMESTAMP insert_timestamp,
'comment' AS category,
'job' AS type,
-10 AS user_account_id

FROM event e
LEFT JOIN person p ON e.create_user = p.person_ref
LEFT JOIN event_type et ON e.type = et.code
WHERE NULLIF(e.opportunity_ref::TEXT, '') IS NOT NULL
AND NULLIF(e.event_date, '') IS NOT NULL