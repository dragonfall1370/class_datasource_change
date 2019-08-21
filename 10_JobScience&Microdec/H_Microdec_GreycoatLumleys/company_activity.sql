WITH event_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '123'
),
company_owner AS (
	SELECT
		person_ref,
		type,
		s.email_address
	FROM person_type pt
	LEFT JOIN staff s ON pt.person_type_ref = s.person_type_ref
	WHERE pt.type LIKE '%Z%'
	AND s.email_address LIKE '%@%'
)
SELECT
e.organisation_ref company_id,
CONCAT_WS(
	E'\n',
	CASE WHEN NULLIF(et.description, '') IS NOT NULL THEN CONCAT('Event type: ', et.description) END,
	CONCAT('Author: ', p.last_name, ' ', p.first_name),
	CASE WHEN NULLIF(co.email_address, '') IS NOT NULL THEN CONCAT('Email: ', co.email_address) END,
	CASE WHEN NULLIF(e.subject, '') IS NOT NULL THEN CONCAT('Subject: ', e.subject) END,
	CASE WHEN NULLIF(e.notes, '') IS NOT NULL THEN CONCAT('Activity notes: ', E'\n', e.notes) END
) AS content,
CONCAT(e.event_date, ' ', e.event_time)::TIMESTAMP insert_timestamp,
'comment' AS category,
'company' AS type,
-10 AS user_account_id

FROM event e
LEFT JOIN person p ON e.create_user = p.person_ref
LEFT JOIN company_owner co ON e.create_user = co.person_ref
LEFT JOIN event_type et ON e.type = et.code
WHERE NULLIF(e.organisation_ref, '') IS NOT NULL
AND NULLIF(e.event_date, '') IS NOT NULL