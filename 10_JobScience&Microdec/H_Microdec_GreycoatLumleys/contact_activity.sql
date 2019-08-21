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
	SELECT *
	FROM contact_record_status
	WHERE rn = 1
),
event_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '123'
),
contact_owner AS (
	SELECT
		person_ref,
		type,
		s.email_address
	FROM person_type pt
	LEFT JOIN staff s ON pt.person_type_ref = s.person_type_ref
	WHERE pt.type LIKE '%Z%'
	AND s.email_address LIKE '%@%'
),
contact_activity AS (
	SELECT
	c.person_ref contact_id,
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
	'contact' AS type,
	-10 AS user_account_id,
	ROW_NUMBER() OVER(PARTITION BY c.person_ref, CONCAT_WS(
																												E'\n',
																												CASE WHEN NULLIF(et.description, '') IS NOT NULL THEN CONCAT('Event type: ', et.description) END,
																												CONCAT('Author: ', p.last_name, ' ', p.first_name),
																												CASE WHEN NULLIF(co.email_address, '') IS NOT NULL THEN CONCAT('Email: ', co.email_address) END,
																												CASE WHEN NULLIF(e.subject, '') IS NOT NULL THEN CONCAT('Subject: ', e.subject) END,
																												CASE WHEN NULLIF(e.notes, '') IS NOT NULL THEN CONCAT('Activity notes: ', E'\n', e.notes) END
																											) ORDER BY c.person_ref) AS rn

	FROM event e
	JOIN event_role er ON e.event_ref = er.event_ref
	JOIN current_contact c ON er.person_ref = c.person_ref
	LEFT JOIN person p ON e.create_user = p.person_ref
	LEFT JOIN contact_owner co ON e.create_user = co.person_ref
	LEFT JOIN event_type et ON e.type = et.code
	WHERE NULLIF(c.person_ref, '') IS NOT NULL
	AND NULLIF(e.event_date, '') IS NOT NULL
)

SELECT *
FROM contact_activity
WHERE rn = 1