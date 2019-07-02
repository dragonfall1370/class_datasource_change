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
et.description AS activity_type,
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
AND et.description IN ('Drop to Client', 'Sick', 'Sales Lead', 'Sales lead - hot', 'Sales lead - normal', 'Client shutdown', 'Maternity/paternity leave', 'Working via another agency', 'Did not attend', 
'Day 1 info request to contact', 'Work history request to candidate', 'Day 1 rights sent to candidate', 'Basic work conditions request to contact', 'Basic work conditions sent to candidate',
'Day 1 info request to contact', 'Work history request to candidate', 'Day 1 rights sent to candidate', 'Basic work conditions request to contact', 'Basic work conditions sent to candidate',
'Activity', 'Referral - good', 'Referral - average', 'Referral - negative', 'Permanent offer', 'Position move to temporary job', 'Position move to contract', 'Position move to permanent position',
'Position move', 'Referral', 'Telephone call', 'Letter,Fax,Email', 'Mailshot', 'CV received', 'CV update', 'CV sent', 'Registration Interview', 'Interview 1 with client', 'Interview 2 with client',
'Interview 3 with client', 'Interview other', 'Canvass call', 'Meeting with client', 'Proposal submitted', 'Reference request', 'Invoice', 'Invoice for retainer', 'Invoice for shortlist',
'Invoice for completion', 'Temporary booking', 'Follow-up', 'Source', 'Debrief', 'Registration Interview', 'Candidate Call', 'Service Meeting', 'Staff Assessment', 'Sales Call', 'Marketing Call',
'Service Contact Call', 'Email Received', 'Text Message Received', 'Quick text', 'Email sent', 'Spec Cv Sent', 'Phone call made', 'Registration Interview', 'Contact call', 'Candidate call',
'Contact sales call', 'Candidate sales call', 'Target', 'Shortlist', 'Placement', 'Hitlist', 'Client Sales Call', 'Telephone Dial', 'Voicemail Call', 'Service Contact Call', 'Sales Contact Call', 
'Timesheet', 'PSL', 'Invite for Interview', 'Reject Candidate', 'Pay Advance Request', 'Profile requested P45', 'P45 auto-raised in back-office', 'P45 requested via website', 'Holiday',
'Shift booking', 'Process CV from email', 'Process vacancy from email', 'Process import from email')