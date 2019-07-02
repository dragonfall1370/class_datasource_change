WITH candidate AS (
	SELECT
	ROW_NUMBER() OVER(PARTITION BY pt.person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
	p.*,
	c.*,
	pt.status,
	CASE
		WHEN pt.type = 'C' THEN 'PERMANENT'
		WHEN pt.type = 'A' THEN 'TEMPORARY'
	END AS candidate_type,
	pt.availability_confirmed,
	pt.status_reason
	FROM candidate c
	LEFT JOIN person_type pt ON c.person_type_ref = pt.person_type_ref
	JOIN person p ON pt.person_ref = p.person_ref
	LEFT JOIN lookup l ON pt.type = l.code
	WHERE l.code_type = '104'
	AND l.code IN ('A', 'C')
),
candidate_status AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '105'
),
event_type AS (
	SELECT 
		code,
		description
	FROM lookup
	WHERE code_type = '123'
),
opportunity_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '117'
),
application_mapping AS (
	SELECT 
		e.event_ref,
		e.opportunity_ref job_id,
		CASE
			WHEN ot.description = 'Permanent vacancy' THEN 'PERMANENT'
			WHEN ot.description = 'Temporary vacancy' THEN 'TEMPORARY'
		END job_type,
		e.organisation_ref company_id,
		er.person_ref candidate_id,
		CASE
			WHEN et.description = 'CV update' THEN 'SENT'
			WHEN et.description = 'Interview other' THEN 'FIRST_INTERVIEW'
			WHEN et.description = 'Telephone call' THEN 'SHORTLISTED'
			WHEN et.description = 'Hitlist' THEN 'SHORTLISTED'
			WHEN et.description = 'Candidate Call' THEN 'SHORTLISTED'
			WHEN et.description = 'Interview 3 with client' THEN 'SECOND_INTERVIEW'
			WHEN et.description = 'Email sent' THEN 'SHORTLISTED'
			WHEN et.description = 'CV sent' THEN 'SENT'
			WHEN et.description = 'Contact call' THEN 'SHORTLISTED'
			WHEN et.description = 'Activity' THEN 'SHORTLISTED'
			WHEN et.description = 'Permanent offer' THEN 'OFFERED'
			WHEN et.description = 'Temporary booking' THEN 'PLACEMENT_CONTRACT'
			WHEN et.description = 'CV received' THEN 'SENT'
			WHEN et.description = 'Spec Cv Sent' THEN 'SENT'
			WHEN et.description = 'Interview 1 with client' THEN 'FIRST_INTERVIEW'
			WHEN et.description = 'Registration Interview' THEN 'SHORTLISTED'
			WHEN et.description = 'Email Received' THEN 'SHORTLISTED'
			WHEN et.description = 'Candidate call' THEN 'SHORTLISTED'
			WHEN et.description = 'Invite for Interview' THEN 'SHORTLISTED'
			WHEN et.description = 'Interview 2 with client' THEN 'SECOND_INTERVIEW'
		END application_stage,
		e.event_date::TIMESTAMP actioned_date,
		tb.start_date::TIMESTAMP,
		tb.end_date::TIMESTAMP,
		tb.hours_per_day::FLOAT,
		tb.days_per_week::FLOAT,
		e.notes
	FROM event e
	JOIN event_role er ON e.event_ref = er.event_ref
	JOIN candidate c ON er.person_ref = c.person_ref AND c.rn = 1
	JOIN candidate_status cs ON c.status = cs.code AND cs.code IN ('A1', 'I1')
	JOIN opportunity o ON e.opportunity_ref = o.opportunity_ref
	LEFT JOIN opportunity_type ot ON o.type = ot.code
	LEFT JOIN event_type et ON e.z_last_type = et.code
	LEFT JOIN temporary_booking tb ON e.event_ref = tb.event_ref
	WHERE NULLIF(e.opportunity_ref, '') IS NOT NULL
	AND NULLIF(er.person_ref, '') IS NOT NULL
	AND et.description IS NOT NULL
),
application AS (
	SELECT *,
	CASE
		WHEN application_stage = 'PLACEMENT_CONTRACT' THEN 'OFFERED'
		ELSE application_stage
	END import_application_stage,
	ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY CASE
																																	WHEN application_stage = 'PLACEMENT_PERMANENT' OR application_stage = 'PLACEMENT_' THEN 1
																																	WHEN application_stage = 'OFFERED' THEN 2
																																	WHEN application_stage = 'SECOND_INTERVIEW' THEN 3
																																	WHEN application_stage = 'FIRST_INTERVIEW' THEN 4
																																	WHEN application_stage = 'SENT' THEN 5
																																	WHEN application_stage = 'SHORTLISTED' THEN 6
																																 END ASC) AS rn
	FROM application_mapping
	WHERE application_stage IS NOT NULL
)
SELECT
	job_id,
	candidate_id,
	application_stage,
	actioned_date offer_date,
	start_date,
	end_date,
	hours_per_day,
	days_per_week,
	CASE
		WHEN job_type = 'PERMANENT' THEN 1
		WHEN job_type = 'TEMPORARY' THEN 4
	END position_type,
	notes,
	CASE
		WHEN job_type = 'PERMANENT' THEN 301
		WHEN job_type = 'TEMPORARY' THEN 303
	END application_status,
	3 AS draft_offer, --used to move OFFERED to PLACED in VC [offer]
	2 AS invoice_status, --used to update invoice status in VC [invoice] as 'active'
	1 AS invoice_renewal_index, --default value in VC [invoice]
	1 AS invoice_renewal_flow_status, --used to update flow status in VC [invoice] as 'placement_active'
	1 AS invoice_valid
FROM application
WHERE rn = 1
AND application_stage = 'PLACEMENT_CONTRACT'