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
application_mapping AS (
	SELECT 
		e.event_ref,
		e.opportunity_ref job_id,
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
		e.event_date actioned_date,
		e.notes
	FROM event e
	JOIN event_role er ON e.event_ref = er.event_ref
	JOIN candidate c ON er.person_ref = c.person_ref AND c.rn = 1
	JOIN candidate_status cs ON c.status = cs.code AND cs.code IN ('A1', 'I1')
	JOIN opportunity o ON e.opportunity_ref = o.opportunity_ref
	LEFT JOIN event_type et ON e.z_last_type = et.code
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
																																	WHEN application_stage = 'PLACEMENT_PERMANENT' OR application_stage = 'PLACEMENT_CONTRACT' THEN 1
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
	job_id "application-positionExternalId",
	candidate_id "application-candidateExternalId",
	import_application_stage "application-stage",
	actioned_date
FROM application
WHERE rn = 1