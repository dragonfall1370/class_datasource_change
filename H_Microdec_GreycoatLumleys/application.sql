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
stages AS (
	SELECT 
		code,
		description
	FROM lookup
	WHERE code_type = '157'
),
event_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '123'
),
application_mapping AS (
	SELECT * 
	FROM (
		SELECT 
			e.event_ref,
			e.opportunity_ref AS job_id,
			e.organisation_ref AS company_id,
			er.person_ref AS candidate_id,
			s.description AS real_stage,
			CASE
				WHEN s.description = 'Client Invoiced' THEN 'OFFERED'
				WHEN s.description = 'Invoice paid' THEN 'OFFERED'
				WHEN s.description = 'Offer accepted' THEN 'OFFERED'
				WHEN s.description = 'Offer rejected' THEN 'OFFERED'
				WHEN s.description = 'Progress to next stage' THEN 'SENT'
				WHEN s.description = 'Shortlist' THEN 'SHORTLISTED'
				WHEN s.description = 'Started new position' THEN 'OFFERED'
				ELSE 'SHORTLISTED'
			END AS application_stage,
			e.event_date AS actioned_date,
			e.notes,
			ROW_NUMBER() OVER(PARTITION BY e.opportunity_ref, er.person_ref ORDER BY CASE WHEN er.type IN ('A1', 'C5', 'CA2', 'CB2', 'CC2', 'F', 'H', 'K', 'D') THEN 1 ELSE 2 END) AS rn_type
		FROM event e
		JOIN event_role er ON e.event_ref = er.event_ref
		JOIN candidate c ON er.person_ref = c.person_ref AND c.rn = 1
		JOIN opportunity o ON e.opportunity_ref = o.opportunity_ref
		LEFT JOIN stages s ON e.z_last_outcome = s.code
		WHERE NULLIF(e.opportunity_ref, '') IS NOT NULL
		AND er.type IN ('A1', 'C5', 'CA2', 'CB2', 'CC2', 'F', 'H', 'K', 'D')
-- 		AND er.type = 'D'
	) tm1
	WHERE rn_type = 1
	
	UNION ALL
	
	SELECT * 
	FROM (
		SELECT 
			e.event_ref,
			e.opportunity_ref AS job_id,
			e.organisation_ref AS company_id,
			er.person_ref AS candidate_id,
			et.description AS real_stage,
			CASE
				WHEN et.description = 'Permanent offer' THEN 'OFFERED'
				WHEN et.description = 'Contract offer' THEN 'OFFERED'
				WHEN et.description = 'CV sent' THEN 'SENT'
				WHEN et.description = 'Int 1 with client' THEN 'FIRST_INTERVIEW'
				WHEN et.description = 'Int 2 with client' THEN 'SECOND_INTERVIEW'
				WHEN et.description = 'Int 3 with client' THEN 'SECOND_INTERVIEW'
				WHEN et.description = 'Telephone call' THEN	'SHORTLISTED'
				WHEN et.description = 'Email CV sent' THEN 'SENT'
				WHEN et.description = 'Invoice' THEN 'OFFERED'
				WHEN et.description = 'Int other' THEN 'FIRST_INTERVIEW'
				WHEN et.description = 'Meeting with candidate' THEN 'SHORTLISTED'
				WHEN et.description = 'Letter sent' THEN 'SHORTLISTED'
				WHEN et.description = 'Shift booking' THEN 'OFFERED'
				ELSE 'SHORTLISTED'
			END application_stage,
			e.event_date AS actioned_date,
			e.notes,
			ROW_NUMBER() OVER(PARTITION BY e.opportunity_ref, er.person_ref ORDER BY CASE WHEN er.type IN ('A1', 'C5', 'CA2', 'CB2', 'CC2', 'F', 'H', 'K', 'D') THEN 1 ELSE 2 END) AS rn_type
		FROM event e
		JOIN event_role er ON e.event_ref = er.event_ref
		JOIN candidate c ON er.person_ref = c.person_ref AND c.rn = 1
		JOIN opportunity o ON e.opportunity_ref = o.opportunity_ref
		LEFT JOIN event_type et ON e.z_last_type = et.code
		WHERE NULLIF(e.opportunity_ref, '') IS NOT NULL
		AND er.type IN ('A1', 'C5', 'CA2', 'CB2', 'CC2', 'F', 'H', 'K', 'D')
-- 	AND er.type = 'D'
	) tm2
	WHERE rn_type = 1
),
application AS (
	SELECT *,
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
	job_id AS "application-positionExternalId",
	real_stage,
	candidate_id AS "application-candidateExternalId",
	application_stage AS "application-stage",
	actioned_date
FROM application
WHERE rn = 1
-- AND real_stage = 'Shift booking'