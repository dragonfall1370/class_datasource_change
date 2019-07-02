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
opportunity_type AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '117'
),
application_mapping AS (
	SELECT * 
	FROM (
		SELECT 
			e.event_ref,
			e.opportunity_ref AS job_id,
			e.organisation_ref AS company_id,
			er.person_ref AS candidate_id,
			ot.description AS job_type,
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
		LEFT JOIN opportunity_type ot ON o.type = ot.code
		LEFT JOIN stages s ON e.z_last_outcome = s.code
		WHERE NULLIF(e.opportunity_ref, '') IS NOT NULL
		AND er.type IN ('A1', 'C5', 'CA2', 'CB2', 'CC2', 'F', 'H', 'K', 'D')
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
			ot.description AS job_type,
			et.description AS real_stage,
			CASE
				WHEN et.description = 'Permanent offer' THEN 'OFFERED'
				WHEN et.description = 'Contract offer' THEN 'OFFERED'
				WHEN et.description = 'CV sent' THEN 'SENT'
				WHEN et.description = 'Int 1 with client' THEN 'FIRST_INTERVIEW'
				WHEN et.description = 'Int 2 with client' THEN 'SECOND_INTERVIEW'
				WHEN et.description = 'Int 3 with client' THEN 'SECOND_INTERVIEW'
				WHEN et.description = 'Email CV sent' THEN 'SENT'
				WHEN et.description = 'Invoice' THEN 'OFFERED'
				WHEN et.description = 'Int other' THEN 'FIRST_INTERVIEW'
				WHEN et.description = 'Meeting with candidate' THEN 'SHORTLISTED'
				WHEN et.description = 'Letter sent' THEN 'SHORTLISTED'
				ELSE 'SHORTLISTED'
			END application_stage,
			e.event_date AS actioned_date,
			e.notes,
			ROW_NUMBER() OVER(PARTITION BY e.opportunity_ref, er.person_ref ORDER BY CASE WHEN er.type IN ('A1', 'C5', 'CA2', 'CB2', 'CC2', 'F', 'H', 'K', 'D') THEN 1 ELSE 2 END) AS rn_type
		FROM event e
		JOIN event_role er ON e.event_ref = er.event_ref
		JOIN candidate c ON er.person_ref = c.person_ref AND c.rn = 1
		JOIN opportunity o ON e.opportunity_ref = o.opportunity_ref
		LEFT JOIN opportunity_type ot ON o.type = ot.code
		LEFT JOIN event_type et ON e.z_last_type = et.code
		WHERE NULLIF(e.opportunity_ref, '') IS NOT NULL
		AND er.type IN ('A1', 'C5', 'CA2', 'CB2', 'CC2', 'F', 'H', 'K', 'D')
	) tm2
	WHERE rn_type = 1
),
application AS (
	SELECT *,
	ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY job_id) AS rn
	FROM application_mapping
	WHERE real_stage IN ('Client Invoiced', 'Invoice paid', 'Started new position', 'Invoice')
)
SELECT
	job_id,
	candidate_id,
	job_type,
	actioned_date::TIMESTAMP AS offer_date,
	COALESCE(p.start_date::TIMESTAMP,	tb.start_date::TIMESTAMP) start_date,
	COALESCE(p.end_date::TIMESTAMP, tb.end_date::TIMESTAMP) end_date,
	tb.hours_per_day::FLOAT,
	tb.days_per_week::FLOAT,
	notes,
	CASE
		WHEN job_type = 'Permanent vacancy' THEN 1
		WHEN job_type = 'Contract vacancy' THEN 2
	END position_type,
	CASE
		WHEN job_type = 'Permanent vacancy' THEN 301
		WHEN job_type = 'Contract vacancy' THEN 303
	END application_status,
	3 AS draft_offer, --used to move OFFERED to PLACED in VC [offer]
	2 AS invoice_status, --used to update invoice status in VC [invoice] as 'active'
	1 AS invoice_renewal_index, --default value in VC [invoice]
	1 AS invoice_renewal_flow_status, --used to update flow status in VC [invoice] as 'placement_active'
	1 AS invoice_valid
FROM application a
LEFT JOIN temporary_booking tb ON a.event_ref = tb.event_ref
LEFT JOIN "placing" p ON a.event_ref = p.event_ref
WHERE rn = 1