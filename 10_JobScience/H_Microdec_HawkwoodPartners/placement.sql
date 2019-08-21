WITH cte_candidate AS (
	SELECT
	ROW_NUMBER() OVER(PARTITION BY pt.person_ref ORDER BY CASE WHEN l.code = 'C' THEN 1 ELSE 2 END) rn,
	p.*,
	c.*,
	pt.status,
	pt.availability_confirmed,
	pt.status_reason
	FROM candidate c
	LEFT JOIN person_type pt ON c.person_type_ref = pt.person_type_ref
	LEFT JOIN person p ON pt.person_ref = p.person_ref
	LEFT JOIN lookup l ON pt.type = l.code
	WHERE l.code_type = '104'
),
stage AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '607'
),
job_status AS (
	SELECT
		code,
		description
	FROM lookup
	WHERE code_type = '119'
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
placement_details AS (
	SELECT 
		e.event_ref,
		e.opportunity_ref job_id,
		CASE
			WHEN ot.description = 'Permanent vacancy' THEN 'PERMANENT'
			WHEN ot.description = 'Temporary vacancy' THEN 'TEMPORARY'
		END job_type,
		e.organisation_ref company_id,
		er.person_ref candidate_id,
		e.event_date::TIMESTAMP actioned_date,
		pl.start_date AS perm_start_date,
		NULL AS temp_start_date,
		pl.income,
		pl.position_type,
		pl.fee_percentage,
		pl.fee_amount,
		e.notes,
		ROW_NUMBER() OVER(PARTITION BY e.opportunity_ref, er.person_ref ORDER BY e.event_date::TIMESTAMP DESC) rn
	FROM event e
	JOIN event_role er ON e.event_ref = er.event_ref
	JOIN cte_candidate c ON er.person_ref = c.person_ref AND c.rn = 1
	JOIN opportunity o ON e.opportunity_ref = o.opportunity_ref
	JOIN "placing" pl ON e.event_ref = pl.event_ref
	LEFT JOIN opportunity_type ot ON o.type = ot.code
	LEFT JOIN event_type et ON e.z_last_type = et.code
	WHERE NULLIF(e.opportunity_ref, '') IS NOT NULL
	AND NULLIF(er.person_ref, '') IS NOT NULL
	AND et.description IS NOT NULL
	
	UNION ALL
	
	SELECT
		e.event_ref,
		e.opportunity_ref job_id,
		CASE
			WHEN ot.description = 'Permanent vacancy' THEN 'PERMANENT'
			WHEN ot.description = 'Temporary vacancy' THEN 'TEMPORARY'
		END job_type,
		e.organisation_ref company_id,
		er.person_ref candidate_id,
		e.event_date::TIMESTAMP actioned_date,
		NULL AS perm_start_date,
		tb.start_date AS temp_start_date,
		NULL income,
		NULL position_type,
		NULL fee_percentage,
		NULL fee_amount,
		e.notes,
		ROW_NUMBER() OVER(PARTITION BY e.opportunity_ref, er.person_ref ORDER BY e.event_date::TIMESTAMP DESC) rn
	FROM event e
	JOIN event_role er ON e.event_ref = er.event_ref
	JOIN cte_candidate c ON er.person_ref = c.person_ref AND c.rn = 1
	JOIN opportunity o ON e.opportunity_ref = o.opportunity_ref
	JOIN temporary_booking tb ON e.event_ref = tb.event_ref
	LEFT JOIN opportunity_type ot ON o.type = ot.code
	LEFT JOIN event_type et ON e.z_last_type = et.code
	WHERE NULLIF(e.opportunity_ref, '') IS NOT NULL
	AND NULLIF(er.person_ref, '') IS NOT NULL
	AND et.description IS NOT NULL
),
map_application AS (
	SELECT
		c.person_ref candidate_id,
		o.opportunity_ref job_id,
		CASE
			WHEN ot.description = 'Permanent vacancy' THEN 'PERMANENT'
			WHEN ot.description = 'Temporary vacancy' THEN 'TEMPORARY'
		END job_type,
		CASE
			WHEN s.description = 'New Vacancy' THEN 'SHORTLISTED'
			WHEN s.description = 'CV' THEN 'SENT'
			WHEN s.description = 'Interview' THEN CASE WHEN js.description = 'Closed successfully' THEN 'OFFERED' ELSE 'FIRST_INTERVIEW'END
			WHEN s.description = 'Additional' THEN 'SECOND_INTERVIEW'
			WHEN s.description = 'Offer' THEN 'OFFERED'
		END AS application_stage,
		js.description AS job_status,
		actioned_date AS offer_date,
		CASE
			WHEN ot.description = 'Permanent vacancy' THEN perm_start_date
			WHEN ot.description = 'Temporary vacancy' THEN temp_start_date
		END AS start_date,
		income,
		fee_percentage,
		fee_amount,
		pd.notes
	FROM pipeline_candidate_stage cs
	LEFT JOIN stage s ON cs.stage = s.code
	JOIN opportunity o ON cs.opportunity_ref = o.opportunity_ref
	LEFT JOIN opportunity_type ot ON o.type = ot.code
	JOIN job_status js ON o.record_status = js.code
	JOIN cte_candidate c ON cs.person_ref = c.person_ref AND c.rn = 1
	LEFT JOIN placement_details pd ON c.person_ref = candidate_id AND o.opportunity_ref = job_id AND pd.rn = 1
),
application AS (
	SELECT *,
		ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY CASE application_stage
																																WHEN 'PLACEMENT_PERMANENT' THEN 1
																																WHEN 'OFFERED' THEN 2
																																WHEN 'SECOND_INTERVIEW' THEN 3
																																WHEN 'FIRST_INTERVIEW' THEN 4
																																WHEN 'SENT' THEN 5
																																WHEN 'SHORTLISTED' THEN 6 
																															 END ASC) AS rn
	FROM map_application
)

SELECT
job_id,
candidate_id,
application_stage,
job_status,
offer_date,
job_type,
start_date::TIMESTAMP,
income::FLOAT,
fee_percentage::FLOAT,
fee_amount::FLOAT,
notes,
CASE
		WHEN job_type = 'PERMANENT' THEN 1
		WHEN job_type = 'TEMPORARY' THEN 4
END position_type,
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
WHERE application_stage = 'OFFERED'
AND job_status = 'Closed successfully'