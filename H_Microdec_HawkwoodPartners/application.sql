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
map_application AS (
	SELECT
		c.person_ref candidate_id,
		o.opportunity_ref job_id,
		CASE
			WHEN s.description = 'New Vacancy' THEN 'SHORTLISTED'
			WHEN s.description = 'CV' THEN 'SENT'
			WHEN s.description = 'Interview' THEN CASE WHEN js.description = 'Closed successfully' THEN 'OFFERED' ELSE 'FIRST_INTERVIEW'END
			WHEN s.description = 'Additional' THEN 'SECOND_INTERVIEW'
			WHEN s.description = 'Offer' THEN 'OFFERED'
		END AS application_stage,
		js.description job_status

	FROM pipeline_candidate_stage cs
	LEFT JOIN stage s ON cs.stage = s.code
	JOIN opportunity o ON cs.opportunity_ref = o.opportunity_ref
	JOIN job_status js ON o.record_status = js.code
	JOIN cte_candidate c ON cs.person_ref = c.person_ref AND c.rn = 1
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
job_id "application-positionExternalId",
candidate_id "application-candidateExternalId",
application_stage "application-stage",
job_status
FROM application
-- WHERE application_stage IN ('OFFERED', 'FIRST_INTERVIEW')
-- AND job_status = 'Closed successfully'