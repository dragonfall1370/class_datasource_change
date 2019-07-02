WITH map_stage AS (
	SELECT
	job job_id,
	candidate_contact candidate_id,
	CASE
		WHEN a.stage = 'Interview' THEN 'FIRST_INTERVIEW'
		WHEN a.stage = 'Submittal' THEN 'SENT'
		WHEN a.stage = 'Placement' THEN 'OFFERED'
		WHEN a.stage = 'Matching' THEN 'SHORTLISTED'
		WHEN a.stage = 'Offer' THEN 'OFFERED'
		WHEN a.stage = 'Application' THEN 'SHORTLISTED'
	END application_stage
	FROM application a	
),
cte_application AS (
	SELECT
	j.job_id,
	c.contact_id candidate_id,
	to_char(a.created_date::DATE, 'YYYY-MM-DD' ) actioned_date,
	application_stage,
	ROW_NUMBER() OVER(PARTITION BY j.job_id, c.contact_id ORDER BY CASE application_stage
																																WHEN 'PLACEMENT_PERMANENT' THEN 1
																																WHEN 'OFFERED' THEN 2
																																WHEN 'SECOND_INTERVIEW' THEN 3
																																WHEN 'FIRST_INTERVIEW' THEN 4
																																WHEN 'SENT' THEN 5
																																WHEN 'SHORTLISTED' THEN 6 
																															 END ASC) AS rn,
	a.stage real_stage,
	a.application_status status,
	rt.name

	FROM application a
	LEFT JOIN job j ON a.job = j.job_id
	LEFT JOIN contact c ON a.candidate_contact = c.contact_id AND c.record_type_id = '01261000000gXaw'
	LEFT JOIN record_type rt ON concat(a.record_type_id, 'AAU') = rt.record_type_id
	LEFT JOIN map_stage ms ON j.job_id = ms.job_id AND c.contact_id = ms.candidate_id
	WHERE c.contact_id is not null
	AND a.stage <> 'Candidate Search'
	AND a.is_deleted = 0
)

SELECT
job_id "application-positionExternalId",
candidate_id "application-candidateExternalId",
application_stage "application-stage",
actioned_date "application-actionedDate",
status
FROM cte_application