WITH cte_candidate AS (
	SELECT
	c.id_person candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.id_person ORDER BY px.created_on DESC) rn
	FROM candidate c
	JOIN person_x px ON c.id_person = px.id_person AND px.is_deleted = 0
	JOIN person p ON c.id_person = p.id_person AND p.is_deleted = 0
),
cte_job AS (
	SELECT
	a.id_assignment job_id,
	CASE
		WHEN f.id_assignment IS NULL THEN 'PERMANENT'
		ELSE 'CONTRACT'
	END job_type,
	ROW_NUMBER() OVER(PARTITION BY a.id_assignment ORDER BY ac.contacted_on ASC) rn
	FROM assignment_contact ac
	JOIN "assignment" a ON ac.id_assignment = a.id_assignment AND a.is_deleted = 0
	LEFT JOIN flex f ON a.id_assignment = f.id_assignment
	WHERE a.is_deleted = 0
),
map_stage AS (
	SELECT
	job_type,
	ac.id_assignment job_id,
	ac.id_person candidate_id,
	to_char(ac.created_on::DATE, 'YYYY-MM-DD') actioned_date,
	CASE
		WHEN cp.value = 'Shortlist' THEN 'SHORTLISTED'
		WHEN cp.value = 'EXTENDED' THEN 'PLACEMENT_CONTRACT'
		WHEN cp.value = 'Placed' THEN 'PLACEMENT_PERMANENT'
		WHEN cp.value = 'Offer' THEN 'OFFERED'
		WHEN cp.value = 'Client Interview Reject' THEN 'SENT'
		WHEN cp.value = 'CONV TO PERM' THEN 'PLACEMENT_CONTRACT'
		WHEN cp.value = 'Offer Reject' THEN 'OFFERED'
		WHEN cp.value = 'COUNTER OFFER' THEN 'OFFERED'
		WHEN cp.value = 'Longlist' THEN 'SHORTLISTED'
		WHEN cp.value = 'NO SHOW' THEN 'SHORTLISTED'
		WHEN cp.value = 'APPLICANT' THEN 'SHORTLISTED'
		WHEN cp.value = 'INT CONTRACT POSTED' THEN 'PLACEMENT_CONTRACT'
		WHEN cp.value = 'OFFER ACCEPTED' THEN 'OFFERED'
		WHEN cp.value = 'Client Interview' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'OFFER WITHDRAWN' THEN 'OFFERED'
		WHEN cp.value = 'NOTICE' THEN 'PLACEMENT_CONTRACT'
		WHEN cp.value = 'CONTRACT' THEN 'PLACEMENT_CONTRACT'
		WHEN cp.value = 'Offer Accept' THEN 'OFFERED'
		WHEN cp.value = 'ASSESSMENT CENTRE' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'CLIENT INT 2' THEN 'SECOND_INTERVIEW'
		WHEN cp.value = 'CV SENT TO CLIENT' THEN 'SHORTLISTED'
		WHEN cp.value = 'Offer Withdrew' THEN 'OFFERED'
		WHEN cp.value = 'Shortlist Reject' THEN 'SHORTLISTED'
		WHEN cp.value = 'SHL TEST' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'CLIENT INT 3' THEN 'SECOND_INTERVIEW'
		WHEN cp.value = 'OFFER REJECTED' THEN 'OFFERED'
	END application_stage,
	cp.value real_stage
	FROM assignment_candidate ac
	JOIN cte_candidate cc ON ac.id_person = cc.candidate_id AND cc.rn = 1
	JOIN cte_job cj ON ac.id_assignment = cj.job_id AND cj.rn = 1
	JOIN candidate_progress cp ON ac.id_candidate_progress = cp.id_candidate_progress AND cp.is_active = 1
	WHERE is_excluded = 0
),
cte_application AS (
	SELECT
	job_type,
	job_id,
	candidate_id,
	actioned_date,
	CASE
		WHEN application_stage IN ('PLACEMENT_PERMANENT', 'PLACEMENT_CONTRACT') THEN 'OFFERED'
		ELSE application_stage
	END application_stage_import,
	application_stage,
	real_stage,
	ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY CASE application_stage
																																WHEN 'PLACEMENT_PERMANENT' THEN 1
																																WHEN 'OFFERED' THEN 2
																																WHEN 'SECOND_INTERVIEW' THEN 3
																																WHEN 'FIRST_INTERVIEW' THEN 4
																																WHEN 'SENT' THEN 5
																																WHEN 'SHORTLISTED' THEN 6
																															 END ASC) AS rn
	FROM map_stage
	WHERE application_stage IS NOT NULL
)

SELECT
job_type,
job_id "application-positionExternalId",
candidate_id "application-candidateExternalId",
application_stage_import "application-stage",
real_stage,
actioned_date "application-actionedDate"
FROM cte_application
WHERE rn = 1
-- AND real_stage = 'Placed'
