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
	ROW_NUMBER() OVER(PARTITION BY a.id_assignment ORDER BY ac.contacted_on ASC) rn
	FROM assignment_contact ac
	JOIN "assignment" a ON ac.id_assignment = a.id_assignment AND a.is_deleted = 0
	WHERE a.is_deleted = 0
),
map_stage AS (
	SELECT
	ac.id_assignment job_id,
	ac.id_person candidate_id,
	to_char(ac.created_on::DATE, 'YYYY-MM-DD') actioned_date,
	CASE
		WHEN cp.value = 'Longlist' THEN 'SHORTLISTED'
		WHEN cp.value = 'Initial Contact' THEN 'SHORTLISTED'
		WHEN cp.value = 'Qualifying' THEN 'SHORTLISTED'
		WHEN cp.value = 'Longlist Withdrew' THEN 'SHORTLISTED'
		WHEN cp.value = 'Longlist Reject' THEN 'SHORTLISTED'
		WHEN cp.value = 'Internal Interview' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'Internal Interview Withdrew' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'Internal Interview Reject' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'Shortlist' THEN 'SHORTLISTED'
		WHEN cp.value = 'Shortlist Withdrew' THEN 'SHORTLISTED'
		WHEN cp.value = 'Shortlist Reject' THEN 'SHORTLISTED'
		WHEN cp.value = 'Client Interview' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'Second Client Interview' THEN 'SECOND_INTERVIEW'
		WHEN cp.value = 'Client Interview Withdrew' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'Client Interview Reject' THEN 'FIRST_INTERVIEW'
		WHEN cp.value = 'Offer' THEN 'OFFERED'
		WHEN cp.value = 'Offer Accept' THEN 'OFFERED'
		WHEN cp.value = 'Offer Withdrew' THEN 'OFFERED'
		WHEN cp.value = 'Offer Reject' THEN 'OFFERED'
		WHEN cp.value = 'Placed' THEN 'OFFERED'
	END application_stage,
	CASE
		WHEN cp.value = 'Longlist' THEN 'Long List'
		WHEN cp.value = 'Initial Contact' THEN 'Initial Contact'
		WHEN cp.value = 'Qualifying' THEN 'Qualifying'
		WHEN cp.value = 'Longlist Withdrew' THEN 'Withdrew'
		WHEN cp.value = 'Internal Interview' THEN 'Internal Interview'
		WHEN cp.value = 'Internal Interview Withdrew' THEN 'Withdrew'
		WHEN cp.value = 'Internal Interview Reject' THEN 'Internal Interview'
		WHEN cp.value = 'Shortlist Withdrew' THEN 'Withdrew'
		WHEN cp.value = 'Client Interview' THEN 'Client Interview'
		WHEN cp.value = 'Client Interview Withdrew' THEN 'Client Interview Withdrew'
		WHEN cp.value = 'Client Interview Reject' THEN 'Client Interview'
		WHEN cp.value = 'Offer Accept' THEN 'Accepted'
		WHEN cp.value = 'Offer Withdrew' THEN 'Withdrew'
	END sub_stage,
	cp.value real_stage
	FROM assignment_candidate ac
	JOIN cte_candidate cc ON ac.id_person = cc.candidate_id AND cc.rn = 1
	JOIN cte_job cj ON ac.id_assignment = cj.job_id AND cj.rn = 1
	JOIN candidate_progress cp ON ac.id_candidate_progress = cp.id_candidate_progress AND cp.is_active = 1
	WHERE is_excluded = 0
),
cte_application AS (
	SELECT
	job_id,
	candidate_id,
	actioned_date,
	application_stage,
	sub_stage,
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
job_id "application-positionExternalId",
candidate_id "application-candidateExternalId",
application_stage "application-stage",
sub_stage
real_stage,
actioned_date "application-actionedDate"
FROM cte_application
WHERE rn = 1
AND application_stage = 'OFFERED'
