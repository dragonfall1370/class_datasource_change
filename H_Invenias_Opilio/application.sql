WITH default_contact_company AS (
	SELECT 
		person_id contact_id,
		company_id,
		ROW_NUMBER() OVER(PARTITION BY company_id ORDER BY created ASC) rn
	FROM alpha_position
),
cte_job AS (
	SELECT
		jo.id job_id,
		COALESCE(p.person_id, cc.contact_id) contact_id,
		jo.company_id,
		jo.roleTitle job_title,
		CASE
			WHEN partTime = 1 THEN 'PART_TIME'
			ELSE 'FULL_TIME'
		END employment_type,
		CASE
			WHEN permanent = 1 THEN 'PERMANENT'
			WHEN temporary = 1 THEN 'TEMPORARY'
			ELSE 'CONTRACT'
		END job_type

	FROM alpha_job_opening jo
	JOIN alpha_company ac ON jo.company_id = ac.id
	LEFT JOIN alpha_position p ON jo.position_id = p.id
	LEFT JOIN default_contact_company cc ON jo.company_id = cc.company_id AND cc.rn = 1
),
map_stage AS (
	SELECT
		candidate_id,
		jobopening_id job_id,
		CASE
			WHEN cc.candControl = 'Associated' THEN 'SHORTLISTED'
			WHEN cc.candControl = 'Applied' THEN 'SHORTLISTED'
			WHEN cc.candControl = 'Shortlist' THEN 'SHORTLISTED'
			WHEN cc.candControl = 'Interested' THEN 'SHORTLISTED'
			WHEN cc.candControl = 'Def Send' THEN 'SENT'
			WHEN cc.candControl = 'CV Sent' THEN 'SENT'
			WHEN cc.candControl = '1st IV' THEN 'FIRST_INTERVIEW'
			WHEN cc.candControl = '2nd IV' THEN 'SECOND_INTERVIEW'
			WHEN cc.candControl = 'Under Offer' THEN 'OFFERED'
			WHEN cc.candControl = 'ANS' THEN 'OFFERED'
			WHEN cc.candControl = 'Hired' THEN 'OFFERED'
-- 			WHEN cc.candControl = 'Rejected' THEN 
-- 			WHEN cc.candControl = 'Rejected by Client' THEN 
-- 			WHEN cc.candControl = 'Self-Withdrawn' THEN 
		END application_stage,
		cc.candControl real_stage,
		a.created created_date,
		COALESCE(dateRejected, dateRejectedByClient) rejected_date,
		date1stInt first_int_date,
		date2ndInt second_int_date,
		dateOffered offered_date,
		dateAccepted accepted_date,
		dateHired placement_date
	FROM alpha_association a
	JOIN alpha_sel_cand_control cc ON a.candControl_id = cc.id
),
cte_application AS (
	SELECT s.*,
	j.job_title,
	ROW_NUMBER() OVER(PARTITION BY s.candidate_id, s.job_id ORDER BY CASE
																																	WHEN application_stage = 'OFFERED' THEN 1
																																	WHEN application_stage = 'SECOND_INTERVIEW' THEN 2
																																	WHEN application_stage = 'FIRST_INTERVIEW	' THEN 	3																															
																																	WHEN application_stage = 'SENT' THEN 4
																																	WHEN application_stage = 'SHORTLISTED' THEN 5
																																END) rn
	FROM map_stage s
	JOIN cte_job j ON s.job_id = j.job_id
)
SELECT
	candidate_id,
	job_id,
	rn,
	real_stage,
-- 	jo.position_id,
-- 	jo.company_id,
	job_title,
	application_stage
FROM cte_application
WHERE application_stage IS NOT NULL
AND rn = 1
