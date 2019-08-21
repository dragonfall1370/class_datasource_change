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
	ac.contacted_on::timestamp offer_date,
	f.contract_start_date::timestamp,
	f.contract_end_date::timestamp,
	f.hours_per_day::int,
	f.days_per_week::int,
	ct.contract_extended_to_date::timestamp renewed_date,
	concat(
		CASE WHEN NULLIF(TRIM(REPLACE(ac.contacted_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contacted by: ', REPLACE(ac.contacted_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN f.base_client_rate IS NULL THEN '' ELSE concat('Base client rate: ', f.base_client_rate, E'\n') END,
		CASE WHEN f.client_rate_conversion_rate IS NULL THEN '' ELSE concat('Client rate conversion rate: ', f.client_rate_conversion_rate, E'\n') END,
		CASE WHEN f.client_rate IS NULL THEN '' ELSE concat('Client rate: ', f.client_rate, E'\n') END,
		CASE WHEN f.base_contractor_rate IS NULL THEN '' ELSE concat('Base contractor rate: ', f.base_contractor_rate, E'\n') END,
		CASE WHEN f.contractor_rate_conversion_rate IS NULL THEN '' ELSE concat('Contractor rate conversion rate: ', f.contractor_rate_conversion_rate, E'\n') END,
		CASE WHEN f.contractor_rate IS NULL THEN '' ELSE concat('Contractor rate: ', f.contractor_rate, E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.rate_comment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Rate comment: ', REPLACE(f.rate_comment, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ac.notes, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Note: ', REPLACE(ac.notes, '\x0d\x0a', ' ')) END
	) note,
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
	LEFT JOIN flex f ON ac.id_assignment = f.id_assignment
	LEFT JOIN contract ct ON ac.id_assignment = ct.id_flex AND ac.id_person = ct.id_person
	WHERE is_excluded = 0
),
cte_application AS (
	SELECT 
	*,
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
	AND application_stage IN ('PLACEMENT_CONTRACT', 'PLACEMENT_PERMANENT')
)

SELECT
job_type,
job_id,
candidate_id,
actioned_date,
offer_date,
contract_start_date start_date,
contract_end_date end_date,
days_per_week,
hours_per_day,
renewed_date,
note,
CASE
	WHEN job_type = 'PERMANENT' THEN 1
	WHEN job_type = 'CONTRACT' THEN 4
END position_type,
CASE
	WHEN job_type = 'PERMANENT' THEN 301
	WHEN job_type = 'CONTRACT' THEN 303
END application_status,
3 AS draft_offer, --used to move OFFERED to PLACED in VC [offer]
2 AS invoice_status, --used to update invoice status in VC [invoice] as 'active'
1 AS invoice_renewal_index, --default value in VC [invoice]
1 AS invoice_renewal_flow_status, --used to update flow status in VC [invoice] as 'placement_active'
1 AS invoice_valid
FROM cte_application
WHERE rn = 1
ORDER BY job_type