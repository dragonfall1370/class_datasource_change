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
	a.created_date::TIMESTAMP created_date,
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
	rt.name job_type

	FROM application a
	LEFT JOIN job j ON a.job = j.job_id
	LEFT JOIN contact c ON a.candidate_contact = c.contact_id AND c.record_type_id = '01261000000gXaw'
	LEFT JOIN record_type rt ON concat(a.record_type_id, 'AAU') = rt.record_type_id
	LEFT JOIN map_stage ms ON j.job_id = ms.job_id AND c.contact_id = ms.candidate_id
	WHERE c.contact_id is not null
	AND a.stage <> 'Candidate Search'	
),
app_placement AS (SELECT
job_id,
candidate_id,
application_stage,
job_type,
a.status,
p.status real_status,
-- a.created_date,
p.created_date::TIMESTAMP offer_date,
p.start_date::TIMESTAMP,
p.app_p_date,
p.salary,
concat(
CASE WHEN p.hiring_manager IS NOT NULL AND c.first_name IS NOT NULL OR c.last_name IS NOT NULL THEN concat('Hiring manager: ', c.first_name, ' ', c.last_name, concat(' - ', c.email), E'\n') END,
CASE WHEN p.filled_by IS NOT NULL AND u.first_name IS NOT NULL OR u.last_name IS NOT NULL THEN concat('Filled by: ', u.first_name, ' ', c.last_name, concat(' - ', u.email), E'\n') END,
CASE WHEN p.bill_rate IS NOT NULL THEN concat('Bill rate: ', p.bill_rate, E'\n') END,
CASE WHEN p.fee_pct IS NOT NULL THEN concat('Fee percentage: ', p.fee_pct, E'\n') END,
CASE WHEN p.filled_pct IS NOT NULL THEN concat('Filled percentage: ', p.filled_pct, E'\n') END,
CASE WHEN p.pay_rate IS NOT NULL THEN concat('Pay rate: ', p.pay_rate) END
) note,
p.status placement_status,
ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY job_id) rn
FROM cte_application a
LEFT JOIN placement p ON a.job_id = p.job AND a.candidate_id = p.employee
LEFT JOIN contact c ON p.hiring_manager = c.contact_id AND c.record_type_id = '01261000000gXax'
LEFT JOIN "user" u ON p.filled_by = u.user_id
WHERE real_stage = 'Placement')

SELECT
job_id,
candidate_id,
offer_date,
(start_date::DATE - INTERVAL '30 DAY')::TIMESTAMP placement_date,
start_date,
CASE
	WHEN job_type = 'Perm' THEN 1
	WHEN job_type = 'Temp' THEN 4
END position_type,
status,
real_status,
salary,
note,
CASE
	WHEN job_type = 'Perm' THEN 301
	WHEN job_type = 'Temp' THEN 303
END application_status,
3 as draft_offer, --used to move OFFERED to PLACED in VC [offer]
CASE
	WHEN real_status = 'Active' THEN 2 
	WHEN real_status = 'In Process' THEN 1
END invoice_status, --used to update invoice status in VC [invoice] as 'active'
1 as invoice_renewal_index, --default value in VC [invoice]
CASE
	WHEN real_status = 'Active' THEN 1
	WHEN real_status = 'In Process' THEN 8
END invoice_renewal_flow_status, --used to update flow status in VC [invoice] as 'placement_active'
1 as invoice_valid

FROM app_placement
WHERE rn = 1