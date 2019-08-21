WITH all_contacts AS (
	SELECT
	cp.id_person contact_id,
	cp.sort_order,
	cp.id_company company_id,
	px.full_name,
	cp.created_on,
	ROW_NUMBER() OVER(PARTITION BY cp.id_person ORDER BY cp.sort_order ASC) rn,
	ROW_NUMBER() OVER(PARTITION BY cp.id_company ORDER BY cp.created_on ASC) rn_contact
	FROM company_person cp
	JOIN person_x px ON cp.id_person = px.id_person AND px.is_deleted = 0
	LEFT JOIN "user" u ON px.id_user = u.id_user
),
cte_contact AS (
	SELECT *
	FROM all_contacts
	WHERE rn = 1
),
cte_job AS (
	SELECT
	a.id_assignment job_id,
	a.id_company company_id,
	COALESCE(cc.contact_id, acs.contact_id, '1') contact_id,
	ac.contacted_on,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(a.assignment_title, 'INterim', 'Interim'),'manager', 'Manager'), 'interim', 'Interim'), 'engineering', 'Engineering'), 'skilled', 'Skilled'), 'nights', 'Nights') job_title,
	c.value currency,
	a.estimated_value actual_salary,
	a.salary_from job_salary_from,
	a.salary_to job_salary_to,
	u.user_email owner_email,
	a.estimated_start_date open_date,
	ROW_NUMBER() OVER(PARTITION BY COALESCE(cc.contact_id, acs.contact_id), LOWER(a.assignment_title) ORDER BY a.created_on ASC) rn_title,
	ROW_NUMBER() OVER(PARTITION BY a.id_assignment ORDER BY ac.contacted_on ASC) rn,
	CONCAT_WS(
		E'\n',
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_no::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Assignment number: ', REPLACE(a.assignment_no::text, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_reference, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Assignment reference: ', REPLACE(a.assignment_reference, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(ass.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Status: ', REPLACE(ass.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(ate.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Type: ', REPLACE(ate.value, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.estimated_fee::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Estimated fee: ', REPLACE(a.estimated_fee::text, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.fee_comment::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Fee comment: ', REPLACE(a.fee_comment::text, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.final_fee::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Final fee: ', REPLACE(a.final_fee::text, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(ac.notes, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Assignment contact note: ', REPLACE(ac.notes, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_brief, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Assignment brief: ', REPLACE(a.assignment_brief, '\x0d\x0a', ' ')) END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_comment, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Assignment comment: ', REPLACE(a.assignment_comment, '\x0d\x0a', ' ')) END
-- 		CASE WHEN NULLIF(TRIM(REPLACE(a.estimated_value::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Estimated value: ', REPLACE(a.estimated_value::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(abi.consultant, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Consultant: ', REPLACE(abi.consultant, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(abi.fee::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Consultant fee: ', REPLACE(abi.fee::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(ao.value, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Origin: ', REPLACE(ao.value, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(ac.contacted_by, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Contacted by: ', REPLACE(ac.contacted_by, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(ac.contact_subject, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Contacted subject: ', REPLACE(ac.contact_subject, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(a.local_salary_from::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Local salary from: ', REPLACE(a.local_salary_from::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(a.local_salary_to::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Local salary to: ', REPLACE(a.local_salary_to::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(a.package_comment, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Package comment: ', REPLACE(a.package_comment, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(a.created_on, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Created on: ', REPLACE(a.created_on, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(a.base_salary_from::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Base salary from: ', REPLACE(a.base_salary_from::text, '\x0d\x0a', ' ')) END,
-- 		CASE WHEN NULLIF(TRIM(REPLACE(a.base_salary_to::text, '\x0d\x0a', '')), '') IS NOT NULL THEN concat('Base salary from: ', REPLACE(a.base_salary_to::text, '\x0d\x0a', ' ')) END,
	) note
	FROM assignment_contact ac
	JOIN "assignment" a ON ac.id_assignment = a.id_assignment AND a.is_deleted = 0
	LEFT JOIN cte_contact cc ON ac.id_person = cc.contact_id AND cc.rn = 1
	LEFT JOIN all_contacts acs ON acs.company_id = a.id_company AND acs.rn_contact = 1
	LEFT JOIN currency c ON a.id_currency = c.id_currency
	LEFT JOIN "user" u ON a.id_user = u.id_user
	LEFT JOIN assignment_origin ao ON a.id_assignment_origin = ao.id_assignment_origin
	LEFT JOIN assignment_status ass ON a.id_assignment_status = ass.id_assignment_status
	LEFT JOIN assignment_type ate ON a.id_assignment_type = ate.id_assignment_type
	LEFT JOIN assignment_ext ae ON a.id_assignment = ae.id_assignment
	LEFT JOIN assignment_b_i abi ON a.id_assignment = abi.id_assignment
	WHERE a.is_deleted = 0
)

SELECT
job_id "position-externalId",
contact_id "position-contactId",
CASE
	WHEN rn_title <> 1 THEN concat(job_title, ' ', rn_title)
	ELSE job_title
END "position-title",
owner_email "position-owners",
-- currency,
-- open_date "position-startDate",
actual_salary "position-actualSalary",
note "position-note"
FROM cte_job j
WHERE rn = 1