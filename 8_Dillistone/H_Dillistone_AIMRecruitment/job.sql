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
	LEFT JOIN selected_company sc ON cp.id_company = sc.idcompany
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
	CASE
		WHEN f.id_assignment IS NULL THEN 'PERMANENT'
		ELSE 'CONTRACT'
	END job_type,
	a.id_company company_id,
	COALESCE(cc.contact_id, acs.contact_id, '1') contact_id,
	ac.contacted_on,
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(a.assignment_title, 'INterim', 'Interim'),'manager', 'Manager'), 'interim', 'Interim'), 'engineering', 'Engineering'), 'skilled', 'Skilled'), 'nights', 'Nights') job_title,
	c.value currency,
	a.salary_from actual_salary,
	a.salary_to job_salary_to,
	u.user_email owner_email,
	a.estimated_start_date open_date,
	ROW_NUMBER() OVER(PARTITION BY COALESCE(cc.contact_id, acs.contact_id), LOWER(a.assignment_title) ORDER BY a.created_on ASC) rn_title,
	ROW_NUMBER() OVER(PARTITION BY a.id_assignment ORDER BY ac.contacted_on ASC) rn,
	concat(
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_no::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Assignment number: ', REPLACE(a.assignment_no::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_reference, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Assignment reference: ', REPLACE(a.assignment_reference, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(abi.consultant, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Consultant: ', REPLACE(abi.consultant, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(abi.fee::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Consultant fee: ', REPLACE(abi.fee::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN ac.is_guest_assignment_contact = 0 THEN concat('Is guest assignment contact: No', E'\n') ELSE concat('Is guest assignment contact: Yes', E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.estimated_fee::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Estimated fee: ', REPLACE(a.estimated_fee::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.estimated_value::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Estimated value: ', REPLACE(a.estimated_value::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.fee_comment::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Fee comment: ', REPLACE(a.fee_comment::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.final_fee::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Final fee: ', REPLACE(a.final_fee::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ao.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Origin: ', REPLACE(ao.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ass.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Status: ', REPLACE(ass.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ate.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Type: ', REPLACE(ate.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.last_contacted_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Last contacted on: ', REPLACE(a.last_contacted_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ac.contacted_by, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contacted by: ', REPLACE(ac.contacted_by, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ac.contact_subject, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contacted subject: ', REPLACE(ac.contact_subject, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.local_salary_from::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Local salary from: ', REPLACE(a.local_salary_from::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.local_salary_to::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Local salary to: ', REPLACE(a.local_salary_to::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.package_comment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Package comment: ', REPLACE(a.package_comment, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.created_on, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Created on: ', REPLACE(a.created_on, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.base_salary_from::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Base salary from: ', REPLACE(a.base_salary_from::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.base_salary_to::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Base salary from: ', REPLACE(a.base_salary_to::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ae.ud_field1, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Assignment user defined field 1: ', REPLACE(ae.ud_field1, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ae.ud_field2, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Assignment user defined field 2: ', REPLACE(ae.ud_field2, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(fe.ud_field1, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Flex user defined field 1: ', REPLACE(fe.ud_field1, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.contract_start_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contract start date: ', REPLACE(f.contract_start_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.contract_end_date, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contract end date: ', REPLACE(f.contract_end_date, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ut.value, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Unit type: ', REPLACE(ut.value, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.client_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Client rate: ', REPLACE(f.client_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.contractor_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Contractor rate: ', REPLACE(f.contractor_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.rate_comment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Rate comment: ', REPLACE(f.rate_comment, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.number_of_positions::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Number of positions: ', REPLACE(f.number_of_positions::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.base_client_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Base client rate: ', REPLACE(f.base_client_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(f.base_contractor_rate::text, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Base contractor rate: ', REPLACE(f.base_contractor_rate::text, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(ac.notes, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Assignment contact note: ', REPLACE(ac.notes, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_brief, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Assignment brief: ', REPLACE(a.assignment_brief, '\x0d\x0a', ' '), E'\n') END,
		CASE WHEN NULLIF(TRIM(REPLACE(a.assignment_comment, '\x0d\x0a', '')), '') IS NULL THEN '' ELSE concat('Assignment comment: ', REPLACE(a.assignment_comment, '\x0d\x0a', ' '), E'\n') END
	) note
	FROM assignment_contact ac
	JOIN "assignment" a ON ac.id_assignment = a.id_assignment AND a.is_deleted = 0
	LEFT JOIN cte_contact cc ON ac.id_person = cc.contact_id AND cc.rn = 1
	LEFT JOIN all_contacts acs ON acs.company_id = a.id_company AND acs.rn_contact = 1
	LEFT JOIN currency c ON a.id_currency = c.id_currency
	LEFT JOIN "user" u ON a.id_user = u.id_user
	LEFT JOIN flex f ON a.id_assignment = f.id_assignment
	LEFT JOIN assignment_origin ao ON a.id_assignment_origin = ao.id_assignment_origin
	LEFT JOIN assignment_status ass ON a.id_assignment_status = ass.id_assignment_status
	LEFT JOIN assignment_type ate ON a.id_assignment_type = ate.id_assignment_type
	LEFT JOIN flex_ext fe ON f.id_flex = fe.id_flex
	LEFT JOIN unit_type ut ON f.id_unit_type = ut.id_unit_type
	LEFT JOIN assignment_ext ae ON a.id_assignment = ae.id_assignment
	LEFT JOIN assignment_b_i abi ON a.id_assignment = abi.id_assignment
	WHERE a.is_deleted = 0
)

SELECT
job_id "position-externalId",
job_type "position-type",
contact_id "position-contactId",
CASE
	WHEN rn_title <> 1 THEN concat(job_title, ' ', rn_title)
	ELSE job_title
END "position-title",
owner_email "position-owners",
currency,
open_date "position-startDate",
actual_salary "position-actualSalary",
note "position-note"
FROM cte_job j
WHERE rn = 1