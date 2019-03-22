WITH company_contact AS (
	SELECT
	a.account_id,
	c.contact_id,
	ROW_NUMBER() OVER(PARTITION BY a.account_id ORDER BY a.account_id) rn_company,
	ROW_NUMBER() OVER(PARTITION BY c.contact_id ORDER BY a.account_id) rn_contact
	FROM account a
	LEFT JOIN contact c ON a.account_id = c.account_id AND c.record_type_id = '01261000000gXax'
),
merge_company_contact_job AS (
	SELECT
	job_id,
	a.account_id,
	CASE
		WHEN c.contact_id IS NULL THEN (rn_contact + 1)::text
		ELSE c.contact_id
	END,
	ROW_NUMBER() OVER(PARTITION BY job_id ORDER BY job_id) rn
	FROM job j
	LEFT JOIN account a ON j.account = a.account_id
	LEFT JOIN company_contact c ON j.account = c.account_id AND c.rn_company = 1
),
cte_job AS (
SELECT
	j.job_id,
	mj.contact_id,
	mj.account_id,
	j.name job_title,
	ROW_NUMBER() OVER(PARTITION BY mj.contact_id, j.name ORDER BY j.job_id) rn,
	to_char(j.created_date::DATE, 'YYYY-MM-DD' ) created_date,
	to_char(j.date_filled::DATE, 'YYYY-MM-DD' ) closed_date,
	strip_tags(j.job_description) public_description,
	openings:: int,
-- 	j.job_number,
	j.max_salary,
	j.min_pay_rate::int,
	j.min_salary,
	u.email owner_email,
	j.job_opportunity,
	CASE
		WHEN rt.name = 'Perm' THEN 'PERMANENT'
		WHEN rt.name = 'Temp' THEN 'TEMPORARY'
	END job_type,
	j.status,
	concat(
		CASE WHEN j.job_number IS NULL THEN '' ELSE concat('Job number: ', j.job_number, E'\n') END,
		CASE WHEN j.c_date IS NULL THEN '' ELSE concat('C date: ', j.c_date, E'\n') END,
		CASE WHEN j.closed_reason IS NULL THEN '' ELSE concat('Closed reason: ', j.closed_reason, E'\n') END,
		CASE WHEN j.date_filled IS NULL THEN '' ELSE concat('Date filled: ', j.date_filled, E'\n') END,
		CASE WHEN j.estimated_start_date IS NULL THEN '' ELSE concat('Estimated start date: ', j.estimated_start_date, E'\n') END,
		CASE WHEN j.fee_pct IS NULL THEN '' ELSE concat('Fee percentage: ', j.fee_pct, E'\n') END,
		CASE WHEN j.status IS NULL THEN '' ELSE concat('Status: ', j.status, E'\n') END,
		CASE WHEN j.custom_picklist4 IS NULL THEN '' ELSE concat('Custom picklist 4: ', j.custom_picklist4, E'\n') END,
		CASE WHEN j.custom_picklist5 IS NULL THEN '' ELSE concat('Custom picklist 5: ', j.custom_picklist5, E'\n') END,
		CASE WHEN j.custom_short1 IS NULL THEN '' ELSE concat('Custom short 1: ', j.custom_short1, E'\n') END,
		CASE WHEN j.close_probability IS NULL THEN '' ELSE concat('Close probability: ', j.close_probability) END
	) note
FROM job j
LEFT JOIN contact c ON j.contact = c.contact_id AND c.record_type_id = '01261000000gXax'
LEFT JOIN "user" u ON j.recruiter = u.user_id
LEFT JOIN record_type rt ON concat(j.record_type_id, 'AAU') = rt.record_type_id
LEFT JOIN merge_company_contact_job mj ON j.job_id = mj.job_id
AND j.is_deleted = 0
)

SELECT
job_id "position-externalId",
contact_id "position-contactId",
CASE
	WHEN rn <> 1 THEN concat(job_title, ' ', rn)
	ELSE job_title
END "position-title",
created_date "position-startDate",
closed_date "position-endDate",
public_description "position-publicDescription",
openings "position-headcount",
min_pay_rate "position-payRate",
owner_email "position-owners",
job_type "position-type",
note "position-note"
FROM cte_job
-- where rn <> 1
-- where closed_date is not null