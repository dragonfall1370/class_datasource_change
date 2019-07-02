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
	j.max_salary::int,
	j.min_salary::int

	
FROM job j
LEFT JOIN merge_company_contact_job mj ON j.job_id = mj.job_id
)

SELECT
job_id,
contact_id,
max_salary,
min_salary
FROM cte_job
