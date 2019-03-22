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
	ROW_NUMBER() OVER(PARTITION BY a.account_id, c.contact_id ORDER BY a.account_id) rn
	FROM job j
	LEFT JOIN account a ON j.account = a.account_id
	LEFT JOIN company_contact c ON j.account = c.account_id AND c.rn_company = 1
	WHERE c.contact_id IS NULL
)
SELECT
account_id "contact-companyId",
contact_id "contact-externalId",
'Default' "contact-lastName" 
-- rn

FROM merge_company_contact_job
WHERE rn = 1