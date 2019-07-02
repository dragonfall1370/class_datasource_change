WITH link_contact_job AS (
	SELECT
		*,
		ROW_NUMBER () OVER ( PARTITION BY job_id ORDER BY contact_id ) rn 
	FROM
		(
		SELECT
			j.job_id,
			j.account_id,
			c.contact_id 
		FROM
			jobs j
			LEFT JOIN ( SELECT DISTINCT job_id, target_id FROM workflow_items ) w ON j.job_id = w.job_id
			LEFT JOIN contacts c ON w.target_id = c.contact_id 
			AND c.record_type IN ( 'Lead Contact', 'Sales/Client Contact', 'Candidate, Sales/Client Contact' ) 
		UNION
		SELECT
			j.job_id,
			j.account_id,
			c.last_activity_regarding_id 
		FROM
			jobs j
			LEFT JOIN contacts c ON j.job_id = c.last_activity_regarding_id 
			AND c.record_type IN ( 'Lead Contact', 'Sales/Client Contact', 'Candidate, Sales/Client Contact' ) 
		) TEMP 
	),
	link_company_contact AS (
	SELECT 
		c.contact_id,
		ROW_NUMBER () OVER ( PARTITION BY c.contact_id ORDER BY A.account_id ) rn,
		a.account_id 
	FROM
		contacts c LEFT JOIN ( SELECT DISTINCT target_entity_id contact_id, where_id account_id FROM timeline_items ) l ON c.contact_id = l.contact_id
		LEFT JOIN accounts a ON l.account_id = a.account_id 
	WHERE
		c.record_type IN ( 'Lead Contact', 'Sales/Client Contact', 'Candidate, Sales/Client Contact' ) 
	),
	merge_company_contact_job AS (
	SELECT
		cj.job_id,
		cc.account_id,
		cc.contact_id,
		ROW_NUMBER () OVER ( PARTITION BY job_id ORDER BY cj.account_id ) rn 
	FROM link_contact_job cj
	LEFT JOIN link_company_contact cc ON cj.account_id = cc.account_id 
	WHERE
		cj.rn = 1 
		AND cc.rn = 1 
	),
	cte_jobs AS (
	SELECT
		j.job_id,
		jt.title,
		a.account_id,
		cj.contact_id,
		ROW_NUMBER() OVER (PARTITION BY jt.title ORDER BY j.job_id ) AS rn_title,
		ROW_NUMBER() OVER (PARTITION BY cj.contact_id ORDER BY j.job_id) AS rn_job,
		COALESCE ( to_char( j.start_date :: DATE, 'YYYY-MM-DD' ), '' ) open_date,
		COALESCE ( strip_tags ( j.description ), '' ) job_description,
		COALESCE ( strip_tags ( A.description ), '' ) note 
	FROM jobs j
		LEFT JOIN job_titles jt ON j.job_title_id = jt.job_title_id
		LEFT JOIN accounts a ON j.account_id = a.account_id
		LEFT JOIN merge_company_contact_job cj ON j.job_id = cj.job_id AND cj.rn = 1
	) 
	
	SELECT
	CASE
		WHEN account_id IS NULL THEN '1'
		ELSE account_id
	END "contact-companyId",
	CASE
		WHEN contact_id IS NULL THEN (rn_job + 1)::text
		ELSE contact_id
	END "contact-externalId",
'Default' "contact-lastName"
FROM cte_jobs j
where contact_id is null

