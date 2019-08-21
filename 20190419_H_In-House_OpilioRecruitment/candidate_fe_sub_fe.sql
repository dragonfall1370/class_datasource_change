WITH functional_expertise AS (
	SELECT
	id,
	jobFunction sub_functional_expertise,
	type functional_expertise
	FROM `alpha_sel_job_function`
	WHERE type IN ('Marketing', 'Project Management', 'Sales')
	AND jobFunction IN ('Ad Operations', 'Brand Marketing', 'Category Manager', 'Chief Marketing Officer', 'Communications & PR', 
												'CRM', 'Digital Marketing', 'eCommerce', 'Insight & Analytics', 'Marketing Events',
												'Media Planning / Buying', 'PPC', 'SEO', 'Social Media', 'Business Analysts',
												'Chief Digital Officer', 'Digital Project Manager', 'Events Manager', 'IT Programme Manager', 'IT Project Manager', 
												'PMO Analyst', 'PMO Assurance', 'Product Developer', 'Product Manager', 'Project Administrator',
												'Project Director', 'Project Manager', 'Project Resource Planner', 'Project Support/Co-ordinator', 'Risk Manager',
												'Transformation and Change Manager', 'COO', 'MD / CEO / VP')
),
candidate_position AS (
	SELECT
	c.person_id candidate_id,
	p.jobFunction_id fe_id,
	ROW_NUMBER() OVER(PARTITION BY c.person_id, p.jobFunction_id ORDER BY c.person_id) rn
	FROM alpha_candidate c
	LEFT JOIN alpha_position p ON c.person_id = p.person_id
	WHERE p.jobFunction_id IS NOT NULL
)

SELECT
candidate_id,
sub_functional_expertise,
functional_expertise
FROM candidate_position cp
JOIN functional_expertise fe ON cp.fe_id = fe.id
WHERE rn = 1