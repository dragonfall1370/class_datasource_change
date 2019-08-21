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
selected_company AS (
	SELECT
		ct.company_id
	FROM companies_tags ct
	JOIN alpha_tag alt ON ct.tag_id = alt.id
	WHERE alt.id = 477
),
current_contact AS (
	SELECT
	cc.person_id contact_id,
	cc.jobFunction_id,
	ROW_NUMBER() OVER(PARTITION BY cc.person_id ORDER BY STR_TO_DATE(REPLACE(cc.startDate, '/', '-'), '%d-%m-%Y') DESC) rn
	FROM alpha_position cc
	JOIN alpha_person ap ON cc.person_id = ap.id
	JOIN alpha_company ac ON cc.company_id = ac.id
	JOIN selected_company sc ON cc.id = sc.company_id
)
SELECT
contact_id,
sub_functional_expertise,
functional_expertise
FROM current_contact cc
JOIN functional_expertise fe ON cc.jobFunction_id = fe.id
WHERE rn = 1
-- AND sub_functional_expertise IS NULL