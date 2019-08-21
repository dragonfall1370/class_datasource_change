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
),
candidate_fe AS (
	SELECT DISTINCT candidate_id
	FROM candidate_position cp
	JOIN functional_expertise fe ON cp.fe_id = fe.id
	WHERE rn = 1
)

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'origCv' AND path IS NOT NULL) doc ON origCv_id = doc.id


UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'formCv' AND path IS NOT NULL) doc ON formCv_id = doc.id

UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'canReg' AND path IS NOT NULL) doc ON canReg_id = doc.id

UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'docId1' AND path IS NOT NULL) doc ON docId1_id = doc.id

UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'docId2' AND path IS NOT NULL) doc ON docId2_id = doc.id

UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'docId3' AND path IS NOT NULL) doc ON docId3_id = doc.id

UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'ukVisa' AND path IS NOT NULL) doc ON ukVisa_id = doc.id

UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'specCv' AND path IS NOT NULL) doc ON specCv_id = doc.id

UNION ALL

SELECT
	c.person_id candidate_id,
	CONCAT(SUBSTRING_INDEX(SUBSTRING_INDEX(doc.path, '/', -1), '_', 2), '_', LPAD(doc.id, 10, '0'), '_', SUBSTRING_INDEX(doc.path,'_',-1)) document,
	'CANDIDATE' entity_type,
	'resume' document_type
FROM alpha_candidate c
JOIN alpha_person p ON c.person_id = p.id
JOIN candidate_fe cf ON c.person_id = cf.candidate_id
LEFT JOIN (SELECT id, path FROM alpha_document WHERE type = 'curCon' AND path IS NOT NULL) doc ON curCon = doc.id
