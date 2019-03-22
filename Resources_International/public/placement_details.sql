SELECT 
c.contact_id candidate_id,
j.job_id,
p.created_on::TIMESTAMP actioned_date,
p."when" start_date,
p.salary annual_salary,
p.fee_percent,
concat(
CASE
	WHEN p.verb_id IS NOT NULL THEN concat('Placement verbid: ', p.verb_id, E'\n')
END,
CASE
	WHEN p.opportunity_type_id IS NOT NULL THEN concat('Opportunity type: ', p.opportunity_type_id, E'\n')
END,
CASE
	WHEN jt.title IS NOT NULL THEN concat('Job name: ', jt.title, E'\n')
END,
CASE
	WHEN strip_tags(j.description) IS NOT NULL THEN concat('Job description: ', strip_tags(j.description), E'\n')
END,
CASE
	WHEN j.compensation_details IS NOT NULL THEN concat('Compensation details: ', j.compensation_details, E'\n')
END,
CASE
	WHEN j.sales_workflow_item_status_id IS NOT NULL THEN concat('Sales workflow item status: ', j.sales_workflow_item_status_id, E'\n')
END,
CASE
	WHEN cs."name" IS NOT NULL THEN concat('Candidate source: ', cs."name", E'\n')
END,
CASE
	WHEN e.verb_id IS NOT NULL THEN concat('Experience verbid: ', e.verb_id, E'\n')
END,
CASE
	WHEN regexp_replace(e.display, E'[\\n\\r]+', ' ', 'g' ) IS NULL THEN ''
	ELSE concat('Content: ', CASE
																		WHEN e.verb_id = 'Merge' THEN regexp_replace(REPLACE(REPLACE(REPLACE(e.display, '[', ''),'"', ''), ']', ''), E'[\\n\\r]+', ' ', 'g')
																		ELSE REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(regexp_replace(REPLACE(REPLACE(REPLACE(REPLACE(e.display, '["', ''), ' ["', ''), '"]', ''),'\r\n', chr(13)), E'<[^>]*>', '', 'gi'), '&nbsp;', ' '), '&ndash;', '-'), '&rsquo;', ''''), '&lt;', '<'), '&gt;', '>')
																	END, E'\n')
END,
CASE
	WHEN pa.created_on_system IS NOT NULL THEN concat('Passed created on: ', pa.created_on_system, E'\n')
END,
CASE
	WHEN pa.passed_reason_id IS NOT NULL THEN concat('Passed reason: ', pa.created_on_system, E'\n')
END,
CASE
	WHEN wf.workflow_item_status_id IS NOT NULL THEN concat('Workflow item status: ', wf.workflow_item_status_id)
END
) note
FROM placements p
LEFT JOIN contacts c ON p.placed_contact_id = c.contact_id AND c.record_type IN ('Candidate', 'Candidate, Sales/Client Contact')
LEFT JOIN jobs j ON p.regarding_id = j.job_id
LEFT JOIN job_titles jt ON p.job_title_id = jt.job_title_id
LEFT JOIN contact_sources cs ON p.candidate_source_id = cs.contact_source_id
LEFT JOIN experiences e ON p.experience_id = e.experience_id
LEFT JOIN passed pa ON j.job_id = pa.job_id AND p.experience_id = pa.experience_id
LEFT JOIN workflow_items wf ON pa.workflow_item_id = wf.workflow_item_id
WHERE j.job_id is not NULL
AND c.contact_id is not NULL