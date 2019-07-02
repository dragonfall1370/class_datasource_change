WITH placement_details AS (
	SELECT 
	c.contact_id candidate_id,
	j.job_id,
	p."when"::TIMESTAMP actioned_date,
	p.start_date::TIMESTAMP start_date,
	p.salary annual_salary,
	p.fee_percent fee_percent,
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
),

job_application AS (
SELECT
wf.workflow_item_id job_application_id,
target_id candidate_id,
case
	when workflow_item_status_id = 'Interview' then 'FIRST_INTERVIEW'
	when workflow_item_status_id = 'Candidate Approval' then 'SHORTLISTED'
	when workflow_item_status_id = 'Screened' then 'SHORTLISTED'
	when workflow_item_status_id = 'Offer' then 'OFFERED'
	when workflow_item_status_id = 'Declined' then 'SHORTLISTED'
	when workflow_item_status_id = 'Finalized' then 'OFFERED'
	when workflow_item_status_id = 'Submitted' then 'SENT'
	when workflow_item_status_id = 'Phone Interview' then 'FIRST_INTERVIEW'
	when workflow_item_status_id = 'Placed' then 'OFFERED'
	when workflow_item_status_id = 'On Hold' then 'SHORTLISTED'
	when workflow_item_status_id = 'Closed' then 'SHORTLISTED'
	else '' 
end application_stage,
workflow_item_status_id real_stage,
j.job_id,
wf.created_on::TIMESTAMP date_added

FROM workflow_items wf
LEFT JOIN contacts c ON wf.target_id = c.contact_id
LEFT JOIN jobs j ON wf.job_id = j.job_id
-- LEFT JOIN placements pl ON wf.targetid = pl.placedcontactid AND job.job_title_id = pl.jobtitleid AND job.account_id = pl.accountid
WHERE workflow_item_status_id NOT IN ('Old', 'Open', 'Prospects')
AND c.record_type IN ('Candidate', 'Candidate, Sales/Client Contact')
),
app_highest_stage as (
select
	job_id,
	candidate_id,
	application_stage,
	real_stage,
	date_added actioned_date,
	ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY CASE application_stage
																																WHEN 'PLACEMENT_PERMANENT' THEN 1
																																WHEN 'OFFERED' THEN 2
																																WHEN 'SECOND_INTERVIEW' THEN 3
																																WHEN 'FIRST_INTERVIEW' THEN 4
																																WHEN 'SENT' THEN 5
																																WHEN 'SHORTLISTED' THEN 6 
																															 END ASC) AS rn

from job_application
where application_stage is not null
),
final_placement_details AS (
	select
	pd.job_id,
	pd.candidate_id,
	ROW_NUMBER() OVER(PARTITION BY pd.job_id, pd.candidate_id ORDER BY pd.actioned_date) rn,
	pd.actioned_date,
	pd.start_date,
	pd.annual_salary,
	pd.fee_percent,
	pd.note

	from app_highest_stage ap
	LEFT JOIN placement_details pd ON ap.job_id = pd.job_id AND ap.candidate_id = pd.candidate_id
	where rn = 1
	AND real_stage IN ('Placed', 'Finalized')
	AND pd.job_id IS NOT NULL
	AND pd.candidate_id IS NOT NULL
),
dup_placements AS (
	SELECT *
	FROM final_placement_details
	WHERE rn = 2
)
SELECT
	pd.job_id,
	pd.candidate_id,
	pd.rn,
	pd.actioned_date,
	pd.start_date,
	COALESCE(pd.annual_salary, dp.annual_salary) annual_salary,
	COALESCE(pd.fee_percent, dp.fee_percent) fee_percent,
	pd.note,
	301 as POSITIONCANDIDATE_status,
	3 as OFFER_draft_offer, --used to move OFFERED to PLACED in VC [offer]
	2 as INVOICE_status, --used to update invoice status in VC [invoice] as 'active'
	1 as INVOICE_renewal_index, --default value in VC [invoice]
	1 as INVOICE_renewal_flow_status, --used to update flow status in VC [invoice] as 'placement_active'
	1 as INVOICE_valid
FROM final_placement_details pd
LEFT JOIN dup_placements dp ON pd.job_id = dp.job_id AND pd.candidate_id = dp.candidate_id
WHERE pd.rn = 1
AND pd.actioned_date < pd.start_date
order by job_id