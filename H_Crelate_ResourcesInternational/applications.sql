WITH job_application AS (
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
to_char(wf.created_on::DATE, 'YYYY-MM-DD') date_added
-- wf.last_engagement_date_for_this_job

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
	date_added actioned_date
-- 	last_engagement_date_for_this_job,
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
)
select
job_id "application-positionExternalId",
candidate_id "application-candidateExternalId",
application_stage "application-stage",
real_stage
-- last_engagement_date_for_this_job
from app_highest_stage
where rn = 1
-- and application_stage = 'OFFERED'
order by job_id,
				CASE application_stage
					WHEN 'PLACEMENT_PERMANENT' THEN 1
					WHEN 'OFFERED' THEN 2
					WHEN 'SECOND_INTERVIEW' THEN 3
					WHEN 'FIRST_INTERVIEW' THEN 4
					WHEN 'SENT' THEN 5
					WHEN 'SHORTLISTED' THEN 6
				END ASC