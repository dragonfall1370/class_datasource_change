with cte_merge_jobs as (
	select *,
	'PERMANENT' type
	from [Permanent Placement Table]
	UNION ALL
	select *,
	'CONTRACT' type
	from [Temporary Booking Table]
),
cte_contacts as (
	select
		con.field2 contact_id,
		com.field2 company_id
	from [Company Contact Database] con
	LEFT JOIN [Company Database] com on con.field1 = com.field2
),
cte_merge_applications as (
	select
	field101 job_id,
	field102 candidate_id,
	field55 stage,
	field46 action_date
	from [Permanent Table]
	UNION ALL
	select
	field101 job_id,
	field102 candidate_id,
	field55 stage,
	field46 action_date
	from [Booking Table]
),
applications as (
	select
	type,
	j.field2 job_id,
	j.field2A company_id,
	c.field2 candidate_id,
	case
		when a.stage = 'Candidate Not Suitable' then 'SHORTLISTED'
		when a.stage = 'Candidate Considering Offer' then 'OFFERED'
		when a.stage = 'Negotiating Offer' then 'OFFERED'
		when a.stage = '2nd Interview Confirmed' then 'SECOND_INTERVIEW'
		when a.stage = 'Candidate Not Interested' then 'SHORTLISTED'
		when a.stage = 'Client To Be Sent CV' then 'SHORTLISTED'
		when a.stage = 'CV Sent Via Portal' then 'SENT'
		when a.stage = 'Client Not To Offer' then 'FIRST_INTERVIEW'
		when a.stage = 'CV Sent By Fax' then 'SENT'
		when a.stage = 'Candidate To Withdraw' then 'SHORTLISTED'
		when a.stage = 'Placement Due To Complete' then 'OFFERED'
		when a.stage = 'Placement Terminated' then 'OFFERED'
		when a.stage = 'Candidate Placed' then 'OFFERED'
		when a.stage = 'Client Feedback' then 'SENT'
		when a.stage = 'Offer Pending' then 'OFFERED'
		when a.stage = '2nd Interview To Arrange' then 'SECOND_INTERVIEW'
		when a.stage = 'CV Sent By E-Mail' then 'SENT'
		when a.stage = 'Placement Completed' then 'OFFERED'
		when a.stage = '3rd Interview To Arrange' then 'SECOND_INTERVIEW'
		when a.stage = 'Send CV Again' then 'SENT'
		when a.stage = '1st Interview Confirmed' then 'FIRST_INTERVIEW'
		when a.stage = 'Candidate Not Placed' then 'OFFERED'
		when a.stage = '3rd Interview Confirmed' then 'SECOND_INTERVIEW'
		when a.stage = '1st Interview To Arrange' then 'FIRST_INTERVIEW'
		when a.stage = 'Awaiting Interview Result' then 'FIRST_INTERVIEW'
		when a.stage = 'Candidate Booked' then 'OFFERED'
		when a.stage = 'Client Not To Book' then 'SECOND_INTERVIEW'
		when a.stage = 'Booking Due To Complete' then 'OFFERED'
		when a.stage = 'Retry Action - Record Locked' then ''
		when a.stage = 'Booking Completed' then 'OFFERED'
		when a.stage = 'Booking Terminated' then 'OFFERED'
	end application_stage,
	a.stage real_stage,
	a.action_date
	from cte_merge_applications a
	JOIN cte_merge_jobs j on a.job_id = j.field2
	JOIN cte_contacts cc ON j.field18 = cc.contact_id AND j.field2A = cc.company_id
	JOIN [Candidates Database] c on a.candidate_id = c.field2	
),
app_highest_stage as (
select
	type,
	job_id,
	company_id,
	candidate_id,
	application_stage,
	real_stage,
	action_date,
	ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY CASE application_stage
																																WHEN 'PLACEMENT_PERMANENT' THEN 1
																																WHEN 'OFFERED' THEN 2
																																WHEN 'SECOND_INTERVIEW' THEN 3
																																WHEN 'FIRST_INTERVIEW' THEN 4
																																WHEN 'SENT' THEN 5
																																WHEN 'SHORTLISTED' THEN 6 
																															 END ASC) AS rn

from applications
where application_stage is not null
)

select
job_id,
candidate_id,
-- application_stage "application-stage",
-- real_stage,
301 as POSITIONCANDIDATE_status,
3 as OFFER_draft_offer, --used to move OFFERED to PLACED in VC [offer]
2 as INVOICE_status, --used to update invoice status in VC [invoice] as 'active'
1 as INVOICE_renewal_index, --default value in VC [invoice]
1 as INVOICE_renewal_flow_status, --used to update flow status in VC [invoice] as 'placement_active'
1 as INVOICE_valid

-- action_date
from app_highest_stage
where rn = 1
and real_stage in ('Placement Completed', 'Candidate Placed', 'Placement Terminated')
-- and real_stage = 'Placement Terminated'

order by job_id,
				CASE application_stage
					WHEN 'PLACEMENT_PERMANENT' THEN 1
					WHEN 'OFFERED' THEN 2
					WHEN 'SECOND_INTERVIEW' THEN 3
					WHEN 'FIRST_INTERVIEW' THEN 4
					WHEN 'SENT' THEN 5
					WHEN 'SHORTLISTED' THEN 6
				END ASC