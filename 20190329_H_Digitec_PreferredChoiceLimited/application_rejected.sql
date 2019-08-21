with cte_merge_jobs as (
	select *,
	'PERMANENT' job_type
	from [Permanent Placement Table]
	UNION ALL
	select *,
	'CONTRACT' job_type
	from [Temporary Booking Table]
),
cte_contacts as (
	select
		con.field2 contact_id,
		com.field2 company_id
	from [Company Contact Database] con
	LEFT JOIN [Company Database] com on con.field1 = com.field2
),
cte_jobs AS (
	SELECT *
	FROM cte_merge_jobs cmj
	JOIN cte_contacts cc ON cmj.field18 = cc.contact_id AND cmj.field2A = cc.company_id
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
	job_type,
	j.field2 job_id,
	j.field2A company_id,
	c.field2 candidate_id,
	CASE
		WHEN a.stage = '1st Interview Confirmed' THEN 'FIRST_INTERVIEW'
		WHEN a.stage = '1st Interview To Arrange' THEN 'FIRST_INTERVIEW'
		WHEN a.stage = 'Awaiting Interview Result' THEN 'FIRST_INTERVIEW'
		WHEN a.stage = 'Client Not To Offer' THEN 'FIRST_INTERVIEW'
		WHEN a.stage = 'Candidate Considering Offer' THEN 'OFFERED'
		WHEN a.stage = 'Negotiating Offer' THEN 'OFFERED'
		WHEN a.stage = 'Offer Pending' THEN 'OFFERED'
		WHEN a.stage = 'Placement Due To Complete' THEN 'OFFERED'
		WHEN a.stage = 'Candidate Not Placed' THEN 'OFFERED'
		WHEN a.stage = 'Booking Completed' THEN 'OFFERED'
		WHEN a.stage = 'Booking Due To Complete' THEN 'OFFERED'
		WHEN a.stage = 'Booking Terminated' THEN 'OFFERED'
		WHEN a.stage = 'Candidate Booked' THEN 'OFFERED'
		WHEN a.stage = 'Candidate Placed' THEN 'OFFERED'
		WHEN a.stage = 'Placement Completed' THEN 'OFFERED'
		WHEN a.stage = 'Placement Terminated' THEN 'OFFERED'
		WHEN a.stage = '2nd Interview Confirmed' THEN 'SECOND_INTERVIEW'
		WHEN a.stage = '2nd Interview To Arrange' THEN 'SECOND_INTERVIEW'
		WHEN a.stage = '3rd Interview Confirmed' THEN 'SECOND_INTERVIEW'
		WHEN a.stage = '3rd Interview To Arrange' THEN 'SECOND_INTERVIEW'
		WHEN a.stage = 'Client Not To Book' THEN 'SECOND_INTERVIEW'
		WHEN a.stage = 'Client Feedback' THEN 'SENT'
		WHEN a.stage = 'CV Sent By E-Mail' THEN 'SENT'
		WHEN a.stage = 'CV Sent By Fax' THEN 'SENT'
		WHEN a.stage = 'CV Sent Via Portal' THEN 'SENT'
		WHEN a.stage = 'Send CV Again' THEN 'SENT'
		WHEN a.stage = 'Client To Be Sent CV' THEN 'SHORTLISTED'
		WHEN a.stage = 'Candidate Not Interested' THEN 'SHORTLISTED'
		WHEN a.stage = 'Candidate Not Suitable' THEN 'SHORTLISTED'
		WHEN a.stage = 'Candidate To Withdraw' THEN 'SHORTLISTED'
	END application_stage,
	a.stage real_stage,
	a.action_date
	from cte_merge_applications a
	JOIN cte_jobs j on a.job_id = j.field2
	JOIN [Candidates Database] c on a.candidate_id = c.field2	
),
app_highest_stage as (
select
	job_type,
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
current_timestamp rejected_date
-- action_date
from app_highest_stage
where rn = 1
and real_stage in ('Candidate Not Suitable', 'Candidate Not Interested', 'Client Not To Offer', 'Candidate To Withdraw', 'Candidate Not Placed')
-- and real_stage = 'Candidate Not Placed'

order by job_id,
				CASE application_stage
					WHEN 'PLACEMENT_PERMANENT' THEN 1
					WHEN 'OFFERED' THEN 2
					WHEN 'SECOND_INTERVIEW' THEN 3
					WHEN 'FIRST_INTERVIEW' THEN 4
					WHEN 'SENT' THEN 5
					WHEN 'SHORTLISTED' THEN 6
				END ASC
