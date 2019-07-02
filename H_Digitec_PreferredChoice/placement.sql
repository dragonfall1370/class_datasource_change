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
	field46 action_date,
	field79 offer_date,
	Field69 start_date,
	field71 end_date
	from [Permanent Table]
	UNION ALL
	select
	field101 job_id,
	field102 candidate_id,
	field55 stage,
	field46 action_date,
	field79 offer_date,
	Field69 start_date,
	field71 end_date
	from [Booking Table]
),
applications as (
	select
	job_type,
	j.field2 job_id,
	j.field2A company_id,
	c.field2 candidate_id,
	a.action_date,
	a.offer_date,
	a.start_date,
	a.end_date,
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
	a.stage real_stage
	from cte_merge_applications a
	JOIN cte_jobs j on a.job_id = j.field2
	JOIN [Candidates Database] c on a.candidate_id = c.field2	
),
app_highest_stage as (
select
	*,
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
-- offer_date,
start_date,
'pound' currency_type,
-- end_date,
-- application_stage "application-stage",
CASE
	WHEN job_type = 'PERMANENT' THEN 1
	WHEN job_type = 'CONTRACT' THEN 4
END position_type,
CASE
	WHEN job_type = 'PERMANENT' THEN 301
	WHEN job_type = 'CONTRACT' THEN 303
END application_status,
3 as draft_offer, --used to move OFFERED to PLACED in VC [offer]
2 as invoice_status, --used to update invoice status in VC [invoice] as 'active'
1 as invoice_renewal_index, --default value in VC [invoice]
1 as invoice_renewal_flow_status, --used to update flow status in VC [invoice] as 'placement_active'
1 as invoice_valid

-- action_date
from app_highest_stage
where rn = 1
and real_stage in ('Booking Completed', 'Booking Due To Complete', 'Booking Terminated', 'Candidate Booked', 'Candidate Placed', 'Placement Completed', 'Placement Terminated')

order by job_id,
				CASE application_stage
					WHEN 'PLACEMENT_PERMANENT' THEN 1
					WHEN 'OFFERED' THEN 2
					WHEN 'SECOND_INTERVIEW' THEN 3
					WHEN 'FIRST_INTERVIEW' THEN 4
					WHEN 'SENT' THEN 5
					WHEN 'SHORTLISTED' THEN 6
				END ASC