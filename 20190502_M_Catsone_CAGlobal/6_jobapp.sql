with JobApp as (select p.id
	, p.candidate_id
	, p.job_id
	, p.status_id
	, case when s.title in ('New Application','Not Suitable: Applied / Searched','Uncontactable','Not Suitable: Contacted'
						,'Not Interested','Rejected: Assessment Completed','Candidate Withdrawn','Regret: Job Order Status Changed','Blacklisted Candidate'
						,'Off the Market','Website Applicant','Email Application','Contacted','Contacted - CV Already Submitted'
						,'Assessments and / or Medicals','Shortlisted') then 1 --SHORTLISTED
		when s.title in ('Rejected: Client - CV Submitted','Rejected: Client - CV Already Received','Rejected: Client - Interviewed','CV Sent: Submitted') then 2 --SENT
		when s.title in ('Interview Requested','Interviewed Realised (1)') then 3 --FIRST_INTERVIEW
		when s.title in ('Interview Realised (2 & 3)') then 4 --SECOND_INTERVIEW
		when s.title in ('Offered','Offer Declined') then 5 --OFFERED
		when s.title in ('Placed: Offer Accepted','Placed (Contractor): Offer Accepted','Fall Away Placement') then 6 --PLACED
		else 0 end as Appstage
	, case when s.title in ('Shortlisted') then NULL --SHORTLISTED
		when s.title in ('Offered') then NULL --OFFERED
		else trim(s.title) end as Substatus
	, case when s.title in ('Not Suitable: Applied / Searched','Uncontactable','Not Suitable: Contacted'
							,'Not Interested','Rejected: Assessment Completed','Candidate Withdrawn','Regret: Job Order Status Changed','Blacklisted Candidate'
							,'Off the Market') then 'Rejected' --SHORTLISTED
		when s.title in ('Rejected: Client - CV Submitted','Rejected: Client - CV Already Received','Rejected: Client - Interviewed') then 'Rejected' --SENT
		else NULL end as RejectedStatus
	, case when s.title in ('Not Suitable: Applied / Searched','Uncontactable','Not Suitable: Contacted'
							,'Not Interested','Rejected: Assessment Completed','Candidate Withdrawn','Regret: Job Order Status Changed','Blacklisted Candidate'
							,'Off the Market') then try_parse(p.date_created as datetime) --SHORTLISTED
		when s.title in ('Rejected: Client - CV Submitted','Rejected: Client - CV Already Received','Rejected: Client - Interviewed') 
				then try_parse(p.date_created as datetime) --SENT
		else NULL end as RejectedDate
	, case when p.date_created is null or p.date_created = 'null' then coalesce(try_parse(p.date_modified as datetime),getdate())
		else try_parse(p.date_created as datetime) end as AppDate
from pipelines p
left join status s on p.status_id = s.id)

--HIGHEST STAGE
, HighestState as (select id
	, candidate_id
	, job_id
	, status_id
	, Appstage
	, row_number() over (partition by candidate_id, job_id order by Appstage desc, id desc, AppDate desc) as jobApprn
	, Substatus
	, RejectedStatus
	, RejectedDate
	, AppDate
	from JobApp)

--select distinct Substatus from HighestState

, JobType as (select cv.jobs_id
	, cv.cf_value
	, c1.label as jobtype
	from jobs_custom_fields_value cv
	left join jobs_custom_fields_153614 c1 on c1.id = cv.cf_value
	where cv.cf_id = 153614) --Job Type

---MAIN SCRIPT
select concat('CG',candidate_id) as CandidateExtID
, concat('CG',job_id) as JobExtID
, case when Appstage = 6 then 'PLACEMENT_PERMANENT'
		when Appstage = 5 then 'OFFERED'
		when Appstage = 4 then 'SECOND_INTERVIEW'
		when Appstage = 3 then 'FIRST_INTERVIEW'
		when Appstage = 2 then 'SENT'
		when Appstage = 1 then 'SHORTLISTED'
		end as [Originalstage]
	, case 
		when Appstage = 6 then 'OFFERED' --to be updated as PLACED afterward
		when Appstage = 5 then 'OFFERED'
		when Appstage = 4 then 'SECOND_INTERVIEW'
		when Appstage = 3 then 'FIRST_INTERVIEW'
		when Appstage = 2 then 'SENT'
		when Appstage = 1 then 'SHORTLISTED'
		end as [application-stage]
	, AppDate as ActionedDate
	, RejectedStatus
	, RejectedDate
	, iif(Substatus is null, '',Substatus) as Substatus 
	, case 
		when jt.JobType = 'FIFO' then 303
		when jt.JobType = 'Contract Position' then 302
		when jt.JobType = 'Permanent Position' then 301
		when jt.JobType = 'Residential' then 301
		when jt.JobType = 'Contractor Payroll' then 302
		else 301 end as PlacedStage --if Appstage = 6
	, 3 as draft_offer --used to move OFFERED to PLACED in VC [offer]
	, 2 as InvoiceStatus --used to update invoice status in VC [invoice] as 'active'
	, 1 as renewal_index --default value in VC [invoice]
	, 1 as renewal_flow_status
from HighestState
left join JobType jt on jt.jobs_id = HighestState.job_id
where jobApprn = 1
--and Appstage = 6 --839
order by candidate_id desc --, HighestState.job_id --604853 rows