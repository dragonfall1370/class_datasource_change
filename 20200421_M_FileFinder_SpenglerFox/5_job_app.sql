WITH cte_candidate AS (SELECT
	c.idperson candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
) --586657 rows

, cte_job AS (
	SELECT a.idassignment job_id,
	CASE
		WHEN f.idassignment IS NULL THEN 'PERMANENT'
		ELSE 'CONTRACT' END job_type
	, ROW_NUMBER() OVER(PARTITION BY a.idassignment ORDER BY ac.contactedon ASC) rn
	FROM assignmentcontact ac
	JOIN (select * from "assignment" where isdeleted = '0') a ON ac.idassignment = a.idassignment
	LEFT JOIN flex f ON f.idassignment = a.idassignment
	WHERE a.isdeleted = '0'
) --21704 rows

, map_stage AS (
	SELECT ac.idassignmentcandidate job_app_ext_id
	, job_type
	, ac.idassignment job_id
	, ac.idperson candidate_id
	, to_char(ac.createdon::DATE, 'YYYY-MM-DD') actioned_date
	, CASE cp.value
		when 'Approach' then 'SHORTLISTED'
		when 'Arrange Interv' then 'FIRST_INTERVIEW'
		when 'Client Interview' then 'FIRST_INTERVIEW'
		when 'CV Benchmark Sent' then 'CANDIDATE'
		when 'CV Received' then 'SHORTLISTED'
		when 'CV Sent' then 'CANDIDATE'
		when 'Follow-Up Conv' then 'SHORTLISTED'
		when 'Hold' then 'SHORTLISTED'
		when 'Interview With SF' then 'FIRST_INTERVIEW'
		when 'Left a Message' then 'SHORTLISTED'
		when 'Longlist' then 'SHORTLISTED'
		when 'Marketing Sent' then 'CANDIDATE'
		when 'Not Interested' then 'SHORTLISTED'
		when 'Not Interested-Intv' then 'FIRST_INTERVIEW'
		when 'Off Limits' then 'CANDIDATE'
		when 'Offer' then 'OFFERED'
		when 'Offer Withdrew' then 'OFFERED'
		when 'Offer/Rej' then 'OFFERED'
		when 'Placed' then 'PLACED'
		when 'Placed Cand Follow' then 'PLACED'
		when 'Reject/I' then 'FIRST_INTERVIEW'
		when 'Reject/S' then 'SECOND_INTERVIEW'
		when 'Rejected' then 'SHORTLISTED'
		when 'Rejected Feedback' then 'CANDIDATE'
		when 'Screening Completed – Not interested' then 'SHORTLISTED'
		when 'Screening Completed – Rejected' then 'SHORTLISTED'
		when 'Screening Completed – Spec Sent' then 'SHORTLISTED'
		when 'Shortlist' then 'SHORTLISTED'
		when 'SL Cons Approval' then 'SHORTLISTED'
		when 'SL Preparation' then 'SHORTLISTED'
		when 'Spec Sent' then 'CANDIDATE'
		when 'Thank You' then 'CANDIDATE'
		when 'To Be Contacted' then 'SHORTLISTED'
		when 'Unreachable' then 'SHORTLISTED'
		when 'Withdrew -S' then 'SHORTLISTED'
		else 'CANDIDATE' END application_stage
	, case when cp.value in ('Not Interested', 'Offer/Rej', 'Offer Withdrew', 'Reject/I', 'Reject/S', 'Rejected', 'Rejected Feedback'
							, 'Screening Completed – Not interested', 'Screening Completed – Rejected', 'Withdrew -S')
						then to_char(ac.createdon::DATE, 'YYYY-MM-DD')
		else NULL end as rejected_date
	, cp.value sub_status
	FROM assignmentcandidate ac
	JOIN (select * from cte_candidate where rn = '1') cc ON ac.idperson = cc.candidate_id
	JOIN (select * from cte_job where rn = '1') cj ON ac.idassignment = cj.job_id
	JOIN (select * from candidateprogress where isactive = '1' ) cp ON ac.idcandidateprogress = cp.idcandidateprogress
	WHERE ac.isexcluded = '0'
)
, cte_application AS (
	SELECT job_app_ext_id
	, job_type
	, job_id
	, candidate_id
	, actioned_date
	, CASE
		WHEN application_stage IN ('PLACED') THEN 'OFFERED'
		ELSE application_stage END application_stage_import
	, application_stage as origin_app_stage
	, rejected_date
	, sub_status
	, ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY CASE application_stage
																WHEN 'PLACED' THEN 1
																WHEN 'OFFERED' THEN 2
																WHEN 'SECOND_INTERVIEW' THEN 3
																WHEN 'FIRST_INTERVIEW' THEN 4
																WHEN 'SENT' THEN 5
																WHEN 'SHORTLISTED' THEN 6
																END ASC, actioned_date DESC) AS rn
	FROM map_stage
	WHERE application_stage IS NOT NULL and application_stage <> 'CANDIDATE'
)

SELECT job_app_ext_id
, case when job_type = 'PERMANENT' then 301
	when job_type = 'CONTRACT' then 302
	end as placed_stage
, job_id "application-positionExternalId"
, candidate_id "application-candidateExternalId"
, application_stage_import "application-stage"
, sub_status
, actioned_date "application-actionedDate"
FROM cte_application
WHERE rn = 1
-- AND real_stage = 'Placed'