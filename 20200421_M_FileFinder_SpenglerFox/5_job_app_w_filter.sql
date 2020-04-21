WITH cte_candidate AS (SELECT
	c.idperson candidate_id,
	ROW_NUMBER() OVER(PARTITION BY c.idperson ORDER BY px.createdon DESC) rn
	FROM candidate c
	JOIN (select * from personx where isdeleted = '0') px ON c.idperson = px.idperson
	JOIN (select * from person where isdeleted = '0') p ON c.idperson = p.idperson
) --586657 rows

, interim as (
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where assignmenttitle ilike '%interim%' --408 rows
	
	UNION
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where idcompany in ('826df702-f17e-4939-9566-75dc74e3b21b', 'd6d459aa-4e5e-4771-a0a4-1b99fce610a4')
) --409 rows

, jobleads as (select idassignment
	from "assignment" a
	where a.assignmentno in ('1004879','1007680','1008886','1011960','1013354','2001160','2001522','2001595','2001616','2001645','2001646','2001647') --jobs migrated
	or a.assignmentno in ('2000501','2000876','2000906','2000909','2001060','2001066','2001104','2001122','2001124','2001150','2001158','2001177','2001178','2001179','2001188','2001197','2001225','2001227','2001228','2001231','2001235','2001238','2001239','2001265','2001266','2001276','2001286','2001292','2001306','2001328','2001334','2001335','2001340','2001349','2001353','2001359','2001387','2001392','2001393','2001397','2001398','2001400','2001407','2001421','2001423','2001424','2001443','2001446','2001469','2001484','2001485','2001513','2001518','2001521','2001533','2001536','2001537','2001538','2001549','2001553','2001558','2001565','2001567','2001572','2001583','2001593','2001593','2001596','2001602','2001613','2001618','2001623','2001625','2001628','2001631','2001632','2001634','2001639','2001640','2001648','2001655','2001669','2001670','2001671','1013982','1013999','2000700','2000923','2000924','2000960','2001064','2001102','2001103','2001109','2001112','2001154','2001157','2001175','2001221','2001226','2001229','2001230','2001232','2001233','2001234','2001236','2001237','2001240','2001241','2001242','2001243','2001244','2001245','2001248','2001249','2001350','2001364','2001373','2001380','2001390','2001412','2001419','2001422','2001427','2001434','2001460','2001473','2001494','2001496','2001547','2001548','2001550','2001574','2001585','2001591','2001624','2001627','2001630','2001633','2001642','2001650','2001651','2001653','2001659','2001660','2001661','2001662') --jobs leads
	)
	
, selected_assignment as 
	(select idassignment
	from interim
	
	UNION
	select idassignment
	from jobleads)

, cte_job AS (
	SELECT a.idassignment job_id,
	CASE
		WHEN f.idassignment IS NULL THEN 'PERMANENT'
		ELSE 'CONTRACT' END job_type
	FROM selected_assignment a
	LEFT JOIN flex f ON f.idassignment = a.idassignment
) --select * from cte_job --566 rows

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
	JOIN (select * from cte_job) cj ON ac.idassignment = cj.job_id
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