--EXECUTE VERSION
with interim as (
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where assignmenttitle ilike '%interim%' --408 rows
	
	UNION
	select idassignment, assignmenttitle, idcompany
	from "assignment"
	where idcompany in ('826df702-f17e-4939-9566-75dc74e3b21b', 'd6d459aa-4e5e-4771-a0a4-1b99fce610a4')
) --409 rows

select distinct idassignment job_ext_id
, idperson cand_ext_id
from assignmentcandidate
where idassignment in (select idassignment from interim)


/* FILTER CANDIDATE
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

, map_stage AS (
	SELECT ac.idassignmentcandidate job_app_ext_id
	, ac.idassignment job_id
	, im.assignmenttitle
	, ac.idperson candidate_id
	, to_char(ac.createdon::DATE, 'YYYY-MM-DD') actioned_date
	, cp.value sub_status
	FROM assignmentcandidate ac
	JOIN (select * from cte_candidate where rn = '1') cc ON ac.idperson = cc.candidate_id
	JOIN (select * from interim) im ON ac.idassignment = im.idassignment
	JOIN (select * from candidateprogress where isactive = '1' ) cp ON ac.idcandidateprogress = cp.idcandidateprogress
	WHERE ac.isexcluded = '0'
)
, cte_application AS (
	SELECT job_app_ext_id
	, job_id
	, assignmenttitle
	, candidate_id
	, actioned_date
	, sub_status
	, ROW_NUMBER() OVER(PARTITION BY job_id, candidate_id ORDER BY actioned_date DESC) AS rn
	FROM map_stage
)

SELECT job_app_ext_id
, job_id job_ext_id --decide talent pool
, candidate_id cand_ext_id
, assignmenttitle
, sub_status
, actioned_date
FROM cte_application
WHERE rn = 1 */