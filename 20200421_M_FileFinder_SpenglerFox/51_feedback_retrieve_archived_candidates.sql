/*
--Requirements changed on 2020/03/26
1. All candidates linked to the Interim Jobs that are active now to be Un - Archived.
2. Find out why the number of candidates linked to the Interim jobs are different than the attached aggregated count and recover these candidates and link them back to the interim jobs.
3. All Interim Jobs should be linked to "Fake Interim Company" under "Jan" contact. So we may need to create a dummy contact under "Jan" name on the same email address but link this contact to "Fake Interim Company". Then re-assign all Interim Jobs.
*/

with migrated_interim as (select idassignment
	from "assignment"
	where assignmentno::int in (2001656,2001606,2001386,2001365,2001337,2001331,2001330,2001190,2001138,2001076,2001030,2000998,2000556,2000555,2000486,1008882,1008881,1008860,1008862,1008869,1008857,1008871,1008867,1008859,1008868,1008861,1008864,1008856,1008866)
)

select distinct ac.idperson
, ac.idassignment
, 'SHORTLISTED' as app_stage
--, ac.idcandidateprogress
--, cp.value
from assignmentcandidate ac
join migrated_interim m on m.idassignment = ac.idassignment
join personx p on p.idperson = ac.idperson
--join (select * from candidateprogress where isactive = '1' ) cp ON ac.idcandidateprogress = cp.idcandidateprogress
where 1=1
and p.isdeleted = '0' --576 candidates
--and ac.idcandidateprogress is NULL