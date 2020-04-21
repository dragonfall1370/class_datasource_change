--UPDATE WITH NEW DEFAULT CONTACT FOR ALL INTERIM JOBS
select id, company_id, contact_id
from position_description
where name ilike '%interim%'

select *
from company
where id = 102994

select *
from contact
where company_id = 102994 --595655 contact_id

update position_description
set company_id = 102994, contact_id = 595655
where name ilike '%interim%'


--JOB APP FOR ALL UN-ARCHIVED CANDIDATES
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


--UDPATE TO UN-ARCHIVE ARCHIVED CANDIDATES
/* AUDIT
with job as (select id, position_description_id, candidate_id
from position_candidate
where position_description_id in (select id from position_description where name ilike '%interim%')
) --576 rows

select id, insert_timestamp, deleted_timestamp
from candidate
where id in (select candidate_id from job)
and deleted_timestamp is not NULL --81 rows
*/

with job as (select id, position_description_id, candidate_id
from position_candidate
where position_description_id in (select id from position_description where name ilike '%interim%')
) --576 rows

update candidate
set deleted_timestamp = NULL
where id in (select distinct candidate_id from job)
and deleted_timestamp is not NULL --update 81 rows