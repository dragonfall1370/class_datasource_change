--Candidate document primary
/* TEMP TABLE
select distinct *
into f01docs1_edited
from f01docs1
*/

with person_doc as (select uniqueid
, replace(right("relative document path", position(E'\\' in reverse("relative document path")) - 1), 'txt', 'rtf') as cand_doc
, description
, to_date(date, 'DD/MM/YY') created
, row_number() over(partition by uniqueid, right("relative document path", position(E'\\' in reverse("relative document path")) - 1) order by to_date(date, 'DD/MM/YY') desc) as rn
from f01docs1_edited)


select uniqueid cand_ext_id
, cand_doc
, description as note
, created
, case when rn = 1 then 1 else 0 end as primary_document
from person_doc


/* UPDATE CANDIDATE DOCUMENTS
select *
--into mike_candidate_document_bkup_20201201
from candidate_document

select id, primary_document
from candidate_document
where candidate_id > 0 --65236 

update candidate_document
set primary_document = 0
where candidate_id > 0

*/