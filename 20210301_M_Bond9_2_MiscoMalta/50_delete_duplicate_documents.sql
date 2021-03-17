--DELETE FROM VC DATABASE IF MULTIPLE DUPLICATE DOCUMENTS
with cand_doc as (select id, candidate_id, uploaded_filename, created, primary_document
, row_number() over(partition by candidate_id, uploaded_filename order by primary_document desc, created, id) as rn
from candidate_document
where 1=1
--and candidate_id = 66980
and candidate_id > 0
)
				  
delete from candidate_document
where id in (select id from cand_doc where rn > 1)