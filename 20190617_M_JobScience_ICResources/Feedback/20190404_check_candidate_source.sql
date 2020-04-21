--Check IC candidate source
select candidate.id
, external_id
, first_name
, last_name
, email
, candidate_source_id
, cs.name
from candidate 
left join candidate_source cs on cs.id = candidate.candidate_source_id
where candidate_source_id is not NULL
and external_id is not NULL
and deleted_timestamp is NULL
and cs.name = 'Data Import'
order by candidate.id