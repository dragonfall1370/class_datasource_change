---Sample case
select pd.id as Job
, pd.external_id as JobExt
, right(pd.external_id,length(pd.external_id)-3) as AFRJobID
, c.id as Cand
, c.external_id as CandExt
, right(c.external_id,length(c.external_id)-3) as AFRJobID
, pc.* 
from position_candidate pc
left join position_description pd on pd.id = pc.position_description_id
left join candidate c on c.id = pc.candidate_id
where pc.id = 254227

---Full list
select pd.id as Job
, pd.external_id as JobExt
, right(pd.external_id,length(pd.external_id)-3) as AFRJobID
, c.id as Cand
, c.external_id as CandExt
, right(c.external_id,length(c.external_id)-3) as AFRJobID
, pc.*
from position_candidate pc
left join position_description pd on pd.id = pc.position_description_id
left join candidate c on c.id = pc.candidate_id
where pc.status = 200
and pc.id not in (select position_candidate_id from offer)