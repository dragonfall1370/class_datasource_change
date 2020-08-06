with appfilter as (select pc.id, pc.candidate_id, m.vc_candidate_id, pc.position_description_id, pc.status
	from position_candidate pc
	join mike_tmp_candidate_dup_check m on m.vc_pa_candidate_id = pc.candidate_id
	--where exists (select 1 from mike_tmp_candidate_dup_check where m.vc_pa_candidate_id = pc.candidate_id)
	and pc.status > 300
	)
	
, jobapp as (select id, vc_candidate_id, position_description_id, status
from appfilter
where 1=1
--and pc.candidate_id in (select vc_candidate_id from appfilter) --general conditions
--and vc_candidate_id = 98652

UNION ALL

select id, candidate_id, position_description_id, status
from position_candidate
where 1=1
--and pc.candidate_id in (select vc_candidate_id from appfilter) --general conditions
--and candidate_id = 98652
and status >= 300)

/* --AUDIT
select vc_candidate_id, count(*)
from jobapp
group by vc_candidate_id
having count(*) > 1
order by vc_candidate_id 
*/

select *
from jobapp
where vc_candidate_id = 143675