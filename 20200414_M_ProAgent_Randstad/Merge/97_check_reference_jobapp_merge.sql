select *
from mike_tmp_candidate_dup_check
where vc_pa_candidate_id = 156924

---
with appfilter as (select pc.id, pc.candidate_id, m.vc_candidate_id, pc.position_description_id, pc.status
	from position_candidate pc
	join mike_tmp_candidate_dup_check m on m.vc_pa_candidate_id = pc.candidate_id --job app links with dup candidate from PA
	--where exists (select 1 from mike_tmp_candidate_dup_check where m.vc_pa_candidate_id = pc.candidate_id)
	--and pc.status > 300
	)

--Check all job apps
, jobapp as (select a.id, a.vc_candidate_id, a.position_description_id, a.status, pd.contact_id, pd.company_id
from appfilter a
left join position_description pd on pd.id = a.position_description_id
where 1=1
--and pc.candidate_id in (select vc_candidate_id from appfilter) --general conditions
--and vc_candidate_id = 98652

UNION ALL

select pc.id, pc.candidate_id, pc.position_description_id, pc.status, pd.contact_id, pd.company_id
from position_candidate pc
left join position_description pd on pd.id = pc.position_description_id
where 1=1
--and pc.candidate_id in (select vc_candidate_id from appfilter) --general conditions
--and candidate_id = 98652
--and status = 301
)

/* --AUDIT
select vc_candidate_id, count(*)
from jobapp
where status >=300
group by vc_candidate_id
having count(*) > 1
order by vc_candidate_id 
*/

select *
from jobapp
where status >=300
--and vc_candidate_id in (40910, 40988)
order by vc_candidate_id