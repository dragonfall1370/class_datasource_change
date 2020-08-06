with appfilter as (select pc.id, pc.candidate_id, m.vc_candidate_id, pc.position_description_id, pc.status
	from position_candidate pc
	join mike_tmp_candidate_dup_check m on m.vc_pa_candidate_id = pc.candidate_id --job app links with dup candidate from PA
	--where exists (select 1 from mike_tmp_candidate_dup_check where m.vc_pa_candidate_id = pc.candidate_id)
	--and pc.status < 200 --offered stage
	)

--JOB APP TO MERGED
select *
--into mike_jobapp_tobemerged
from appfilter

/*
--AUDIT SCRIPT
select id
, candidate_id
, vc_candidate_id
, position_description_id
, status
from appfilter
where 1=1
--and status >=300
--and vc_candidate_id in (156924, 98652)
and candidate_id in (156924)
order by vc_candidate_id

--BACKUP CURRENT CANDIDATE ID IN JOB APP
alter table position_candidate
add column candidate_id_bkup bigint

update position_candidate
set candidate_id_bkup = candidate_id --846666
*/

--MAIN SCRIPT TO UPDATE
update position_candidate pc
set candidate_id = af.vc_candidate_id
from appfilter af --(temp table: mike_jobapp_tobemerged)
where pc.id = af.id
and pc.candidate_id in (156924) --new candidate from ProAgent | merged to 98652