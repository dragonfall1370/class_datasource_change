--CHECK BEFORE EXECUTION
select *
from candidate_functional_expertise

select max(id), count(id)
from candidate_functional_expertise

--CANDIDATE FE/SFE ALL
with candidate_w_sfe as (select id, candidate_id, functional_expertise_id, sub_functional_expertise_id
	from candidate_functional_expertise
	where functional_expertise_id is not NULL
	and sub_functional_expertise_id is not NULL --31329 rows
	)

, merged_new as (select c.id, c.candidate_id, c.functional_expertise_id, c.sub_functional_expertise_id
	, m.vcfeid
	, m.vcsfeid
	, m.vc_new_fe_en
	, m.vc_new_sfe_split
	from candidate_w_sfe c
	join mike_tmp_vc_2_vc_new_fe_sfe m on m.vc_fe_id = c.functional_expertise_id and m.vc_sfe_id = c.sub_functional_expertise_id
	) --select * from merged_new --31791
	
--MAIN SCRIPT	
insert into candidate_functional_expertise (candidate_id, functional_expertise_id, sub_functional_expertise_id)
select candidate_id
, vcfeid
, vcsfeid
from merged_new