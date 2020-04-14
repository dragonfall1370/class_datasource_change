/* BACKUP
select *
into position_description_functional_expertise_bkup_20200304
from position_description_functional_expertise
*/

-->>JOBS WITHOUT SFE
with position_wo_sfe as (select id, position_id, functional_expertise_id, sub_functional_expertise_id
	from position_description_functional_expertise
	where functional_expertise_id is not NULL
	and sub_functional_expertise_id is NULL --6221
	)

, merged_new as (select c.id, c.position_id, c.functional_expertise_id, c.sub_functional_expertise_id
	, m.vcfeid
	--, m.vcsfeid
	, m. vc_new_fe_en
	--, m.vc_new_sfe_split
	from position_wo_sfe c
	join (select distinct vc_fe_id, vcfeid, vc_new_fe_en from mike_tmp_vc_2_vc_new_fe_sfe) m on m.vc_fe_id = c.functional_expertise_id
	) --select * from merged_new --14784

--MAIN SCRIPT
insert into position_description_functional_expertise (position_id, functional_expertise_id)
select position_id
, vcfeid
from merged_new


-->>JOBS WITH SFE
with position_w_sfe as (select id, position_id, functional_expertise_id, sub_functional_expertise_id
	from position_description_functional_expertise
	where functional_expertise_id is not NULL
	and sub_functional_expertise_id is not NULL --357517 rows
	)

, merged_new as (select c.id, c.position_id, c.functional_expertise_id, c.sub_functional_expertise_id
	, m.vcfeid
	, m.vcsfeid
	, m.vc_new_fe_en
	, m.vc_new_sfe_split
	from position_w_sfe c
	join mike_tmp_vc_2_vc_new_fe_sfe m on m.vc_fe_id = c.functional_expertise_id and c.sub_functional_expertise_id = m.sub_functional_expertise_id
	) --select * from merged_new --490018
	
--MAIN SCRIPT	
insert into position_description_functional_expertise (position_id, functional_expertise_id, sub_functional_expertise_id)
select position_id
, vcfeid
, vcsfeid
from merged_new