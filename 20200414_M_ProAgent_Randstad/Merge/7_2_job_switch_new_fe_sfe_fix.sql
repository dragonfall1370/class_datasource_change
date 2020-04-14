--FIX MERGED FE / SFE
with position_w_sfe as (select id, position_id, functional_expertise_id, sub_functional_expertise_id
	from position_description_functional_expertise
	where functional_expertise_id is not NULL
	and sub_functional_expertise_id is not NULL --357517 rows
	and id < 384669 --357516 rows
	) --select * from position_w_sfe where position_id = 63858

, merged_new as (select c.id, c.position_id, c.functional_expertise_id, c.sub_functional_expertise_id
	, m.vcfeid
	, m.vcsfeid
	, m.vc_new_fe_en
	, m.vc_new_sfe_split
	from position_w_sfe c
	left join mike_tmp_vc_2_vc_new_fe_sfe m on m.vc_fe_id = c.functional_expertise_id and m.vc_sfe_id = c.sub_functional_expertise_id
	--where position_id = 63858
	) --select * from merged_new where position_id = 63858 --357669 | id sample = 36380
	
--MAIN SCRIPT	
insert into position_description_functional_expertise (position_id, functional_expertise_id, sub_functional_expertise_id)
select position_id
, vcfeid
, vcsfeid
from merged_new	


/* >> CHECK REFERENCE
, current_job_fe_sfe as (select *
	from position_description_functional_expertise
	where 1=1
	and id between 384670 and 889474 --504802 --migrated for merged job fe_sfe
	--and id <= 384669
	--order by id desc
	)
	
, removed_job_fe as (select cj.id, cj.position_id
from current_job_fe_sfe cj
left join merged_new m on m.position_id = cj.position_id
where 1=1
and cj.functional_expertise_id = m.vcfeid
and cj.sub_functional_expertise_id = m.vcsfeid
--24 rows to be removed
) select * from removed_job_fe
*/
/*
--ADD COLUMN TO MARK CORRECT MERGED FE/SFE
alter table position_description_functional_expertise
add column merged_fe_sfe_ok int

update position_description_functional_expertise p
set merged_fe_sfe_ok = 1
from removed_job_fe r
where r.id = p.id
and p.id between 384670 and 889474
*/
/* DOUBLE CHECK
select *
from position_description_functional_expertise
where id in (select id from removed_job_fe)
*/

/* AUDIT CHECK */
with job_fe_sfe as (select *
from position_description_functional_expertise
where 1=1
--and (functional_expertise_id is not NULL and sub_functional_expertise_id is not NULL) --375516
and id < 384669 --368309
)

select *
from job_fe_sfe
where functional_expertise_id is not NULL and sub_functional_expertise_id is not NULL

--
select * from merged_new
where position_id = 63858 --3049 | 820

select *
from position_description_functional_expertise
where 1=1
and position_id = 63858
--and created_date is not NULL
order by id desc

select count(*)
from position_description_functional_expertise
where id > 384669 --504873

select max(id) from position_description_functional_expertise --889703
	
select *
from position_description_functional_expertise
where 1=1
and id > 889474 --71