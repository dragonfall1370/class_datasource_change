--Job FE/SFE in conversion 1 to be changed
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)

--Job FE/SFE from backup 20200705
, job_fe_sfe_bkup as (select distinct position_id
	from mike_tmp_position_description_functional_expertise_20200705
	where 1=1
	--and position_id = 40135 --audit case
	and functional_expertise_id in (3004, 3001)
	and sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
	)

--Capture all jobs from 1st conversion
select distinct pfe.position_id
, pd.name
, pfe.functional_expertise_id
, overlay(v.fe placing '' from 1 for length('【PP】')) as fe
, pfe.sub_functional_expertise_id
, v.sfe
from position_description_functional_expertise pfe
join position_description pd on pd.id = pfe.position_id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', pfe.functional_expertise_id, pfe.sub_functional_expertise_id)
where pfe.functional_expertise_id in (3044, 3046, 3047, 3051, 3052, 3055, 3058, 3059) --8 FEs in conversion 1
and pfe.sub_functional_expertise_id in (657, 685, 686, 695, 755, 756, 757, 758, 759, 760, 763, 764, 766, 790, 799, 805, 807, 822, 823, 824, 825, 827, 855, 858, 861, 866, 869, 872, 906)
and pd.id in (select position_id from job_fe_sfe_bkup) --2108 rows
--and (pd.external_id not ilike 'JOB%' or pd.external_id is NULL) --4251 rows
order by pfe.position_id --2013 unique job IDs



---REFRERENCE AND REVERSED CHECK
select *
from mike_tmp_vc_2_vc_new_fe_sfe_v1_to_v2

select *
from position_description_functional_expertise
where position_id = 44004

--reversed check
select *
from mike_tmp_vc_2_vc_new_fe_sfe_v1_to_v2
where 1=1
and vcfeid = 3052 and vcsfeid = 805 --conversion 1
and vc_fe_id = 3001 and vc_sfe_id = 594 --orginal





