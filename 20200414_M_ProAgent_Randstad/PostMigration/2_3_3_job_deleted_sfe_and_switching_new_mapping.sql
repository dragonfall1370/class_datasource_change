--#STEP 1: Delete job SFE records if SFE deleted
--Backup job FE/SFE
select *
into mike_bkup_position_description_functional_expertise_20200804
from position_description_functional_expertise


--Delete empty FE/SFE
select *
from position_description_functional_expertise
where functional_expertise_id is NULL and sub_functional_expertise_id is NULL


delete from position_description_functional_expertise
where functional_expertise_id is NULL and sub_functional_expertise_id is NULL


--Check to-be-deleted records
select cfe.position_id
, cfe.functional_expertise_id
, cfe.sub_functional_expertise_id
from position_description_functional_expertise cfe
where cfe.functional_expertise_id in (3046, 3051, 3055)
and cfe.sub_functional_expertise_id in (698, 756, 757, 758, 764, 822, 823, 824, 825)


--Deleted SFEs
delete from position_description_functional_expertise
where functional_expertise_id in (3046, 3051, 3055)
and sub_functional_expertise_id in (698, 756, 757, 758, 764, 822, 823, 824, 825) -- rows


--#STEP 2: Delete job FE/SFE records if mapping changed
with job_fe_sfe_bkup as (select distinct position_id
	from mike_tmp_position_description_functional_expertise_20200705
	where 1=1
	and functional_expertise_id in (3004, 3001)
	and sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
	) --select * from job_fe_sfe_bkup --2023 rows

	
delete from position_description_functional_expertise
where functional_expertise_id in (3044, 3046, 3047, 3051, 3052, 3055, 3058, 3059) --8 FEs in conversion 1
and sub_functional_expertise_id in (657, 685, 686, 695, 755, 756, 757, 758, 759, 760, 763, 764, 766, 790, 799, 805, 807, 822, 823, 824, 825, 827, 855, 858, 861, 866, 869, 872, 906)
and position_id in (select position_id from job_fe_sfe_bkup) --1788 rows


--#STEP 3: Insert new mapping from backup on 05/07/2020 - before original FE/SFE to 1st conversion mapping
---Check 2nd conversion before insert
select cfe.position_id
, functional_expertise_id
, m.vc_fe_name
, sub_functional_expertise_id
, m.vc_sfe_name
, m.vcfeid
, m.vc_new_fe
, m.vcsfeid
, m.vc_new_sfe_split
from mike_tmp_position_description_functional_expertise_20200705 cfe
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
join (select id from position_description) c on c.id = cfe.position_id
where 1=1
and cfe.functional_expertise_id in (3004, 3001)
and cfe.sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)

/*
select max(id), count(id)
from position_description_functional_expertise
*/

--Insert into position_description_functional_expertise
insert into position_description_functional_expertise (position_id, functional_expertise_id, sub_functional_expertise_id, created_date)
select distinct cfe.position_id
, m.vcfeid functional_expertise_id
, m.vcsfeid sub_functional_expertise_id
, current_timestamp as created_date
from mike_tmp_position_description_functional_expertise_20200705 cfe
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
join (select id from position_description) c on c.id = cfe.position_id
where 1=1
and cfe.functional_expertise_id in (3004, 3001)
and cfe.sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)


--AUDIT JOBS
