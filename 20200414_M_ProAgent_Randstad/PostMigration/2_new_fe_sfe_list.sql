/* IMPORT CSV FILES FOR NEW FE/SFE

E:\DataMigration\Randstad\Working\PA_Mapping\FinalIndustry_FESFE20200727_vc_2_vc_new_fe_sfe_v2.csv

*/

--LIST OF NEW VC-VC FE/SFE
---Running from spoon | Temp table [vc_2_vc_new_fe_sfe_v2]
select vc_fe_id
, vc_fe_name
, vc_sfe_id
, vc_sfe_name
, concat_ws('', '【PP】', replace(vc_new_fe, char(10), ' / ')) as vc_new_fe_en
, note
, replace(trim(value), '[P]', '') as vc_new_sfe_split
--, 3043 id_filter
from vc_2_vc_new_fe_sfe_v2
cross apply string_split(vc_new_sfe, char(10))


/* TEMP TABLE FOR NEW MAPPING ON RANDSTAD

CREATE TABLE mike_tmp_vc_2_vc_new_fe_sfe_v2
(
	vc_fe_id bigint, 
	vc_fe_name character varying (1000), 
	vc_sfe_id bigint, 
	vc_sfe_name character varying (1000), 
	vcfeid bigint, 
	vc_new_fe character varying (1000), 
	vcsfeid bigint, 
	vc_new_sfe_split character varying (1000)
)

*/


--LIST OF TOBEDELETED VC-VC SFE
--To be deleted SFE | 9 records
select *
from sub_functional_expertise
where 1=1
and functional_expertise_id in (3046, 3051, 3055)
and id in (698, 756, 757, 758, 764, 822, 823, 824, 825)
order by functional_expertise_id, name


--To be changed FE/SFE | 35 records
select *
from mike_tmp_vc_2_vc_new_fe_sfe_v2
where 1=1
and vc_fe_id in (3004, 3001)
and vc_sfe_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
order by vc_fe_id, vc_sfe_id


--MAPPING CHANGED FROM VER1 to VER2
select m.vc_fe_id
, m.vc_fe_name
, m.vc_sfe_id
, m.vc_sfe_name
, m.vcfeid
, m.vc_new_fe_en
, m.vcsfeid
, m.vc_new_sfe_split
, m2.vcfeid as vcfeid2
, m2.vc_new_fe as vc_new_fe2
, m2.vcsfeid as vcsfeid2
, m2.vc_new_sfe_split as vc_new_sfe_split2
--into mike_tmp_vc_2_vc_new_fe_sfe_v1_to_v2 --35 rows
from mike_tmp_vc_2_vc_new_fe_sfe m
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m2 on concat_ws('', m2.vc_fe_id, m2.vc_sfe_id) = concat_ws('', m.vc_fe_id, m.vc_sfe_id)
where 1=1
and m.vc_fe_id in (3004, 3001)
and m.vc_sfe_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
order by m.vc_fe_id, m.vc_sfe_id


--NEW CONVERSION - DOUBLE CHECK
select vc_fe_id
, vc_fe_name
, vc_sfe_id
, vc_sfe_name
, vcfeid
, overlay(vc_new_fe_en placing '' from 1 for length('【PP】')) as vc_new_fe
, vcsfeid
, vc_new_sfe_split as vc_new_sfe
, vcfeid2
, overlay(vc_new_fe2 placing '' from 1 for length('【PP】')) as vc_new_fe_2
, vcsfeid2
, vc_new_sfe_split2 as vc_new_sfe_2
from mike_tmp_vc_2_vc_new_fe_sfe_v1_to_v2