--DRAFT CONVERSION 2
select position_id
, functional_expertise_id
, m.vc_fe_name
, sub_functional_expertise_id
, m.vc_sfe_name
, m.vcfeid
, m.vc_new_fe
, m.vcsfeid
, m.vc_new_sfe_split
from mike_tmp_position_description_functional_expertise_20200705 pfe
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', pfe.functional_expertise_id, pfe.sub_functional_expertise_id)
join position_description pd on pd.id = pfe.position_id
where 1=1
--and position_id = 40135 --audit case
and functional_expertise_id in (3004, 3001)
and sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
	

--JOBS ALREADY DELETED
select pfe.position_id
from mike_tmp_position_description_functional_expertise_20200705 pfe
left join position_description pd on pd.id = pfe.position_id
where 1=1
and functional_expertise_id in (3004, 3001)
and sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
and position_id not in (select id from position_description)