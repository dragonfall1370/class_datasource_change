--FE/SFE having more than 1 conversion 
with fesfe_group as (select vcfeid, vcsfeid, count(*) as counts
	from mike_tmp_vc_2_vc_new_fe_sfe_v1_to_v2
	where 1=1
	group by vcfeid, vcsfeid
	having count(*) > 1)
	
select *
from mike_tmp_vc_2_vc_new_fe_sfe_v1_to_v2
where concat_ws('', vcfeid, vcsfeid) in (select concat_ws('', vcfeid, vcsfeid) from fesfe_group)
order by vcfeid, vcsfeid


-->> FROM BACK UP <<--
---CONTACT FE/SFE BACKUP
select c.contact_id
, c.functional_expertise_id
, fe.name as fe
, c.sub_functional_expertise_id
, sfe.name as sfe
--to be converted
, m.vcfeid
, m.vc_new_fe_en
, m.vcsfeid
, m.vc_new_sfe_split
from contact_functional_expertise c
left join sub_functional_expertise sfe on sfe.id = c.sub_functional_expertise_id
left join functional_expertise fe on fe.id = sfe.functional_expertise_id
left join mike_tmp_vc_2_vc_new_fe_sfe m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', c.functional_expertise_id, c.sub_functional_expertise_id)
where contact_id = 20469


select *
from contact_functional_expertise
where 1=1
and functional_expertise_id in (3004, 3001)
and sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438) --70


--
select *
from mike_tmp_vc_2_vc_new_fe_sfe
where 1=1
and vc_fe_id in (3004, 3001)
and vc_sfe_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
order by vc_fe_id, vc_sfe_id


-->> FROM VC <<--
--CONACT FE/SFE AFTER CONVERSION
select c.contact_id
, c.functional_expertise_id
, fe.name as fe
, c.sub_functional_expertise_id
, sfe.name as sfe
--, m.vcfeid2
--, m.vc_new_fe2
--, m.vcsfeid2
--, m.vc_new_sfe_split2
from contact_functional_expertise c
left join sub_functional_expertise sfe on sfe.id = c.sub_functional_expertise_id
left join functional_expertise fe on fe.id = c.functional_expertise_id
left join mike_tmp_vc_2_vc_new_fe_sfe_v1_to_v2 m on concat_ws('', m.vcfeid, m.vcsfeid) = concat_ws('', c.functional_expertise_id, c.sub_functional_expertise_id)
where contact_id = 20469