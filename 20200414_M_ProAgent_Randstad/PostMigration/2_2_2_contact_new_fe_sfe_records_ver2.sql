--DRAFT CONVERSION 2
select cfe.contact_id
, c.first_name
, c.last_name
, c.phone
, c.email
, functional_expertise_id
, m.vc_fe_name
, sub_functional_expertise_id
, m.vc_sfe_name
, m.vcfeid
, m.vc_new_fe
, m.vcsfeid
, m.vc_new_sfe_split
from mike_tmp_contact_functional_expertise_20200705 cfe
left join mike_tmp_vc_2_vc_new_fe_sfe_v2 m on concat_ws('', m.vc_fe_id, m.vc_sfe_id) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
join contact c on c.id = cfe.contact_id
where 1=1
and cfe.functional_expertise_id in (3004, 3001)
and cfe.sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
