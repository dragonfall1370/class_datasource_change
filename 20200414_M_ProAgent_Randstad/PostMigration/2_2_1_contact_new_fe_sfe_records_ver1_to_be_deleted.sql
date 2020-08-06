--Contact FE/SFE in conversion 1 to be changed
with vc_fe_sfe as (select sfe.functional_expertise_id as feid
	, fe.name as fe
	, sfe.id as sfeid
	, sfe.name as sfe
	from sub_functional_expertise sfe
	left join functional_expertise fe on fe.id = sfe.functional_expertise_id
	order by fe.id, sfe.name
)


--Contact FE/SFE from backup 20200705
, contact_fe_sfe_bkup as (select distinct contact_id
	from mike_tmp_contact_functional_expertise_20200705
	where 1=1
	and functional_expertise_id in (3004, 3001)
	and sub_functional_expertise_id in (330, 250, 549, 639, 435, 254, 553, 523, 524, 582, 516, 592, 420, 604, 425, 606, 529, 422, 426, 607, 608, 427, 603, 255, 256, 580, 528, 423, 579, 429, 584, 430, 525, 431, 438)
	) --select * from contact_fe_sfe_bkup --67 records


--Capture all contacts from 1st conversion
select distinct cfe.contact_id
, c.first_name
, c.last_name
, c.phone
, c.email
, cfe.functional_expertise_id
, overlay(v.fe placing '' from 1 for length('【PP】')) as fe
, cfe.sub_functional_expertise_id
, v.sfe
from contact_functional_expertise cfe
join contact c on c.id = cfe.contact_id
left join vc_fe_sfe v on concat_ws('', v.feid, v.sfeid) = concat_ws('', cfe.functional_expertise_id, cfe.sub_functional_expertise_id)
where cfe.functional_expertise_id in (3044, 3046, 3047, 3051, 3052, 3055, 3058, 3059) --8 FEs in conversion 1
and cfe.sub_functional_expertise_id in (657, 685, 686, 695, 755, 756, 757, 758, 759, 760, 763, 764, 766, 790, 799, 805, 807, 822, 823, 824, 825, 827, 855, 858, 861, 866, 869, 872, 906)
and c.id in (select contact_id from contact_fe_sfe_bkup) --67 rows
order by cfe.contact_id -- unique contact IDs