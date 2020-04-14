--In VC, industry without parentid, sub industry with parentid
select vc_ind_id
, vc_ind_name
, concat_ws('', '【PP】', vc_new_ind_en, coalesce(' / ' + nullif(vc_new_ind_ja,''), NULL)) as vc_new_ind
, vc_new_ind_en
, vc_new_ind_ja
, vc_new_sub_ind
, replace(trim(value), '[P]', '') as vc_new_sfe_split
from vc_2_vc_new_ind
cross apply string_split(vc_new_sub_ind, char(10))