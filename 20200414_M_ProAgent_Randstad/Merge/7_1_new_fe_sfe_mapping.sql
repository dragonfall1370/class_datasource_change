CREATE TABLE mike_tmp_vc_2_vc_new_fe_sfe 
(
	vc_fe_id bigint, 
	vc_fe_name character varying (1000), 
	vc_sfe_id bigint, 
	vc_sfe_name character varying (1000), 
	VCFEID bigint, 
	vc_new_fe_en character varying (1000), 
	vc_new_fe_ja character varying (1000), 
	VCSFEID bigint, 
	vc_new_sfe_split character varying (1000)
)


--Running from spoon | Temp table [vc_2_vc_new_fe_sfe]
select vc_fe_id
, vc_fe_name
, vc_sfe_id
, vc_sfe_name
, concat_ws('', '【PP】', vc_new_fe_en, coalesce(' / ' + nullif(vc_new_fe_ja,''), NULL)) as vc_new_fe_en
, vc_new_fe_ja
, note
, replace(trim(value), '[P]', '') as vc_new_sfe_split
--, 3043 id_filter
from vc_2_vc_new_fe_sfe
cross apply string_split(vc_new_sfe, char(10))