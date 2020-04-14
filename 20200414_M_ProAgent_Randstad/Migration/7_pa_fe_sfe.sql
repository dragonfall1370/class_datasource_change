with split_sfe as (select vc_fe_en
	, vc_fe_ja
	, vc_sfe
	, trim(value) as vc_sfe_split
	from PA_fe_sfe
	cross apply string_split(vc_sfe, char(10)))

--MAIN SCRIPT
, fe_sfe as (select vc_fe_en
	, vc_fe_ja
	, vc_sfe
	, vc_sfe_split
	, left(vc_sfe_split, charindex('/', vc_sfe_split) - 2) as vc_sfe_en
	, right(vc_sfe_split, len(vc_sfe_split) - charindex('/', vc_sfe_split) - 1) as vc_sfe_ja
	, row_number() over(partition by vc_fe_en order by case when charindex('[P]', vc_sfe_split) = 1 then 1 else 2 end asc, vc_sfe_split asc) as rn
	from split_sfe)

--FILTER SCRIPT | FE
select distinct concat('【PP】', vc_fe_en, ' / ', vc_fe_ja) as vc_fe
, vc_sfe_split
, replace(trim(vc_sfe_split), '[P]', '') as vc_sfe_en
, current_timestamp as insert_timestamp
from fe_sfe --150 rows
--order by fe, rn