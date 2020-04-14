with cand_fe_sfe as (select [PANO ] as cand_ext_id
	, [希望職種カテゴリー1] as fe
	, [希望職種1] as sfe
	from csv_can c
	where coalesce(nullif([希望職種カテゴリー1],''), nullif([希望職種1],'')) is not NULL

	UNION ALL
	select [PANO ] as cand_ext_id
	, [希望職種カテゴリー2] as fe
	, [希望職種2] as sfe
	from csv_can c
	where coalesce(nullif([希望職種カテゴリー2],''), nullif([希望職種2],'')) is not NULL

	UNION ALL
	select [PANO ] as cand_ext_id
	, [希望職種カテゴリー3] as fe
	, [希望職種3] as sfe
	from csv_can c
	where coalesce(nullif([希望職種カテゴリー3],''), nullif([希望職種3],'')) is not NULL

	UNION ALL
	select [PANO ] as cand_ext_id
	, [希望職種カテゴリー4] as fe
	, [希望職種4] as sfe
	from csv_can c
	where coalesce(nullif([希望職種カテゴリー4],''), nullif([希望職種4],'')) is not NULL

	UNION ALL
	select [PANO ] as cand_ext_id
	, [希望職種カテゴリー5] as fe
	, [希望職種5] as sfe
	from csv_can c
	where coalesce(nullif([希望職種カテゴリー5],''), nullif([希望職種5],'')) is not NULL

	UNION ALL
	select [PANO ] as cand_ext_id
	, [希望職種カテゴリー6] as fe
	, [希望職種6] as sfe
	from csv_can c
	where coalesce(nullif([希望職種カテゴリー6],''), nullif([希望職種6],'')) is not NULL
)

, cand_final as (select distinct cand_ext_id
	, fe
	, sfe
	from cand_fe_sfe
	where fe is not NULL and fe not in ('＜仮＞') and sfe <> '')

, split_sfe as (select pa_category
	, pa_category_name
	, vc_fe_en
	, vc_fe_ja
	, vc_sfe
	, trim(value) as vc_sfe_split
	from PA_fe_sfe
	cross apply string_split(vc_sfe, char(10)))

, fe_sfe as (select pa_category
	, pa_category_name
	, vc_fe_en
	, vc_fe_ja
	, vc_sfe
	, vc_sfe_split
	, left(vc_sfe_split, charindex('/', vc_sfe_split) - 2) as vc_sfe_en
	, right(vc_sfe_split, len(vc_sfe_split) - charindex('/', vc_sfe_split) - 1) as vc_sfe_ja
	, row_number() over(partition by vc_fe_en order by case when charindex('[P]', vc_sfe_split) = 1 then 1 else 2 end asc, vc_sfe_split asc) as rn
	from split_sfe)


select distinct j.cand_ext_id
, j.fe
, concat('【PP】', vc_fe_en, coalesce(' / ' + nullif(vc_fe_ja,''), NULL)) as vc_fe
, j.sfe
, replace(trim(fe.vc_sfe_split), '[P]', '') as vc_sfe
from cand_final j
left join fe_sfe fe on fe.pa_category = j.fe and fe.pa_category_name = j.sfe
-- rows

/* This case is excluded, not mentioned in mapping
UNION
select distinct j.cand_ext_id
, j.fe
, concat('【PP】', vc_fe_en, coalesce(' / ' + nullif(vc_fe_ja,''), NULL)) as vc_fe
, '' as sfe
, '' as vc_sfe
from cand_final j
left join fe_sfe fe on fe.pa_category = j.fe
where j.sfe = ''
--total: 355760 rows */