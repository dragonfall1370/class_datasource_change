with industry as (select pa_name
		, industry_en
		, industry_ja
		, row_number() over (partition by pa_name order by industry_en asc) as rn
	from PA_industry)

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

/* AUDIT INDUSTRY / FE / SFE LIST

select [PANO ] as cand_ext_id
	--, '勤務歴 会社名' as currentEmployer
	--, '勤務歴 役職名' as jobTitle
	, [勤務歴 業種1] as origin_industry
	, [勤務歴 職種カテゴリー1] as origin_fe
	, [勤務歴 職種1] as origin_sfe1
	, '' as dateRangeFrom
	, '' as dateRangeTo
	, '' as experience
from csv_can c
where 1=1
and [PANO ] in ('CDT223957', 'CDT223540')

UNION
select [PANO ] as cand_ext_id
	--, '勤務歴 会社名' as currentEmployer
	--, '勤務歴 役職名' as jobTitle
	, [勤務歴 業種2] as origin_industry
	, [勤務歴 職種カテゴリー2] as origin_fe
	, [勤務歴 職種2] as origin_sfe1
	, '' as dateRangeFrom
	, '' as dateRangeTo
	, '' as experience
from csv_can c
where 1=1
and [PANO ] in ('CDT223957', 'CDT223540')

UNION
select [PANO ] as cand_ext_id
	--, '勤務歴 会社名' as currentEmployer
	--, '勤務歴 役職名' as jobTitle
	, [勤務歴 業種3] as origin_industry
	, [勤務歴 職種カテゴリー3] as origin_fe
	, [勤務歴 職種3] as origin_sfe1
	, '' as dateRangeFrom
	, '' as dateRangeTo
	, '' as experience
from csv_can c
where 1=1
and [PANO ] in ('CDT223957', 'CDT223540')

*/

, dummy_wh as (
--Industry / FE / SFE set 1
	select [PANO ] as cand_ext_id
	, '勤務歴 会社名1' as currentEmployer
	, '勤務歴 役職名1' as jobTitle
	, [勤務歴 業種1] as origin_industry
	, i.industry_en as industry
	, [勤務歴 職種カテゴリー1] as origin_fe
	, concat('【PP】', vc_fe_en, coalesce(' / ' + nullif(vc_fe_ja,''), NULL)) as vc_fe
	, [勤務歴 職種1] as origin_sfe1
	, replace(trim(fe.vc_sfe_split), '[P]', '') as vc_sfe
	, '' as dateRangeFrom
	, '' as dateRangeTo
	, concat_ws('<br/>'
		, coalesce('【業種1】' + '<br/>' + [勤務歴 業種1], NULL)
		, coalesce('【職種カテゴリー1】' + '<br/>' + [勤務歴 職種カテゴリー1], NULL)
		, coalesce('【職種1】' + '<br/>' + [勤務歴 職種1], NULL)
		) as company
from csv_can c
--only 1 industry can be mapped so row_number will be applied
left join (select * from industry where rn=1) i on i.pa_name = c.[勤務歴 業種1]
left join fe_sfe fe on fe.pa_category = c.[勤務歴 職種カテゴリー1] and fe.pa_category_name = c.[勤務歴 職種1]
where 1=1
--and [PANO ] = 'CDT154379'
and coalesce(nullif([勤務歴 業種1], ''), nullif([勤務歴 職種カテゴリー1], ''), nullif([勤務歴 職種1], '')) is not NULL

UNION ALL
--Industry / FE / SFE set 2
	select [PANO ] as cand_ext_id
	, '勤務歴 会社名2' as currentEmployer
	, '勤務歴 役職名2' as jobTitle
	, [勤務歴 業種2] as origin_industry
	, i.industry_en as industry
	, [勤務歴 職種カテゴリー2] as origin_fe
	, concat('【PP】', vc_fe_en, coalesce(' / ' + nullif(vc_fe_ja,''), NULL)) as vc_fe
	, [勤務歴 職種2] as origin_sfe1
	, replace(trim(fe.vc_sfe_split), '[P]', '') as vc_sfe
	, '' as dateRangeFrom
	, '' as dateRangeTo
	, concat_ws('<br/>'
		, coalesce('【業種2】' + '<br/>' + [勤務歴 業種2], NULL)
		, coalesce('【職種カテゴリー2】' + '<br/>' + [勤務歴 職種カテゴリー2], NULL)
		, coalesce('【職種2】' + '<br/>' + [勤務歴 職種2], NULL)
		) as company
from csv_can c
--only 1 industry can be mapped so row_number will be applied
left join (select * from industry where rn=1) i on i.pa_name = c.[勤務歴 業種2]
left join fe_sfe fe on fe.pa_category = c.[勤務歴 職種カテゴリー2] and fe.pa_category_name = c.[勤務歴 職種2]
where 1=1
--and [PANO ] = 'CDT154379'
and coalesce(nullif([勤務歴 業種2], ''), nullif([勤務歴 職種カテゴリー2], ''), nullif([勤務歴 職種2], '')) is not NULL

UNION ALL
--Industry / FE / SFE set 3
	select [PANO ] as cand_ext_id
	, '勤務歴 会社名3' as currentEmployer
	, '勤務歴 役職名3' as jobTitle
	, [勤務歴 業種3] as origin_industry
	, i.industry_en as industry
	, [勤務歴 職種カテゴリー3] as origin_fe
	, concat('【PP】', vc_fe_en, coalesce(' / ' + nullif(vc_fe_ja,''), NULL)) as vc_fe
	, [勤務歴 職種3] as origin_sfe1
	, replace(trim(fe.vc_sfe_split), '[P]', '') as vc_sfe
	, '' as dateRangeFrom
	, '' as dateRangeTo
	, concat_ws('<br/>'
		, coalesce('【業種3】' + '<br/>' + [勤務歴 業種3], NULL)
		, coalesce('【職種カテゴリー3】' + '<br/>' + [勤務歴 職種カテゴリー3], NULL)
		, coalesce('【職種3】' + '<br/>' + [勤務歴 職種3], NULL)
		) as company
from csv_can c
--only 1 industry can be mapped so row_number will be applied
left join (select * from industry where rn=1) i on i.pa_name = c.[勤務歴 業種3]
left join fe_sfe fe on fe.pa_category = c.[勤務歴 職種カテゴリー3] and fe.pa_category_name = c.[勤務歴 職種3]
where 1=1
--and [PANO ] = 'CDT154379'
and coalesce(nullif([勤務歴 業種3], ''), nullif([勤務歴 職種カテゴリー3], ''), nullif([勤務歴 職種3], '')) is not NULL
)

/* Audit industry / fe / sfe | only 1 value each will be used/mapped
select * from dummy_wh
where cand_ext_id = 'CDT001120'
*/
/* Audit original category as FE / SFE
select [勤務歴 業種2], [勤務歴 職種カテゴリー2], [勤務歴 職種2]
from csv_can
where [PANO ] = 'CDT154379' --製造・技術系	回路設計

select *
from PA_fe_sfe
where pa_category = '製造・技術系'
and pa_category_name = '回路設計'

select *
from PA_fe_sfe
where pa_category = '流通・サービス・外食系'
and pa_category_name = '旅行・ホテル・ブライダル関連' 
*/

select *
into cand_work_history_fe_sfe
from dummy_wh
--where cand_ext_id = 'CDT001120'