-->> Update Contact Full Name Kanji
with rename as (select [採用担当者ID]
		, charindex(' ', [採用担当者]) as char_index
		, case when charindex(' ', [採用担当者]) = 1 then right([採用担当者], len([採用担当者]) - 1)
			else [採用担当者] end as [採用担当者]
		from csv_rec
		)

select --top 10 
c.[採用担当者ID] as con_ext_id --#CF inject then
	, [採用担当者]
	, case when len(trim([採用担当者])) <= 2 then left([採用担当者], 1)
		when charindex(' ', trim([採用担当者])) = 0 then left(trim([採用担当者]), 2)
		when charindex(' ', trim([採用担当者])) < len(trim([採用担当者])) and charindex(' ', trim([採用担当者])) > 0 then left(trim([採用担当者]), charindex(' ', trim([採用担当者])) - 1)
		else trim([採用担当者]) end as con_fname
	, case when len(trim([採用担当者])) <= 2 then right(trim([採用担当者]), 1)
		when charindex(' ', trim([採用担当者])) = 0 then trim(right(trim([採用担当者]), len([採用担当者]) - 2))
		when charindex('　', trim([採用担当者])) < len(trim([採用担当者])) then right(trim([採用担当者]), len(trim([採用担当者])) - charindex('　', trim([採用担当者])))
		else coalesce(nullif(right(trim([採用担当者]), len(trim([採用担当者])) - len(left(trim([採用担当者]), 2))),''),'Last name') end as con_lname
from rename c
where [採用担当者] <> '' --remove blank values
order by c.[採用担当者ID]


-->> Update Candidate Full Name Kanji
with rename as (select [PANO ]
		, charindex(' ', [氏名]) as char_index
		, case when charindex(' ', [氏名]) = 1 then right([氏名], len([氏名]) - 1)
			else [氏名] end as [氏名]
		from csv_can
		--where [PANO ] = 'CDT129812'
		)

select --top 10 
c.[PANO ] as cand_ext_id --#CF inject then
	, [氏名]
	, case when len(trim([氏名])) <= 2 then left([氏名], 1)
		when charindex('　', trim([氏名])) < len(trim([氏名])) and charindex('　', trim([氏名])) > 0 then left(trim([氏名]), charindex('　', trim([氏名])) - 1)
		when charindex('　', trim([氏名])) = 0 then left(trim([氏名]), 2)
		else trim([氏名]) end as cand_fname
	, case when len(trim([氏名])) <= 2 then right(trim([氏名]), 1)
		when charindex('　', trim([氏名])) < len(trim([氏名])) and charindex('　', trim([氏名])) > 0 then right(trim([氏名]), len(trim([氏名])) - charindex('　', trim([氏名])))
		when charindex('　', trim([氏名])) = 0 then right(trim([氏名]), len(trim([氏名])) - 2)
		else coalesce(nullif(right(trim([氏名]), len(trim([氏名])) - len(left(trim([氏名]), 2))),''),'Last name') end as cand_lname
from rename c
where 1=1
and [氏名] <> ''
--and [PANO ] = 'CDT155711'