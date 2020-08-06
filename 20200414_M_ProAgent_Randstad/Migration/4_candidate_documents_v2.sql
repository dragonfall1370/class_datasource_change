--CANDIDATE REAL FILE NAME
with doc as (select seq
	, can_id
	, pano as cand_ext_id
	, charindex('.', [file]) as ext
	--extension should be reversed in case of wrong file extension
	, case when charindex('.', [file]) = 0 and charindex('.', reverse([memo])) > 1 then --starting position for extension
		reverse(substring(reverse([memo]), patindex('%[a-zA-Z]%', reverse([memo]))
				, 1 + charindex('.', reverse([memo])) - patindex('%[a-zA-Z]%', reverse([memo])))) 
		else NULL end as memo_ext
	, left(memo, case when charindex('.', memo) > 0 then charindex('.', memo) - 1 else len(memo) end) as memo_filename
	, concat(coalesce(nullif(memo,''), concat_ws('_', convert(nvarchar,seq), pano)), right(trim([file]), charindex('.', reverse(trim([file]))))) as RealName
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, [file]
	, memo
from CAN_resume
where 1=1
--and seq = '58587' --wrong extension and invalid case
--and seq = '58796'
--and pano = 'CDT152336' --invalid extension
--and pano = 'CDT076813' --wrong extension position
--and pano = 'CDT078522' --wrong extension
)

, doc_final as (select cand_ext_id
	, ext
	, memo_ext
	, memo_filename
	, memo
	, case when len(RealName) > 255 then concat(left(memo_filename,100), right(trim([file]), charindex('.', reverse(trim([file])))))
		else RealName end as RealName_final
	, case when ext > 0 then UploadedName
		else concat(UploadedName, memo_ext) end as UploadedName
	from doc)

select cand_ext_id
, UploadedName
, memo
, RealName_final
, current_timestamp as insert_timestamp
from doc_final
where 1=1
--and cand_ext_id = 'CDT078522' --wrong extension
--and cand_ext_id = 'CDT076813' --wrong extension position
--and cand_ext_id = 'CDT152336' --invalid extension
order by cand_ext_id