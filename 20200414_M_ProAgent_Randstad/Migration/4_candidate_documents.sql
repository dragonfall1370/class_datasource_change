--CANDIDATE REAL FILE NAME
with doc as (select seq
	, can_id
	, pano as can_ext_id
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, memo
	, left(memo, case when charindex('.', memo) > 0 then charindex('.', memo) - 1 else len(memo) end) as memo_filename
	, [file]
	, concat(coalesce(nullif(memo,''), concat_ws('_', convert(nvarchar,seq), pano)), right(trim([file]), charindex('.', reverse(trim([file]))))) as RealName
	from CAN_resume)

select can_ext_id
, UploadedName
, RealName
, memo
, memo_filename
, current_timestamp as insert_timestamp
--maximum file name has 255 chars
, case when len(RealName) > 255 then concat(left(memo_filename,100), right(trim([file]), charindex('.', reverse(trim([file])))))
	else RealName end as RealName_final
from doc
order by can_ext_id