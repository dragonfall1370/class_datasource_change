--JOB REAL FILE NAME
with doc as (select seq
	, job_id
	, pano as job_ext_id
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, memo
	, [file]
	, concat(coalesce(nullif(memo,''), concat_ws('_', convert(nvarchar,seq), pano))
			, right(trim([file]), charindex('.', reverse(trim([file]))))) as RealName
	from JOB_resume)

select job_ext_id
, UploadedName
, RealName
, memo
, current_timestamp as insert_timestamp
--maximum file name has 255 chars
, case when len(RealName) > 255 then concat(left(memo,100), right(trim([file]), charindex('.', reverse(trim([file])))))
	else RealName end as RealName_final
from doc
order by job_ext_id