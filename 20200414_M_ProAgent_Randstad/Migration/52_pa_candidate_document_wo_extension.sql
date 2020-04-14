with doc as (select seq
	, can_id
	, pano as cand_ext_id 
	, charindex('.', [file]) as ext
	--extension should be reversed in case of wrong file extension
	, reverse(substring(reverse([memo]), patindex('%[a-zA-Z]%', reverse([memo]))
				, 1 + charindex('.', reverse([memo])) - patindex('%[a-zA-Z]%', reverse([memo])))) as memo_ext
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, [file]
	, memo
	, left(memo, case when charindex('.', memo) > 0 then charindex('.', memo) - 1 else len(memo) end) as memo_filename
	, concat(coalesce(nullif(memo,''), concat_ws('_', convert(nvarchar,seq), pano)), right(trim([file]), charindex('.', reverse(trim([file]))))) as RealName
from CAN_resume
where 1=1
--and exists (select [PANO ] from csv_can where csv_can.[PANO ] = CAN_resume.pano)
--and pano = 'CDT152336' --invalid extension
--and pano = 'CDT076813' --wrong extension position
--and pano = 'CDT078522' --wrong extension
)

, doc_final as (select cand_ext_id
	, memo
	, memo_filename
	, ext
	, memo_ext
	, case when ext > 0 then UploadedName
		else concat(UploadedName, memo_ext) end as UploadedName
	, RealName
	, case when len(RealName) > 255 then concat(left(memo_filename,100), right(trim([file]), charindex('.', reverse(trim([file])))))
	else RealName end as RealName_final
	from doc) 
	
select cand_ext_id
, memo
, UploadedName
, 'CANDIDATE' entity_type
, 'resume' document_type
, RealName_final
, current_timestamp as insert_timestamp
from doc_final 
where ext = 0
--and UploadedName like '480510%'
and cand_ext_id in ('CDT062667', 'CDT062763', 'CDT063457', 'CDT094872', 'CDT095026', 'CDT095247') --migrated candidate documents
order by UploadedName


---SAMPLE CHECK FROM VINCERE---
select *
from mike_tmp_candidate_dup_check
where cand_ext_id in ('CDT062667', 'CDT062763', 'CDT063457', 'CDT094872', 'CDT095026', 'CDT095247')

select id, external_id, deleted_timestamp
from candidate
where external_id in ('CDT062667', 'CDT062763', 'CDT063457', 'CDT094872', 'CDT095026', 'CDT095247')

select max(id), count(id)
from bulk_upload_document_mapping --max=181479 | count=181479

select *
from candidate_document
order by id desc
limit 30

select cd.id
, c.external_id
, cd.candidate_id
, cd.uploaded_filename
from candidate_document cd
join candidate c on c.id = cd.candidate_id
where candidate_id in (231573, 121937, 125502, 177971, 217966, 179115)