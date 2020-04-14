--COMPANY LEGAL DOC START DATE
select [PANO ] as com_ext_id
, [契約日] as start_date
from csv_recf
where nullif([契約日],'') is not NULL

/* REVIEW 2 UPDATE
update company_legal_document
set type = '契約日'
where insert_timestamp > '2019-11-20'
and title = 'Default' --769
*/

--COMPANY REAL FILE NAME
with doc as (select seq
	, recf_id as company_id
	, pano as com_ext_id
	, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as UploadedName
	, memo
	, [file]
	, concat(coalesce(nullif(memo,''), concat_ws('_', convert(nvarchar,seq), pano))
			, right(trim([file]), charindex('.', reverse(trim([file]))))) as RealName
	from RECF_resume)

select com_ext_id
, UploadedName
, RealName
, memo
, current_timestamp as insert_timestamp
--maximum file name has 255 chars
, case when len(RealName) > 255 then concat(left(memo,100), right(trim([file]), charindex('.', reverse(trim([file])))))
	else RealName end as RealName_final
from doc
order by com_ext_id


/* Audit company logo

select pano, count(*)
from RECF_picture
group by pano
having count(*) > 1

*/
--COMPANY PICTURE
select pano as com_ext_id --can be used for external_id
, recf_id
, seq
, [file]
, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as uploaded_filename
, right(trim([file]), charindex('/', reverse(trim([file]))) - 1) as saved_filename
, case 
	when right(trim([file]), len(trim([file])) - charindex('.', trim([file]))) = 'jpg' then 'image/jpeg'
	when right(trim([file]), len(trim([file])) - charindex('.', trim([file]))) = 'gif' then 'image/gif'
	when right(trim([file]), len(trim([file])) - charindex('.', trim([file]))) = 'png' then 'image/png'
	else NULL end as mime_type
, 1 as version_no
, 'company_logo' as document_type
, current_timestamp as insert_timestamp
, current_timestamp as created
, -10 as user_account_id
, 0 as primary_document
, -1 as google_viewer
, 1 as visible
from RECF_picture