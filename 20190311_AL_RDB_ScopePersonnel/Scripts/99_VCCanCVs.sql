drop table if exists VCCanCVs

;with
TmpTab1 as (
select
x.ApplicantId as CanId
, y.CVId
, y.CVRefNo
, replace(
	isnull(nullif(trim(isnull(a.FileAs, 'NoFileName')), ''), 'NoFileName')
	, ' ', '_'
) as DocName
, z.FileExtension
from
(select ApplicantId from RF_Candidates_Complete) x
left join CV y on x.ApplicantId = y.ApplicantId
left join CVContents z on y.CVId = z.CVId
left join (select ObjectId, FileAs from Objects where ObjectTypeId = 1) a on x.ApplicantId = a.ObjectID
where
z.FileExtension in
-- supported doc type for document
--(
--	'.pdf'
--	,'.doc'
--	,'.rtf'
--	,'.xls'
--	,'.xlsx'
--	,'.docx'
--	,'.png'
--	,'.jpg'
--	,'.jpeg'
--	,'.gif'
--	,'.bmp'
--	,'.msg'
--)
-- supported doc type for resume
(
	'.pdf'
	,'.doc'
	,'.docx'
	,'.rtf'
	,'.xls'
	,'.xlsx'
	,'.html'
	,'.txt'
)
)

, TmpTab2 as (
select
*
, row_number() over(partition by CanId, DocName order by CVId desc) rn
from TmpTab1
)



select
CanId
--, CVRefNo
, concat(CanId, '_', DocName, FileExtension) DocName
into VCCanCVs

from TmpTab2

where rn = 1

select * from VCCanCVs
--where CanId = 95759