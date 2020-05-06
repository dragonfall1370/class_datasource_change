/* AWS S3 FOLDERS
s3://fra-vc-p1-file2/nextphase.vincere.io/upload/file/5c1748a8-04ce-4b2b-943a-9d204fd1658a/np_contactdoc.zip
s3://fra-vc-p1-file2/nextphase.vincere.io/upload/file/87806e45-7ce8-4944-9d69-b3d9af0b3b9d/np_cv.zip
s3://fra-vc-p1-file2/nextphase.vincere.io/upload/file/dd534394-f1d2-48fa-b6c9-05f4d5a286df/np_cvsend.zip
s3://fra-vc-p1-file2/nextphase.vincere.io/upload/file/6ca660d2-b281-46fb-bac6-4a30b8face0f/np_interviews.zip
s3://fra-vc-p1-file2/nextphase.vincere.io/upload/file/939c62ec-4239-44de-aea3-5c5814da63fb/np_jobdocs.zip
*/

-->>CONTACT<<--
--CONTACT Document Name
/*
select concat('NP', cc.ClientContactId) as con_ext_id
, cc.ContactPersonId
, concat_ws('_','NP_C', d.DocumentID, concat(cc.ClientContactId, dc.FileExtension)) as UploadedName
, d.Description
, case when right(trim(d.Description), charindex('.', reverse(trim(d.Description)))) = concat_ws('_', d.DocumentID, dc.FileExtension then d.Description
	else concat(trim(d.Description), dc.FileExtension) end as RealName --decription may probably include file extension
, dc.FileExtension
, convert(datetime, dc.CreatedOn, 120) as Created
from ClientContacts cc
left join NotebookLinks nl on nl.ObjectId = cc.ContactPersonId
left join Documents d on d.NotebookItemId = nl.NotebookItemId
left join DocumentContent dc on dc.DocumentId = d.DocumentID
where d.Description is not NULL
and d.Description <> ''
and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.msg','.txt','.htm','.html')
--total: 113871
*/

select concat('NP', cc.ClientContactId) as con_ext_id
, cc.ContactPersonId
, concat_ws('_','NP_S', t.TemplateId, concat(cc.ClientContactId, td.FileExtension)) as UploadedName
, t.TemplateName
, case when right(trim(t.TemplateName), charindex('.', reverse(trim(t.TemplateName)))) = concat_ws('_', td.TemplateId, td.FileExtension) 
			then t.Description
	else concat(trim(t.TemplateName), td.FileExtension) end as RealName --decription may probably include file extension
, td.FileExtension
, convert(datetime, td.CreatedOn, 120) as Created
from ClientContacts cc
left join Templates t on t.ObjectId = cc.ContactPersonId
left join TemplateDocument td on td.TemplateId = t.TemplateId
where 1=1
and td.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.msg','.txt','.htm','.html')
and t.TemplateTypeId = 53


-->>JOB<<--
--JOB Document Name
select concat('NP', jd.JobId) as job_ext_id
	, concat_ws('_','NP_JD',jd.DocumentId,concat(jd.jobId,dc.FileExtension)) as UploadedName
	, d.Description
	, case when right(trim(d.Description), charindex('.', reverse(trim(d.Description)))) = dc.FileExtension then trim(d.Description)
		when trim(d.Description) = '' or d.Description is NULL then concat_ws('_', 'jobdocuments', jd.DocumentId, concat(jd.jobId,dc.FileExtension))
		else concat(trim(d.Description), dc.FileExtension) end as RealName --decription may probably include file extension
	, dc.FileExtension
	, convert(datetime, dc.CreatedOn, 120) as Created
from JobDocuments jd
left join DocumentContent dc on dc.DocumentID=jd.DocumentId
left join Documents d on d.DocumentId = dc.DocumentId
where dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html') --21 rows
--and d.Description <> '' --3 rows

/*
--JOB Placement Name | No need
select concat('NP', p.JobId) as job_ext_id
, concat_ws('_','NP_P', d.DocumentID, concat(p.JobId, dc.FileExtension)) as UploadedName
, d.Description
, case when right(trim(d.Description), charindex('.', reverse(trim(d.Description)))) = dc.FileExtension then d.Description
		when trim(d.Description) = '' or d.Description is NULL then concat_ws('_', 'placement', d.DocumentID, p.PlacementID, dc.FileExtension)
		else concat(trim(d.Description), dc.FileExtension) end as RealName --decription may probably include file extension
, dc.FileExtension
, convert(datetime, dc.CreatedOn, 120) as Created
from PlacementDocuments pd
left join Placements p on p.PlacementID = pd.PlacementID
left join Documents d on d.DocumentID = pd.DocumentId
left join DocumentContent dc on d.DocumentId = dc.DocumentId
where 1=1
and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
and p.JobId is not NULL
*/

--JOB CVSEND Documents
select concat('NP', aa.JobId) as job_ext_id
		, concat_ws('_','NP_CVSend', d.DocumentID, concat(aa.JobId, dc.FileExtension)) as UploadedName
		, d.Description
		, case when right(trim(d.Description), charindex('.', reverse(trim(d.Description)))) = dc.FileExtension then trim(d.Description)
			when trim(d.Description) = '' or d.Description is NULL then concat_ws('_','cvsend', cvs.CVSendDocumentId, concat(aa.JobId, dc.FileExtension))
			else concat(trim(d.Description), dc.FileExtension) end as RealName --decription may probably include file extension
		, dc.FileExtension
		, convert(datetime, dc.CreatedOn, 120) as Created
from CVSendDocuments cvs
left join ApplicantActions aa on aa.ApplicantActionId = cvs.ApplicantActionId
left join Documents d on d.DocumentID = cvs.DocumentId
left join DocumentContent dc on d.DocumentId = dc.DocumentId
where 1=1
and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
and aa.JobId is not NULL --17270 rows


--JOB INTERVIEW Documents
select concat('NP',nl.JobId) as job_ext_id
		, concat_ws('_','NP_IV', d.DocumentID, concat(nl.JobId, dc.FileExtension)) as UploadedName
		, d.Description
		, case when right(trim(d.Description), charindex('.', reverse(trim(d.Description)))) = dc.FileExtension then trim(d.Description)
			when trim(d.Description) = '' or d.Description is NULL then concat_ws('_', 'Interviews', d.DocumentID, concat(nl.JobId, dc.FileExtension))
			else concat(trim(d.Description), dc.FileExtension) end as RealName --decription may probably include file extension
		, dc.FileExtension
		, convert(datetime, dc.CreatedOn, 120) as Created
		from (select distinct InterviewDocumentId, NotebookItemId from InterviewDocuments) id --5602
		left join NotebookLinks nl on nl.NotebookItemId = id.NotebookItemId
		left join Documents d on d.NotebookItemId = nl.NotebookItemId
		left join DocumentContent dc on dc.DocumentId = d.DocumentID
		where 1=1
		and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')
		and nl.JobId is not NULL --5656 rows


-->>CANDIDATE<<--
--CANDIDATE CV: no need to change real name
select cv.ApplicantId
, concat_ws('_', 'NP_CV', cv.CVId, concat(cv.ApplicantId, cvc.FileExtension)) as candidate_cv
from CV
left join CVContents cvc on cvc.CVId = cv.CVId
where cvc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')

/*
--CANDIDATE CVSEND
select concat('NP', aa.ApplicantId) as cand_ext_id
, concat_ws('_','NP_D', d.DocumentID, concat(aa.ApplicantId, dc.FileExtension)) as UploadedName
, d.Description
, case when right(trim(d.Description), charindex('.', reverse(trim(d.Description)))) = dc.FileExtension then d.Description
		when trim(d.Description) = '' or d.Description is NULL then concat_ws('_', 'cvsend', d.DocumentID, aa.ApplicantId, dc.FileExtension)
		else concat(trim(d.Description), dc.FileExtension) end as RealName --decription may probably include file extension
, dc.FileExtension
, convert(datetime, dc.CreatedOn, 120) as Created
from CVSendDocuments cvs
left join ApplicantActions aa on aa.ApplicantActionId = cvs.ApplicantActionId
left join Documents d on d.DocumentID = cvs.DocumentId
left join DocumentContent dc on d.DocumentId = dc.DocumentId
where cvs.CVId is NULL --10321 --CV already parsed
and dc.FileExtension in ('.pdf','.doc','.rtf','.xls','.xlsx','.docx','.png','.jpg','.jpeg','.gif','.bmp','.msg','.txt','.htm','.html')