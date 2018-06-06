--UPDATE MISSING DOCUMENTS due to FILES FILTER
with CandidateFiles as (select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C
	, NAME as CandidateFiles
	from ResumeCompliance
	where NAME like '%.doc%' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html' --edit the error: .%doc

UNION ALL

select PARENTID
	, NAME
	from Attachments
	where NAME like '%.doc%' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'

UNION ALL

select PEOPLECLOUD1__DOCUMENT_RELATED_TO__C
	, NAME as CandidateFiles
	from ResumeComplianceDelta
	where NAME like '%.doc%' or NAME like '%.pdf' or NAME like '%.rtf' or NAME like '%.xls%' or NAME like '%.html'
) --edit the error: .%doc

, MainCandidate as (select c.ID
	, cf.CandidateFiles
	from Candidate c
	left join CandidateFiles cf on cf.PEOPLECLOUD1__DOCUMENT_RELATED_TO__C = c.ID
	where c.PEOPLECLOUD1__STATUS__C not in ('Inactive') or c.PEOPLECLOUD1__STATUS__C is NULL

UNION ALL

select c.ID
	, cf.CandidateFiles
	from CandidateDelta c
	left join CandidateFiles cf on cf.PEOPLECLOUD1__DOCUMENT_RELATED_TO__C = c.ID
	where c.PEOPLECLOUD1__STATUS__C not in ('Inactive') or c.PEOPLECLOUD1__STATUS__C is NULL

)

select ID as Sirius_CandExtID
, 'CANDIDATE' as Sirius_entity_type
, 'resume' as Sirius_document_type
, CandidateFiles as Sirius_file_name
, 0 as Sirius_default_file
from MainCandidate
where CandidateFiles is not NULL