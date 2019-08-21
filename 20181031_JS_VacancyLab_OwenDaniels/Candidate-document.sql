with docdup as 
(select CandidateID as 'CandidateID', Filename as 'FileName' from tblDocument where CandidateID is not null),
------
docname as
(SELECT CandidateID as 'CandidateID', docdup.FileName
FROM docdup),
doctype as (select CandidateID as 'typeid',docdup.Filename from docdup where docdup.FileName like '%.PNG' or docdup.FileName like '%.JPG'),

candidate as (select tblCandidate.ContactID as 'id',
case when (tblCandidate.ContactID = docname.CandidateID) then docname.FileName else '' end as 'document',
'CANDIDATE' as 'entitytype',
case when (tblCandidate.ContactID=doctype.typeid) then 'candidate_photo' else 'resume' end
as 'doctype'

from tblCandidate
left join docname on tblCandidate.ContactID = docname.CandidateID
left join doctype on tblCandidate.ContactID = doctype.typeid),

candidatedup as (select candidate.id, candidate.document,candidate.doctype,candidate.entitytype, ROW_NUMBER() over(partition by candidate.document
order by candidate.id desc) as 'row doc' from candidate)

select candidatedup.id as 'candidate-externalid',
candidatedup.entitytype as 'entity_type',
candidatedup.document as 'file_name',
candidatedup.doctype as 'document_type'
from candidatedup
where candidatedup.[row doc]=1


