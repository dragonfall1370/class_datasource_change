
with 
-- DOCUMENT
 t4(candidateUserID, finame) as (SELECT candidateUserID, STUFF((SELECT ',' + name from bullhorn1.View_CandidateFile WHERE candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS string FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID)

-- Files
--select count(*) from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') --173091
--, files(candidateUserID, ResumeId) as (SELECT candidateUserID, STUFF((SELECT DISTINCT ',' + concat(candidateFileID, fileExtension) from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and candidateUserID = a.candidateUserID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_CandidateFile AS a GROUP BY a.candidateUserID) --where a.type = 'Resume') ==> get all candidates files
, files(candidateUserID, resumeId, filename) as ( SELECT candidateUserID, concat(candidateFileID, fileExtension) as ResumeId, concat(name, fileExtension) as filename from bullhorn1.View_CandidateFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') )
--select count(*) from files --173091
--select * from files where candidateuserid in (34214)


-- Placement Files
--, placementfiles(userID, placementfile) as (SELECT userID, STUFF((SELECT DISTINCT ',' + concat(placementFileID, fileExtension) from bullhorn1.View_PlacementFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') and userID = a.userID FOR XML PATH (''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '')  AS URLList FROM bullhorn1.View_PlacementFile AS a GROUP BY a.userID)
, placementfiles(userID, placementfile, filename) as (SELECT userID, concat(placementFileID, fileExtension) as placementfile, concat(name, fileExtension) as filename from bullhorn1.View_PlacementFile WHERE fileExtension in ('.doc','.docx','.pdf','.xls','.xlsx','.rtf','.html') )
--select top 10 * from placementfiles
--select count(*) from placementfiles --11



select --top 1000
         C.candidateID as 'candidate-externalId', C.userID as '#userID'
	, coalesce(nullif(replace(C.FirstName,'?',''), ''), 'No Firstname') as 'contact-firstName'
       , coalesce(nullif(replace(C.LastName,'?',''), ''), concat('Lastname-',C.userID)) as 'contact-lastName'
	, C.middleName as 'candidate-middleName'
	--, stuff( coalesce(' ' + nullif(files.ResumeId, ''), '') + coalesce(', ' + nullif(p.placementfile, ''), ''), 1, 1, '') as 'candidate-resume'
	, f.resumeId
	, f.filename
--select count (*) --133616
from bullhorn1.Candidate C 
left join (select * from files union all select * from placementfiles ) f on f.candidateUserID = C.userid
where C.isdeleted <> 1 and C.status <> 'Archive' and f.candidateUserID is not null
--and C.userid in (34214)