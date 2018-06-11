 select concat('MP',a.ApplicantId) as CandidateExternalId, -10 as userId, ng.CreatedOn as CommentTimestamp
		, ng.CreatedOn as InsertTimeStamp, -10 as AssignedUserId, 1 as RelatedStatus
		,ltrim(Stuff(
				Coalesce('Created On: ' + NULLIF(convert(varchar(20),ng.CreatedOn,120), ''), '')
				+ Coalesce(char(10)+ 'Updated On: ' + NULLIF(convert(varchar(20),ng.UpdatedOn,120), ''), '')
				+ Coalesce(char(10)+ 'Notebook Type: ' + NULLIF(ng.NotebookType, ''), '')
				+ Coalesce(char(10)+ 'From: ' + NULLIF(ng.[From], ''), '')
				+ Coalesce(char(10)+ 'Recipient(s): ' + NULLIF(left(ng.Recipients,len(ng.Recipients)-1), ''), '')
				+ Coalesce(char(10)+ 'Subject: ' + NULLIF(ng.Subject, ''), '')
				+ Coalesce(char(10)+ 'Email Content:' +char(10) + NULLIF(cni.ItemContent, ''), '')
			, 1, 0, '') ) as 'CommentContent'
from NotebookLinks nl 
 left join VW_NOTEBOOK_GRID ng on nl.NotebookItemId = ng.NotebookItemId
 left join Applicants a on nl.ObjectId = a.ApplicantId
 left join Convert_NotebookItemContent cni on ng.NotebookItemId = cni.NotebookItemId
where ApplicantId is not NULL and ng.NotebookTypeId in (75,84,53) and ng.NotebookItemId = 58-- and ng.NotebookItemId = 1
order by a.ApplicantId, ng.CreatedOn desc