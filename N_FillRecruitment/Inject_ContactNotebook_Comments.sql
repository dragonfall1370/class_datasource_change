 select concat('FR',cc.ClientContactId) as ContactExternalId, -10 as userId, ng.CreatedOn as CommentTimestamp
		, ng.CreatedOn as InsertTimeStamp, -10 as AssignedUserId, 1 as RelatedStatus
		,ltrim(Stuff(
				Coalesce('Created On: ' + NULLIF(convert(varchar(20),ng.CreatedOn,120), ''), '')
				+ Coalesce(char(10)+ 'Notebook Type: ' + NULLIF(ng.NotebookType, ''), '')
				+ Coalesce(char(10)+ 'From: ' + NULLIF(ng.[From], ''), '')
				+ Coalesce(char(10)+ 'Recipient(s): ' + NULLIF(left(ng.Recipients,len(ng.Recipients)-1), ''), '')
				+ Coalesce(char(10)+ 'Subject: ' + NULLIF(ng.Subject, ''), '')
			, 1, 0, '') ) as 'CommentContent'
from VW_NOTEBOOK_GRID ng 
 left join NotebookLinks nl on ng.NotebookItemId = nl.NotebookItemId
 left join ClientContacts cc on nl.ObjectId = cc.ContactPersonId
where ClientContactId is not NULL
order by cc.ClientContactId, ng.CreatedOn desc


