with ContactNotebookInfo as (select concat('MP',cc.ClientContactId) as ContactExternalId, -10 as userId, ng.CreatedOn as CommentTimestamp
		, ng.CreatedOn as InsertTimeStamp, -10 as AssignedUserId, 1 as RelatedStatus
		,ltrim(Stuff(
				Coalesce('Created On: ' + NULLIF(convert(varchar(20),ng.CreatedOn,120), ''), '')
				+ Coalesce(char(10)+ 'Updated On: ' + NULLIF(convert(varchar(20),ng.UpdatedOn,120), ''), '')
				+ Coalesce(char(10)+ 'Notebook Type: ' + NULLIF(ng.NotebookType, ''), '')
				+ Coalesce(char(10) + iif(ng.Protected = 'N', 'Protected: No','Protected: Yes'), '')
				+ Coalesce(char(10) + iif(ng.HotItem = 'N', 'Hot: No','Hot: Yes'), '')
				+ Coalesce(char(10)+ 'Created User: ' + NULLIF(ng.CreatedUserName, ''), '')
				+ Coalesce(char(10)+ 'Created Date Only: ' + NULLIF(convert(varchar(10),ng.CreatedDateOnly,120), ''), '')
				+ Coalesce(char(10)+ 'Task: ' + t.Subject + ', Status: ' + ng.TaskStatusDescription, '')
				+ Coalesce(char(10)+ 'From: ' + NULLIF(ng.[From], ''), '')
				+ Coalesce(char(10)+ 'Recipient(s): ' + NULLIF(left(ng.Recipients,len(ng.Recipients)-1), ''), '')
				+ Coalesce(char(10)+ 'Subject: ' + NULLIF(ng.Subject, ''), '')
				+ Coalesce(char(10)+ 'Body Message:' +char(10) + NULLIF(replace(cni.ItemContent,concat(char(10),' ',char(10),' ',char(10)),char(10)), ''), '')
			, 1, 0, '') ) as 'CommentContent'
from NotebookLinks nl 
 left join VW_NOTEBOOK_GRID ng on nl.NotebookItemId = ng.NotebookItemId
 left join ClientContacts cc on nl.ObjectId = cc.ContactPersonId
 left join Convert_NotebookItemContent_2016 cni on ng.NotebookItemId = cni.NotebookItemId
 left join Tasks t on ng.TaskId = t.TaskId
where cc.ContactPersonId is not NULL
	and cni.NotebookItemId is not null)
	--and ng.NotebookTypeId in (53,75,84))-- and ng.NotebookItemId = 58-- and ng.NotebookItemId = 1
--order by ContactExternalId, CommentTimestamp desc

select * from ContactNotebookInfo order by ContactExternalId, CommentTimestamp desc--ContactExternalId = 'MP539'order by CandidateExternalId, CommentTimestamp
select count(*) from Convert_NotebookItemContent_2017
select count(*) from VW_NOTEBOOK_GRID where year(CreatedOn) = 2017
select count(*) from NotebookLinks where ObjectId = 539 and year(CreatedOn) = 2017

--select count(*) from Convert_NotebookItemContent_2016

