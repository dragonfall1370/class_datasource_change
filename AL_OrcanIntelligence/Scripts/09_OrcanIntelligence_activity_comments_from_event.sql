-- Activities Comments
-- Event

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
  x.Id

, iif(len(trim(isnull(x.AccountId, ''))) > 0 and trim(isnull(x.AccountId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
	, trim(isnull(x.AccountId, ''))
	, iif(len(trim(isnull(rel.AccountId, ''))) > 0 and trim(isnull(rel.AccountId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
		, trim(isnull(rel.AccountId, '')), null)) as ComExtId

, iif(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
	, trim(isnull(x.WhoId, ''))
	, iif(len(trim(isnull(rel.RelationId, ''))) > 0 and trim(isnull(rel.RelationId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
		, trim(isnull(rel.RelationId, '')), null)) as ConExtId

, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA' and len(trim(isnull(job.Id, ''))) > 0, trim(isnull(job.Id, '')), null)
	as JobExtId

, iif(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RD'
	, trim(isnull(x.WhoId, ''))
	, iif(len(trim(isnull(rel.RelationId, ''))) > 0 and trim(isnull(rel.RelationId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RD'
		, trim(isnull(rel.RelationId, '')), null)) as CanExtId

, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA' and len(trim(isnull(app.Id, ''))) > 0, trim(isnull(app.Id, '')), null)
	as AppExtId

, x.CreatedDate as insert_timestamp

, concat(
upper('Event')
, @NewLineChar
, replicate('-', len('Event'))
, @NewLineChar
, trim(@NewLineChar from concat(
	iif(len(trim(isnull(convert(varchar(50), x.ActivityDateTime, 111), ''))) > 0,  @NewLineChar + 'Activity Date: ' + trim(isnull(convert(varchar(50), x.ActivityDateTime, 111), '')), '')
	
	, @NewLineChar + 'Owner: ' + concat(trim(isnull(u.FirstName, '')), ' ', trim(isnull(u.LastName, '')), ' (External ID:', u.Id, ')')

	, @NewLineChar + 'All Day Event: ' + iif(x.IsAllDayEvent = 1, 'Yes', 'No')

	, @NewLineChar + 'Archived: ' + iif(x.IsArchived = 1, 'Yes', 'No')

	, @NewLineChar + 'Private: ' + iif(x.IsPrivate = 1, 'Yes', 'No')

	, @NewLineChar + 'Completed: ' + iif(x.Completed__c = 1, 'Yes', 'No')

	, iif(len(trim(isnull(x.Subject, ''))) > 0, @NewLineChar + 'Subject:' + @NewLineChar + trim(isnull(x.Subject, '')), '')

	, iif(len(trim(isnull(x.Location, ''))) > 0, @NewLineChar + 'Location: ' + trim(isnull(x.Location, '')), '')

	, iif(len(trim(isnull(x.ShowAs, ''))) > 0, @NewLineChar + 'Show As: ' + trim(isnull(x.ShowAs, '')), '')

	, iif(len(trim(isnull(x.Description, ''))) > 0, @NewLineChar + 'Description:' + @NewLineChar + trim(isnull(x.Description, '')), '')

	, iif(len(trim(isnull(x.Type, ''))) > 0, @NewLineChar + 'Type: ' + trim(isnull(x.Type, '')), '')

	, iif(x.DurationInMinutes is not null, @NewLineChar + 'Duration (m): ' + cast(x.DurationInMinutes as varchar(20)), '')
	
	, iif(len(trim(isnull(x.AVTRRT__Call_Result__c, ''))) > 0, @NewLineChar + 'Call Result: ' + trim(isnull(x.AVTRRT__Call_Result__c, '')), '')

	, iif(len(trim(isnull(x.AVTRRT__Message_Result__c, ''))) > 0, @NewLineChar + 'MessageResult: ' + trim(isnull(x.AVTRRT__Message_Result__c, '')), '')

	, iif(len(trim(isnull(x.AVTRRT__Message_Type__c, ''))) > 0, @NewLineChar + 'Message Type: ' + trim(isnull(x.AVTRRT__Message_Type__c, '')), '')

	, iif(len(trim(isnull(x.AVTRRT__Comments__c, ''))) > 0, @NewLineChar + 'Comments: ' + trim(isnull(x.AVTRRT__Comments__c, '')), '')

	, iif(len(trim(isnull(x.Meeting__c, ''))) > 0, @NewLineChar + 'Meeting: ' + trim(isnull(x.Meeting__c, '')), '')

	, iif(len(trim(isnull(x.Meeting_Comments__c, ''))) > 0, @NewLineChar + 'Meeting Comments: ' + trim(isnull(x.Meeting_Comments__c, '')), '')

	, iif(
		(
			(len(trim(isnull(x.AccountId, ''))) > 0 and trim(isnull(x.AccountId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(rel.AccountId, ''))) > 0 and trim(isnull(rel.AccountId, '')) <> '000000000000000AAA')
		)
		and con.RecordTypeId = '012b0000000J2RE'
		,   @NewLineChar + 'Account: ' + concat(trim(isnull(com.Name, '')), ' (External ID: ', com.Id, ')')
		, ''
	)

	, iif(
		(
			(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(rel.RelationId, ''))) > 0 and trim(isnull(rel.RelationId, '')) <> '000000000000000AAA')
		)
		and con.RecordTypeId = '012b0000000J2RE'
		,  @NewLineChar + 'Contact: ' + concat(isnull(con.FirstName, ''), ' ', isnull(con.LastName, ''), ' (External ID: ', con.Id, ')')
		, ''
	)
	
	, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA' and len(trim(isnull(job.Id, ''))) > 0,  @NewLineChar + 'Job: ' + concat(isnull(job.AVTRRT__Job_Title__c, ''), ' (External ID: ', job.Id, ')'), '')
	
	, iif(
		(
			(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(rel.RelationId, ''))) > 0 and trim(isnull(rel.RelationId, '')) <> '000000000000000AAA')
		)
		and con.RecordTypeId = '012b0000000J2RD'
		,  @NewLineChar + 'Contact: ' + concat(isnull(con.FirstName, ''), ' ', isnull(con.LastName, ''), ' (External ID: ', con.Id, ')')
		, ''
	)

	, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA' and len(trim(isnull(app.Id, ''))) > 0,  @NewLineChar + 'Application: ' + concat(isnull(app.Name, ''), ' (External ID: ', app.Id, ')'), '')

	, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA' and len(trim(isnull(inv.Id, ''))) > 0,  @NewLineChar + 'Interview: ' + concat(isnull(inv.Name, ''), ' (External ID: ', inv.Id, ')'), '')
))) as content

, 'comment' as category

, 'company'
--, 'contact'
--, 'job'
--, 'candidate'
--, 'application'
as type

, -10 as user_account_id

from [Event] x
left join EventRelation rel on rel.EventId = x.Id
left join Account com on com.Id = x.AccountId
left join Contact con on con.Id = x.WhoId or con.Id = rel.RelationId
left join AVTRRT__Job__c job on job.Id = x.WhatId
left join AVTRRT__Job_Applicant__c app on app.Id = x.WhatId
left join AVTRRT__Interview__c inv on inv.Id = x.WhatId
left join AVTRRT__Placement__c pla on pla.Id = x.WhatId
left join [User] u on u.Id = x.OwnerId
where
x.IsDeleted = 0 and
(
	(len(trim(isnull(x.AccountId, ''))) > 0 and trim(isnull(x.AccountId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(rel.AccountId, ''))) > 0 and trim(isnull(rel.AccountId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(rel.RelationId, ''))) > 0 and trim(isnull(rel.RelationId, '')) <> '000000000000000AAA') 
	or (len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA')
)
order by x.CreatedDate