-- Activities Comments
-- Email Message

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
  x.Id

, iif(len(trim(isnull(t.AccountId, ''))) > 0 and trim(isnull(t.AccountId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
	, trim(isnull(t.AccountId, ''))
	, iif(len(trim(isnull(tr.AccountId, ''))) > 0 and trim(isnull(tr.AccountId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
		, trim(isnull(tr.AccountId, ''))
		, iif(len(trim(isnull(e.AccountId, ''))) > 0 and trim(isnull(e.AccountId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
			, trim(isnull(e.AccountId, ''))
			, iif(len(trim(isnull(er.AccountId, ''))) > 0 and trim(isnull(er.AccountId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
				, trim(isnull(er.AccountId, '')), null)))) as ComExtId

, iif(len(trim(isnull(t.WhoId, ''))) > 0 and trim(isnull(t.WhoId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
	, trim(isnull(t.WhoId, ''))
	, iif(len(trim(isnull(tr.RelationId, ''))) > 0 and trim(isnull(tr.RelationId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
		, trim(isnull(tr.RelationId, ''))
		, iif(len(trim(isnull(e.WhoId, ''))) > 0 and trim(isnull(e.WhoId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
			, trim(isnull(e.WhoId, ''))
			, iif(len(trim(isnull(er.RelationId, ''))) > 0 and trim(isnull(er.RelationId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RE'
				, trim(isnull(er.RelationId, '')), null)))) as ConExtId

, iif(((len(trim(isnull(t.WhatId, ''))) > 0 and trim(isnull(t.WhatId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(e.WhatId, ''))) > 0 and trim(isnull(e.WhatId, '')) <> '000000000000000AAA')) and len(trim(isnull(job.Id, ''))) > 0, trim(isnull(job.Id, '')), null)
	as JobExtId

, iif(len(trim(isnull(t.WhoId, ''))) > 0 and trim(isnull(t.WhoId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RD'
	, trim(isnull(t.WhoId, ''))
	, iif(len(trim(isnull(tr.RelationId, ''))) > 0 and trim(isnull(tr.RelationId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RD'
		, trim(isnull(tr.RelationId, ''))
		, iif(len(trim(isnull(e.WhoId, ''))) > 0 and trim(isnull(e.WhoId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RD'
			, trim(isnull(e.WhoId, ''))
			, iif(len(trim(isnull(er.RelationId, ''))) > 0 and trim(isnull(er.RelationId, '')) <> '000000000000000AAA' and con.RecordTypeId = '012b0000000J2RD'
				, trim(isnull(er.RelationId, '')), null)))) as CanExtId

, iif(((len(trim(isnull(t.WhatId, ''))) > 0 and trim(isnull(t.WhatId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(e.WhatId, ''))) > 0 and trim(isnull(e.WhatId, '')) <> '000000000000000AAA')) and len(trim(isnull(app.Id, ''))) > 0, trim(isnull(app.Id, '')), null)
	as AppExtId

, x.CreatedDate as insert_timestamp

, concat(
upper('Email Message')
, @NewLineChar
, replicate('-', len('Email Message'))
, @NewLineChar
, trim(@NewLineChar from concat(
	iif(len(trim(isnull(convert(varchar(50), x.MessageDate, 111), ''))) > 0,  @NewLineChar + 'Message Date: ' + trim(isnull(convert(varchar(50), x.MessageDate, 111), '')), '')
	
	, @NewLineChar + 'Creator: ' + concat(trim(isnull(u.FirstName, '')), ' ', trim(isnull(u.LastName, '')), ' (External ID:', u.Id, ')')

	, @NewLineChar + 'Status: ' + trim(isnull(cast(x.Status as varchar(10)), ''))

	, @NewLineChar + 'Incoming: ' + iif(x.Incoming = 1, 'Yes', 'No')

	, @NewLineChar + 'Has Attachment: ' + iif(x.HasAttachment = 1, 'Yes', 'No')

	, iif(len(trim(isnull(x.Headers, ''))) > 0, @NewLineChar + 'Headers: ' + trim(isnull(x.Headers, '')), '')

	, iif(len(trim(isnull(x.FromName, ''))) > 0, @NewLineChar + 'From Name: ' + trim(isnull(x.FromName, '')), '')

	, iif(len(trim(isnull(x.FromAddress, ''))) > 0, @NewLineChar + 'From Address: ' + trim(isnull(x.FromAddress, '')), '')

	, iif(len(trim(isnull(x.ToAddress, ''))) > 0, @NewLineChar + 'To Address: ' + trim(isnull(x.ToAddress, '')), '')

	, iif(len(trim(isnull(x.CcAddress, ''))) > 0, @NewLineChar + 'Cc Address: ' + trim(isnull(x.CcAddress, '')), '')

	, iif(len(trim(isnull(x.BccAddress, ''))) > 0, @NewLineChar + 'Bcc Address: ' + trim(isnull(x.BccAddress, '')), '')

	, iif(len(trim(isnull(x.Subject, ''))) > 0, @NewLineChar + 'Subject: ' + trim(isnull(x.Subject, '')), '')

	, iif(len(trim(isnull(x.TextBody, ''))) > 0, @NewLineChar + 'Text Body: ' + trim(isnull(x.TextBody, '')), '')

	, iif(
		(
			(len(trim(isnull(t.AccountId, ''))) > 0 and trim(isnull(t.AccountId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(tr.AccountId, ''))) > 0 and trim(isnull(tr.AccountId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(e.AccountId, ''))) > 0 and trim(isnull(e.AccountId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(er.AccountId, ''))) > 0 and trim(isnull(er.AccountId, '')) <> '000000000000000AAA')
		)
		and con.RecordTypeId = '012b0000000J2RE'
		,   @NewLineChar + 'Account: ' + concat(trim(isnull(com.Name, '')), ' (External ID: ', com.Id, ')')
		, ''
	)

	, iif(
		(
			(len(trim(isnull(t.WhoId, ''))) > 0 and trim(isnull(t.WhoId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(tr.RelationId, ''))) > 0 and trim(isnull(tr.RelationId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(e.WhoId, ''))) > 0 and trim(isnull(e.WhoId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(er.RelationId, ''))) > 0 and trim(isnull(er.RelationId, '')) <> '000000000000000AAA')
		)
		and con.RecordTypeId = '012b0000000J2RE'
		,  @NewLineChar + 'Contact: ' + concat(isnull(con.FirstName, ''), ' ', isnull(con.LastName, ''), ' (External ID: ', con.Id, ')')
		, ''
	)
	
	, iif(((len(trim(isnull(t.WhatId, ''))) > 0 and trim(isnull(t.WhatId, '')) <> '000000000000000AAA')
		or (len(trim(isnull(e.WhatId, ''))) > 0 and trim(isnull(e.WhatId, '')) <> '000000000000000AAA')) and len(trim(isnull(job.Id, ''))) > 0,  @NewLineChar + 'Job: ' + concat(isnull(job.AVTRRT__Job_Title__c, ''), ' (External ID: ', job.Id, ')'), '')
	
	, iif(
		(
			(len(trim(isnull(t.WhoId, ''))) > 0 and trim(isnull(t.WhoId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(tr.RelationId, ''))) > 0 and trim(isnull(tr.RelationId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(e.WhoId, ''))) > 0 and trim(isnull(e.WhoId, '')) <> '000000000000000AAA')
			or (len(trim(isnull(er.RelationId, ''))) > 0 and trim(isnull(er.RelationId, '')) <> '000000000000000AAA')
		)
		and con.RecordTypeId = '012b0000000J2RD'
		,  @NewLineChar + 'Contact: ' + concat(isnull(con.FirstName, ''), ' ', isnull(con.LastName, ''), ' (External ID: ', con.Id, ')')
		, ''
	)

	, iif(((len(trim(isnull(t.WhatId, ''))) > 0 and trim(isnull(t.WhatId, '')) <> '000000000000000AAA')
		or (len(trim(isnull(e.WhatId, ''))) > 0 and trim(isnull(e.WhatId, '')) <> '000000000000000AAA')) and len(trim(isnull(app.Id, ''))) > 0,  @NewLineChar + 'Application: ' + concat(isnull(app.Name, ''), ' (External ID: ', app.Id, ')'), '')

	, iif(((len(trim(isnull(t.WhatId, ''))) > 0 and trim(isnull(t.WhatId, '')) <> '000000000000000AAA')
		or (len(trim(isnull(e.WhatId, ''))) > 0 and trim(isnull(e.WhatId, '')) <> '000000000000000AAA')) and len(trim(isnull(inv.Id, ''))) > 0,  @NewLineChar + 'Interview: ' + concat(isnull(inv.Name, ''), ' (External ID: ', inv.Id, ')'), '')

))) as content

, 'comment' as category

--, 'company'
--, 'contact'
--, 'job'
, 'candidate'
--, 'application'
as type

, -10 as user_account_id

from EmailMessage x
left join Task t on t.Id = x.ActivityId
left join TaskRelation tr on tr.TaskId = t.Id
left join Event e on e.Id = x.ActivityId
left join EventRelation er on er.EventId = e.Id
left join Account com on com.Id = t.AccountId or com.Id = tr.AccountId or com.Id = e.AccountId or com.Id = er.AccountId
left join Contact con on con.Id = t.WhoId or con.Id = tr.RelationId or con.Id = e.WhoId or con.Id = er.RelationId
left join AVTRRT__Job__c job on job.Id = t.WhatId or job.Id = e.WhatId
left join AVTRRT__Job_Applicant__c app on app.Id = t.WhatId or app.Id = e.WhatId
left join AVTRRT__Interview__c inv on inv.Id = t.WhatId or inv.Id = e.WhatId
left join AVTRRT__Placement__c pla on pla.Id = t.WhatId or pla.Id = e.WhatId
left join [User] u on u.Id = t.OwnerId or u.Id = e.OwnerId
where
x.IsDeleted = 0 and
(
	(len(trim(isnull(t.AccountId, ''))) > 0 and trim(isnull(t.AccountId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(tr.AccountId, ''))) > 0 and trim(isnull(tr.AccountId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(e.AccountId, ''))) > 0 and trim(isnull(e.AccountId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(er.AccountId, ''))) > 0 and trim(isnull(er.AccountId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(t.WhoId, ''))) > 0 and trim(isnull(t.WhoId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(tr.RelationId, ''))) > 0 and trim(isnull(tr.RelationId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(e.WhoId, ''))) > 0 and trim(isnull(e.WhoId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(er.RelationId, ''))) > 0 and trim(isnull(er.RelationId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(t.WhatId, ''))) > 0 and trim(isnull(t.WhatId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(e.WhatId, ''))) > 0 and trim(isnull(e.WhatId, '')) <> '000000000000000AAA')
)
order by x.CreatedDate