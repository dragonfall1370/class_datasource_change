-- Activities Comments
-- Tasks

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
  x.Id

, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA'
	and len(trim(isnull(com.Id, ''))) > 0 and trim(isnull(com.Id, '')) <> '000000000000000AAA'
	, trim(isnull(x.WhatId, '')), null) as ComExtId

, iif(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA'
	and len(trim(isnull(con.Id, ''))) > 0 and trim(isnull(con.Id, '')) <> '000000000000000AAA'
	, trim(isnull(x.WhoId, '')), null) as ConExtId

, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA'
	and len(trim(isnull(job.Id, ''))) > 0 and trim(isnull(job.Id, '')) <> '000000000000000AAA'
	, trim(isnull(x.WhatId, '')), null) as JobExtId

, iif(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA'
	and len(trim(isnull(can.Id, ''))) > 0 and trim(isnull(can.Id, '')) <> '000000000000000AAA'
	,trim(isnull(x.WhoId, '')), null) as CanExtId

, cast(x.CreatedDate as datetime) as insert_timestamp

, concat(
upper('Task')
, @NewLineChar
, replicate('-', len('Task'))
, @NewLineChar
, trim(@NewLineChar from concat(
	'Assigned To: ' + concat(trim(isnull(ou.FirstName, '')), ' ', trim(isnull(ou.LastName, '')), ' (External ID:', ou.Id, ')')
	
	, @NewLineChar + 'Subject: ' + trim(isnull(x.Subject, ''))

	, iif(len(trim(isnull(convert(varchar(50), cast(x.ActivityDate as datetime), 111), ''))) > 0,  @NewLineChar + 'Due Date: ' + trim(isnull(convert(varchar(50), cast(x.ActivityDate as datetime), 111), '')), '')
	
	, @NewLineChar + 'Status: ' + trim(isnull(x.Status, ''))

	, iif(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA'
		and len(trim(isnull(con.Id, ''))) > 0 and trim(isnull(con.Id, '')) <> '000000000000000AAA'
		, @NewLineChar + 'Contact => ' + trim(isnull(con.Id, ''))
		, ''
	)
	, iif(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA'
		and len(trim(isnull(can.Id, ''))) > 0 and trim(isnull(can.Id, '')) <> '000000000000000AAA'
		, @NewLineChar + 'Candidate => ' + trim(isnull(can.Id, ''))
		, ''
	)

	, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA'
		and len(trim(isnull(com.Id, ''))) > 0 and trim(isnull(com.Id, '')) <> '000000000000000AAA'
		, @NewLineChar + 'Related To: Account => ' + trim(isnull(com.Id, ''))
		, ''
	)
	, iif(len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA'
		and len(trim(isnull(job.Id, ''))) > 0 and trim(isnull(job.Id, '')) <> '000000000000000AAA'
		, @NewLineChar + 'Related To: Opportunity => ' + trim(isnull(job.Id, ''))
		, ''
	)

	, iif(len(trim(isnull(x.EmailMessageId, ''))) > 0, @NewLineChar + 'Email: ' + trim(isnull(x.EmailMessageId, '')), '')

	, iif(len(trim(isnull(x.Description, ''))) > 0, @NewLineChar + 'Comments: ' + trim(isnull(x.Description, '')), '')

	, @NewLineChar + 'Priority: ' + trim(isnull(x.Priority, ''))

	, @NewLineChar + 'Closed: ' + iif(x.IsClosed = 1, 'Yes', 'No')

	, @NewLineChar + 'Archived: ' + iif(x.IsArchived = 1, 'Yes', 'No')

	, iif(len(trim(isnull(x.Type, ''))) > 0, @NewLineChar + 'Type: ' + trim(isnull(x.Type, '')), '')

	, iif(len(trim(isnull(x.CallDurationInSeconds, ''))) > 0, @NewLineChar + 'Call Duration (s): ' + trim(isnull(x.CallDurationInSeconds, '')), '')
))) as content

, 'comment' as category

, 'company'
--, 'contact'
--, 'job'
--, 'candidate'
as type

, ou.Username as UserEmail

from [Task] x
left join VCAccIdxs com on com.Id = x.WhatId
left join VCConIdxs con on con.Id = x.WhoId
left join VCJobIdxs job on job.Id = x.WhatId
left join VCCanIdxs can on can.Id = x.WhoId
left join [User] ou on ou.Id = x.OwnerId
where
x.IsDeleted = 0 and
(
	(len(trim(isnull(x.WhoId, ''))) > 0 and trim(isnull(x.WhoId, '')) <> '000000000000000AAA')
		or (len(trim(isnull(x.WhatId, ''))) > 0 and trim(isnull(x.WhatId, '')) <> '000000000000000AAA')
)
order by insert_timestamp