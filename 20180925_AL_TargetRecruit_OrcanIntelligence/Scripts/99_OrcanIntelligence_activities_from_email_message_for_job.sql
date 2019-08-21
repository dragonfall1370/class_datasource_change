-- Activities Comments
-- EmailMessage

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
--'company'
--'contact'
'job'
--'candidate'
--'application'
as [type]

, 'comment' as category

, x.CreatedDate as insert_timestamp

, x.Id

, trim(isnull(job.Id, '')) as JobExtId

, u.Username as UserEmail

, concat(
upper('Email Message')
, @NewLineChar
, replicate('-', len('Email Message'))
, @NewLineChar
, trim(@NewLineChar from concat(
	@DoubleNewLine + 'Related To: ' + concat('Job #', trim(isnull(job.Name, '')), ' (', job.Id, ')')
	
	, @NewLineChar + 'Message Date: ' + convert(varchar(50), x.MessageDate, 111)

	, @NewLineChar + 'Created By: ' + concat(trim(isnull(u.FirstName, '')), ' ', trim(isnull(u.LastName, '')), ' (', u.Username, ')')

	, @NewLineChar + 'Status: ' +
		case(x.Status)
			when 0 then 'New'
			when 1 then 'Read'
			when 2 then 'Replied'
			when 3 then 'Sent'
			when 4 then 'Forwarded'
			when 5 then 'Draft'
		end

	, @NewLineChar + 'From Address: ' + trim(isnull(x.FromAddress, ''))

	, @NewLineChar + 'From Name: ' + trim(isnull(x.FromName, ''))

	, @NewLineChar + 'To Address: ' + trim(isnull(x.ToAddress, ''))

	, iif(len(trim(isnull(x.CcAddress, ''))) > 0, @NewLineChar + 'Cc Address: ' + trim(isnull(x.CcAddress, '')), '')

	, iif(len(trim(isnull(x.BccAddress, ''))) > 0, @NewLineChar + 'Bcc Address: ' + trim(isnull(x.BccAddress, '')), '')

	, @DoubleNewLine + 'Subject:' + @NewLineChar + trim(isnull(x.Subject, ''))

	, @DoubleNewLine + 'Text Body:' + @DoubleNewLine + trim(isnull(x.TextBody, ''))
))) as content

from [EmailMessage] x
--join Account com on com.Id = x.RelatedToId
--left join Contact con on con.Id = x.WhoId or con.Id = rel.RelationId
join AVTRRT__Job__c job on job.Id = x.RelatedToId
--left join AVTRRT__Job_Applicant__c app on app.Id = x.WhatId
--left join AVTRRT__Interview__c inv on inv.Id = x.WhatId
--left join AVTRRT__Placement__c pla on pla.Id = x.WhatId
left join [User] u on u.Id = x.CreatedById
where
x.IsDeleted = 0 and
(
	len(trim(isnull(RelatedToId, ''))) > 0
	and trim(isnull(RelatedToId, '')) <> '000000000000000AAA'
	-- 11965
	--and trim(isnull(RelatedToId, '')) in (
	--	select Id from
	--	Account -- 4
	--	--Contact where RecordTypeId = '012b0000000J2RE' -- 0
	--	--Contact where RecordTypeId = '012b0000000J2RD' -- 0
	--	--AVTRRT__Job__c -- 11961
	--)
)
order by x.CreatedDate