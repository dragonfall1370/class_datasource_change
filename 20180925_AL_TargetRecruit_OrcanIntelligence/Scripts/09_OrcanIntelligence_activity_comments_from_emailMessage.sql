-- Activities Comments
-- Email Message

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
  x.Id
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

	, iif(len(trim(isnull(x.Subject, ''))) > 0, @NewLineChar + 'Subject:' + @NewLineChar + trim(isnull(x.Subject, '')), '')

	, iif(len(trim(isnull(x.TextBody, ''))) > 0, @NewLineChar + 'Text Body:' + @DoubleNewLine + trim(isnull(x.TextBody, '')), '')
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
left join [User] u on x.CreatedById = u.Id

