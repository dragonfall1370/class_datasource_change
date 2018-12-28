-- Activities Comments
-- Contact Status History
-- Contact

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
x.ContactStatusHistoryID

, 'comment' as category

, 'contact' as [type]

, trim(u.Email) as UserEmail

, cast(x.ContactID as varchar(20)) as ConExtId

, x.CreationDate as insert_timestamp

, concat(
	upper('Contact Status History')
	, @NewLineChar
	, replicate('-', len('Contact Status History')*3)
	, concat(@NewLineChar, 'Status Date: ', convert(datetime, x.StatusDate, 120))
	, concat(@NewLineChar, 'Contact Status: ', trim(isnull(cs.Description, '')))
	, concat(@NewLineChar, 'Comments: ', trim(isnull(cast(x.Comments as nvarchar(max)), '')))
) as content

from

ContactStatusHistory x
left join ContactStatus cs on x.ContactStatusID = cs.ContactStatusID
left join Users u on x.CreatorID = u.UserID
left join VCConIdxs con on x.ContactID = con.ConId

order by x.CreationDate