-- Activities Comments
-- Notes

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
  x.Id as NoteExtId

, iif(len(trim(isnull(x.AccountId, ''))) > 0 and trim(isnull(x.AccountId, '')) <> '000000000000000AAA'
	and len(trim(isnull(com.Id, ''))) > 0 and trim(isnull(com.Id, '')) <> '000000000000000AAA'
	, trim(isnull(x.AccountId, '')), null) as ComExtId

, iif(len(trim(isnull(x.ParentId, ''))) > 0 and trim(isnull(x.ParentId, '')) <> '000000000000000AAA'
	and len(trim(isnull(con.Id, ''))) > 0 and trim(isnull(con.Id, '')) <> '000000000000000AAA'
	, trim(isnull(x.ParentId, '')), null) as ConExtId

, iif(len(trim(isnull(x.ParentId, ''))) > 0 and trim(isnull(x.ParentId, '')) <> '000000000000000AAA'
	and len(trim(isnull(job.Id, ''))) > 0 and trim(isnull(job.Id, '')) <> '000000000000000AAA'
	, trim(isnull(x.ParentId, '')), null) as JobExtId

, iif(len(trim(isnull(x.ParentId, ''))) > 0 and trim(isnull(x.ParentId, '')) <> '000000000000000AAA'
	and len(trim(isnull(can.Id, ''))) > 0 and trim(isnull(can.Id, '')) <> '000000000000000AAA'
	,trim(isnull(x.ParentId, '')), null) as CanExtId

, cast(x.CreatedDate as datetime) as insert_timestamp
, concat(
upper('Notes')
, @NewLineChar
, replicate('-', len('Notes'))
, @NewLineChar
, trim(@NewLineChar from concat(
	iif(len(trim(isnull(x.IsPrivate, ''))) > 0, @NewLineChar + 'Private: ' + iif(trim(isnull(x.IsPrivate, '')) = '0', 'No', 'Yes'), '')
	, iif(len(trim(isnull(x.Title, ''))) > 0, @NewLineChar + 'Title: ' + trim(isnull(x.Title, '')), '')
	, iif(len(trim(isnull(x.Body, ''))) > 0,  @NewLineChar + 'Body:' + @DoubleNewLine + trim(isnull(x.Body, '')), '')
))) as content
, 'comment' as category
, 'company' as type
--, 'contact' as type
--, 'job' as type
--, 'candidate' as type
, u.Username as UserEmail
from Note x
left join VCAccIdxs com on com.Id = x.AccountId
left join VCConIdxs con on con.Id = x.ParentId
left join VCCanIdxs can on can.Id = x.ParentId
left join VCJobIdxs job on job.Id = x.ParentId
--left join AVTRRT__Job__c job on job.Id = x.ParentId
left join [User] u on u.Id = x.OwnerId
where
x.IsDeleted = 0 and (
	(len(trim(isnull(x.AccountId, ''))) > 0 and trim(isnull(x.AccountId, '')) <> '000000000000000AAA')
	or (len(trim(isnull(x.ParentId, ''))) > 0 and trim(isnull(x.ParentId, '')) <> '000000000000000AAA')
)
order by insert_timestamp