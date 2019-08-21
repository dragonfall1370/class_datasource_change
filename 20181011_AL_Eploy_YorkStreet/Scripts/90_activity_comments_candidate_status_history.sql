-- Activities Comments
-- Candidate Status History
-- Candidate

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
x.CandidateStatusHistoryID

, 'comment' as category

, 'candidate' as [type]

, trim(u.Email) as UserEmail

, cast(x.CandidateID as varchar(20)) as CanExtId

, x.CreationDate as insert_timestamp

, concat(
	upper('Candidate Status History')
	, @NewLineChar
	, replicate('-', len('Candidate Status History')*3)
	, concat(@NewLineChar, 'Status Date: ', convert(datetime, x.StatusDate, 120))
	, concat(@NewLineChar, 'Candidate Status: ', trim(isnull(cs.Description, '')))
	, concat(@NewLineChar, 'Comments: ', trim(isnull(cast(x.Comments as nvarchar(max)), '')))
) as content

from
CandidateStatusHistory x
left join EmploymentStatus cs on x.EmploymentStatusID = cs.EmploymentStatusID
left join Users u on x.CreatorID = u.UserID
left join VCCanIdxs can on x.CandidateID = can.CanId

order by x.CreationDate