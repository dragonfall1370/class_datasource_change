-- Activities Comments
-- Correspondence
-- Company

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
x.CorrespondenceID

, 'comment' as category

, 'company' as [type]

, trim(u.Email) as UserEmail

, cast(x.CompanyID as varchar(20)) as ComExtId

, x.CreationDate as insert_timestamp

, concat(
	upper('Correspondence')
	, @NewLineChar
	, replicate('-', len('Correspondence')*3)
	, @NewLineChar
	, 'Details'
	, concat(@DoubleNewLine, '*Date Sent: ', convert(datetime, x.CreationDate, 120))
	, concat(@NewLineChar, 'Outgoing / Incoming: ', iif(x.OutGoing = 1, 'Outgoing', 'Incoming'))
	, concat(@NewLineChar, 'Sent: ', iif(x.DateSent is not null, 'Yes', 'No'))
	, concat(@NewLineChar, 'Title: ', trim(isnull(x.Title, '')))
	, concat(@NewLineChar, 'Type: ', trim(isnull(ct.Description, '')))
	, concat(@NewLineChar, 'Merge To: ', iif(x.RecordTypeID = 1, 'Companies', 'Candidates'))
	, concat(@NewLineChar, 'User: ', trim(isnull(u.UserDisplayName, '')))
	, @DoubleNewLine
	, 'Relationships'
	, concat(@DoubleNewLine, 'Company: ', iif(x.CompanyId = 0, 'None Selected', com.ComName))
	, concat(@NewLineChar, 'Contact: ', iif(x.ContactId = 0, 'None Selected', con.FullName))
	, concat(@NewLineChar, 'Vacancy: ', iif(x.VacancyId = 0, 'None Selected', job.JobTitle))
	, concat(@NewLineChar, 'Candidate: ', iif(x.CandidateId = 0, 'None Selected', can.FullName))
	, @DoubleNewLine
	, 'Other Information'
	, concat(@DoubleNewLine, 'Comments:', @DoubleNewLine, trim(isnull(cast(x.Comments as nvarchar(max)), '')))
	, @DoubleNewLine
	, 'Email Merge Content'
	, @DoubleNewLine
	, replace(cast(MergeContent as nvarchar(max)), '{EmailBody}', cast(EmailBody as nvarchar(max)))
) as content

from Correspondence x
left join CorrespondenceTypes ct on x.CorrespondenceTypeID = ct.CorrespondenceTypeID
left join Users u on x.CreatorID = u.UserID
left join VCComIdxs com on x.CompanyID = com.ComId
left join VCConIdxs con on x.ContactID = con.ConId
left join VCJobIdxs job on x.VacancyID = job.JobId
left join VCCanIdxs can on x.CandidateID = can.CanId

where
x.CompanyID <> 0
order by x.CreationDate


--select * from CompanyDetails
--where CompanyID = 214