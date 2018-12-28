-- Activities Comments
-- Notes
-- Company

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
x.NoteID

, 'comment' as category

, 'company' as [type]

, trim(u.Email) as UserEmail

, cast(x.CompanyID as varchar(20)) as ComExtId

, x.CreationDate as insert_timestamp

, concat(
	upper('Note')
	, @NewLineChar
	, replicate('-', len('Note')*3)
	, @NewLineChar
	, 'Details'
	, concat(@DoubleNewLine, 'Date: ', convert(datetime, x.CreationDate, 120))
	, concat(@NewLineChar, 'Title: ', trim(isnull(x.Title, '')))
	, concat(@NewLineChar, 'Type: ', trim(isnull(nt.Description, '')))
	, concat(@NewLineChar, 'Our Contact: ', iif(x.ContactId = 0, 'None Selected', con.FullName))
	, @DoubleNewLine
	, 'Relationships'
	, concat(@DoubleNewLine, 'Vacancy: ', iif(x.VacancyId = 0, 'None Selected', job.JobTitle))
	, concat(@NewLineChar, 'Candidate: ', iif(x.CandidateId = 0, 'None Selected', can.FullName))
	, @DoubleNewLine
	, 'Other Information'
	, concat(@DoubleNewLine, 'Description:', @DoubleNewLine, trim(isnull(cast(x.Description as nvarchar(max)), '')))
) as content

from Notes x
left join NoteTypes nt on x.NoteTypeID = nt.NoteTypeID
left join Users u on x.CreatorID = u.UserID
left join VCConIdxs con on x.ContactID = con.ConId
left join VCCanIdxs can on x.CandidateID = can.CanId
left join VCJobIdxs job on x.VacancyID = job.JobId

where
x.CompanyID <> 0
order by x.CreationDate


--select * from CompanyDetails
--where CompanyID = 214