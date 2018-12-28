-- Activities Comments
-- Call Logs
-- Company

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
x.TelephoneCallID

, 'comment' as category

, 'company' as [type]

, trim(u.Email) as UserEmail

, cast(x.CompanyID as varchar(20)) as ComExtId

, x.CreationDate as insert_timestamp

, concat(
	upper('Call Log')
	, @NewLineChar
	, replicate('-', len('Call Log')*3)
	, @NewLineChar
	, 'Details'
	, concat(@DoubleNewLine, 'Call Date: ', convert(datetime, x.CreationDate, 120))
	, concat(@NewLineChar, 'Outgoing / Incoming: ', iif(x.OutGoing = 1, 'Outgoing', 'Incoming'))
	, concat(@NewLineChar, 'Duration: ', x.Duration)
	, concat(@NewLineChar, 'Call Type: ', trim(isnull(ct.Description, '')))
	, concat(@NewLineChar, 'Outcome: ', iif(x.CallOutcomeID = 0, 'None Selected', trim(isnull(co.Description, ''))))
	, concat(@NewLineChar, 'Spoke To: ', iif(x.RecordTypeID = 1, 'Companies', 'Candidates'))
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
) as content

from TelephoneCalls x
left join callTypes ct on x.CallTypeID = ct.CallTypeID
left join CallOutcomes co on x.CallOutcomeID = co.CallOutcomeID
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