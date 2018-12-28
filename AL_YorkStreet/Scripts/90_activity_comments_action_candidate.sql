-- Activities Comments
-- Action
-- Candidate

declare @NewLineChar as char(2) = char(13) + char(10)
declare @DoubleNewLine as char(4) = @NewLineChar + @NewLineChar

select distinct
x.ActionID

, 'comment' as category

, 'candidate' as [type]

, trim(u.Email) as UserEmail

, cast(x.CandidateID as varchar(20)) as CanExtId

, x.CreationDate as insert_timestamp

, concat(
	upper('Action')
	, @NewLineChar
	, replicate('-', len('Action')*3)
	, @NewLineChar
	, 'Details'
	, concat(@DoubleNewLine, 'Subject: ', trim(isnull(x.Subject, '')))
	, concat(@NewLineChar, 'Start Date: ', convert(datetime, x.StartDate, 120))
	--, concat(@NewLineChar, 'All Day: ', iif(x.AllDay = 1, 'Outgoing', 'Incoming'))
	, concat(@NewLineChar, 'End Date: ', convert(datetime, x.EndDate, 120))
	--, concat(@NewLineChar, 'City (Timezone): ', iif(x.AllDay = 1, 'Outgoing', 'Incoming'))
	, concat(@NewLineChar, 'Completion Date: ', convert(datetime, x.CompletionDate, 120))
	, concat(@NewLineChar, 'Action Type: ', trim(isnull(ats.Description, '')))
	, concat(@NewLineChar, 'Outcome: ', iif(x.ActionOutcomeID = 0, 'None Selected', trim(isnull(ao.Description, ''))))
	, concat(@NewLineChar, 'Priority: ', iif(x.ActionOutcomeID = 0, 'None Selected', trim(isnull(ap.Description, ''))))
	, concat(@NewLineChar, 'User: ', trim(isnull(u.UserDisplayName, '')))
	, concat(@NewLineChar, 'Priority: ', iif(x.ActionOutcomeID = 0, 'None Selected', trim(isnull(ap.Description, ''))))
	, concat(@DoubleNewLine, 'Company Type: ', iif(x.BusinessAreaID = 0, 'None Selected', trim(isnull(ba.Description, ''))))
	--, concat(@DoubleNewLine, 'Notify: ', iif(x.NotifyInterval = 0, 'None Selected', trim(isnull(ba.Description, ''))))
	, concat(@NewLineChar, 'Notify by Email: ', iif(x.NotifyByEmail = 1, 'Yes', 'No'))
	, @DoubleNewLine
	, 'Relationships'
	, concat(@DoubleNewLine, 'Company: ', iif(x.CompanyId = 0, 'None Selected', com.ComName))
	, concat(@NewLineChar, 'Contact: ', iif(x.ContactId = 0, 'None Selected', con.FullName))
	, concat(@NewLineChar, 'Requires Confirmation from Contact: ', iif(x.RequiresContactConfirmation = 1, 'Yes', 'No'))
	--, concat(@NewLineChar, 'Project: ', iif(x.ProjectId = 0, 'None Selected', p.Name))
	, concat(@NewLineChar, 'Vacancy: ', iif(x.VacancyId = 0, 'None Selected', job.JobTitle))
	, concat(@NewLineChar, 'Candidate: ', iif(x.CandidateId = 0, 'None Selected', can.FullName))
	, concat(@NewLineChar, 'Requires Confirmation from Candidate: ', iif(x.RequiresCandidateConfirmation = 1, 'Yes', 'No'))
	, concat(@NewLineChar, 'Candidate Reference: ', iif(x.CandidateReferenceID = 0, 'None Selected'
		, concat(trim(isnull(cr.Referee, '')), ' - ', trim(isnull(cr.Email, '')), ' - ', trim(isnull(cr.Telephone, ''))))
	)
	, concat(@NewLineChar, 'Online Reference Requested: ', iif(x.CandidateReferenceDateSent is not null, 'Yes', 'No'))
	, concat(@NewLineChar, 'Online Reference Received: ', iif(x.RequiresContactConfirmation is not null, 'Yes', 'No'))
	, @DoubleNewLine
	, 'Location'
	, concat(@DoubleNewLine, 'Location: ', trim(isnull(x.Location, '')))
	, concat(@NewLineChar, 'Other Attendees: ', trim(isnull(x.OtherAttendees, '')))
	, @DoubleNewLine
	, 'Other Information'
	, concat(@DoubleNewLine, 'Private: ', iif(x.Private = 1, 'Yes', 'No'))
	, concat(@NewLineChar, 'Comments:', @DoubleNewLine, trim(isnull(cast(x.Comments as nvarchar(max)), '')))
) as content

from Actions x
left join ActionTypes ats on x.ActionTypeID = ats.ActionTypeID
left join ActionOutcomes ao on x.ActionOutcomeID = ao.ActionOutcomeID
left join ActionPriorities ap on x.ActionPriorityID = ap.ActionPriorityID
left join ActionConfirmationStatus acsu on x.UserConfirmationStatusID = acsu.ActionConfirmationStatusID
left join ActionConfirmationStatus acsca on x.CandidateConfirmationStatusID = acsca.ActionConfirmationStatusID
left join ActionConfirmationStatus acsco on x.ContactConfirmationStatusID = acsco.ActionConfirmationStatusID
left join BusinessAreas ba on x.BusinessAreaID = ba.BusinessAreaID
left join Users u on x.CreatorID = u.UserID
left join VCComIdxs com on x.CompanyID = com.ComId
left join VCConIdxs con on x.ContactID = con.ConId
left join VCJobIdxs job on x.VacancyID = job.JobId
left join VCCanIdxs can on x.CandidateID = can.CanId
--left join Projects p on x.ProjectId = p.ProjectID
left join CandidateReferences cr on x.CandidateReferenceID = cr.CandidateReferenceID

where
x.CandidateID <> 0
order by x.CreationDate

--select * from Industries
--where ParentIndustryID = 0

--select * from EPloySubjects

