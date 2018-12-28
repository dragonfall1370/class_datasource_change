--drop table if exists #VCJobAppsTmp1

--select
--v.VacancyID as JobId
--, v.Title as JobTitle
--, iif(v.VacancyTypeID = 0
--	, ''
--	, trim(isnull(vt.Description, ''))
--) as JobType

--into #VCJobAppsTmp1

--from
--VacancyApplications va
--left join Vacancies v on va.VacancyID = v.VacancyID
--left join VacancyTypes vt on v.VacancyTypeID = vt.VacancyTypeID
--where va.VacancyHistoryID <> 0

--select * from #VCJobAppsTmp1

drop table if exists VCJobApplications

select 
x.VacancyID as [application-positionExternalId]
, x.CandidateID as [application-candidateExternalId]
, iif(x.VacancyHistoryID <> 0,
	case(lower(trim(isnull(vt.Description, ''))))
		when lower('Permanent') then 'PLACEMENT_PERMANENT'
		when lower('Contract') then 'PLACEMENT_CONTRACT'
		when lower('Part Time') then 'PLACEMENT_TEMP'
		when lower('Temp') then 'PLACEMENT_TEMP'
		else 'PLACEMENT_PERMANENT'
	end
	, iif(x.ApplicationStatusID <> 0
		, 'SHORTLISTED'
		, iif(
			x.ActionID <> 0
			
			, case(lower(trim(isnull(ats2.Description, ''))))
				when lower('To-Do') then 'SHORTLISTED'
				when lower('Appointment') then 'SHORTLISTED'
				when lower('To-Phone') then 'SHORTLISTED'
				when lower('First Interview') then 'FIRST_INTERVIEW'
				when lower('Second Interview') then 'SECOND_INTERVIEW'
				when lower('Offer Made') then 'OFFERED'
				when lower('Thomas Assessment') then 'SHORTLISTED'
				when lower('Shortlisted') then 'SHORTLISTED'
				when lower('Client Meeting') then 'SHORTLISTED'
				when lower('CV Sent') then 'SENT'
				when lower('Xmas Card') then 'SHORTLISTED'
				when lower('Interview Slot') then 'SHORTLISTED'
				else 'SHORTLISTED'
			end
			
			, case(lower(trim(isnull(ats.Description, ''))))
				when lower('To-Do') then 'SHORTLISTED'
				when lower('Appointment') then 'SHORTLISTED'
				when lower('To-Phone') then 'SHORTLISTED'
				when lower('First Interview') then 'FIRST_INTERVIEW'
				when lower('Second Interview') then 'SECOND_INTERVIEW'
				when lower('Offer Made') then 'OFFERED'
				when lower('Thomas Assessment') then 'SHORTLISTED'
				when lower('Shortlisted') then 'SHORTLISTED'
				when lower('Client Meeting') then 'SHORTLISTED'
				when lower('CV Sent') then 'SENT'
				when lower('Xmas Card') then 'SHORTLISTED'
				when lower('Interview Slot') then 'SHORTLISTED'
				else 'SHORTLISTED'
			end
		)
	)
) as [application-stage]

into VCJobApplications

from VacancyApplications x
left join Vacancies v on x.VacancyId = v.VacancyId
left join VacancyTypes vt on v.VacancyTypeID = vt.VacancyTypeID
left join ActionTypes ats on x.ApplicationStageID = ats.ActionTypeID
left join Actions a on x.ActionID = a.ActionID
left join ActionTypes ats2 on a.ActionTypeID = ats2.ActionTypeID
order by x.ApplicationDate, x.VacancyID, x.CandidateID

select * from VCJobApplications
--where [application-stage] <> 'SHORTLISTED'
--where [application-stage] like '%placement%'
--where [application-stage] like '%offer%'
--where len(isnull([application-stage], '')) > 0

--select VacancyId, Title from Vacancies
--where VacancyId = 39

--select * from VCJobApplications
--where [application-positionExternalId] = 39

--select * from VacancyApplications
--where VacancyID = 39

--select * from ActionTypes

--select * from Actions
--where ActionID in (929, 736)

--select * from Vacancies
--where VacancyID in (
--select VacancyID
--from VacancyApplications
--where VacancyHistoryID > 0
--)

----select * from (
--select
----va.VacancyApplicationID,
--va.VacancyID as [application-positionExternalId]
--, va.CandidateID as [application-candidateExternalId]
--, [dbo].ufn_ConvertApplicationStageYS2VC(
--	iif(
--		va.ApplicationStatusID <> 0
--		, isnull((select top 1 vas.Description from VacancyApplicationStatus vas where vas.VacancyApplicationStatusID = va.ApplicationStatusID), '')
--		, iif(
--			va.ApplicationStageID <> 0
--			, isnull(
--				(select top 1 at.Description from ActionTypes at where at.ActionTypeID = va.ApplicationStageID)
--				, iif(
--					va.VacancyHistoryID <> 0
--					, 'Placed'
--					,''
--				)
--			)
--			, iif(
--				va.VacancyHistoryID <> 0
--				, 'Placed'
--				,''
--			)
--		)
--	)
--)
--as [application-stage]
--from VacancyApplications va
----left join Actions a on va.ActionID = a.ActionID
----left join ActionTypes at on va.ApplicationStageID = at.ActionTypeID or va.ApplicationSubStageID = at.ActionTypeID
----left join VacancyApplicationStatus vas on va.ApplicationStatusID = vas.VacancyApplicationStatusID
--where va.ApplicationStatusID not in (
--	1 -- Not Suitable
--	, 6 -- Withdrawn
--	, 7 -- Rejected by Client
--)
--order by va.CreationDate
--) abc where
--[application-candidateExternalId] = 107
--abc.[application-stage] =
--''
--'PLACED'

--select
--va.VacancyID as [application-positionExternalId]
--, va.CandidateID as [application-candidateExternalId]
--, '???' as [application-stage]
--, at.Description as ActionType
--, vas.Description ApplicationStatus
--, va.ApplicationStageID
--, va.ApplicationSubStageID
--, va.ApplicationStatusID
--, com.Name as Company
--, con.FirstName + ' ' + con.Surname as Contact
--, can.FirstName + ' ' + can.Surname as Candidate
--, va.ActionID
--, va.ActionOutcomeID
--, va.VacancyHistoryID
--, va.VacancyHistoryStatusID
--, ao.Description as ActionOutcome
--, ap.Description as ActionPriority
--, acs.Description ActionConfirmationStatus
--, a.*
--from
--VacancyApplications va
--left join ActionOutcomes ao on va.ActionOutcomeID = ao.ActionOutcomeID
--left join Actions a on va.ActionID = a.ActionID
--left join ActionTypes at on a.ActionTypeID = at.ActionTypeID
--left join ActionPriorities ap on a.ActionPriorityID = ap.ActionPriorityID
--left join ActionConfirmationStatus acs on
--	acs.ActionConfirmationStatusID = a.CandidateConfirmationStatusID
--	or acs.ActionConfirmationStatusID = a.ContactConfirmationStatusID
--	or acs.ActionConfirmationStatusID = a.ContactConfirmationStatusID
--left join CompanyDetails com on a.CompanyID = com.CompanyID
--left join Contacts con on a.ContactID = con.ContactID
--left join Candidates can on a.CandidateID = can.CandidateID
--left join VacancyApplicationStatus vas on
--	va.ApplicationStageID = vas.VacancyApplicationStatusID
--	or va.ApplicationSubStageID = vas.VacancyApplicationStatusID
--	or va.ApplicationStatusID = vas.VacancyApplicationStatusID
----where va.ActionID <> 0
--order by va.VacancyID, va.CandidateID

--select * from VacancyApplications

--select * from StoredFilePaths sfp
--where sfp.StoredFilePathID in (select va.CVID from VacancyApplications va)

--select top 1 string_agg(sfp.FileName, ',')
--		from
--		StoredFilePaths sfp
--		join RecordTypes rt on sfp.RecordTypeID = rt.RecordTypeID
--		where rt.Description like '%Cover Letters%'-- and sfp.RecordID = v.VacancyID
--		group by sfp.RecordID

--select count(*) from VacancyApplications where CVID <> 0

--select * from RecordTypes

--'Cover Letters'