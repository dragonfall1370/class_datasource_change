drop table if exists [dbo].[VC_Applications1]
go

select
x.[ClientId]
, y.ComName
, x.[JobId]
, x.[ClientContactId]
, x.[ApplicantId]
, x.[ApplicantActionStatus_StatusIdDescription]
, x.[ApplicantActionStatus_StatusIdPos]
, x.[ApplicantActionId]
, iif(lower(trim(isnull(x.ApplicantActionStatus_StatusIdDescription, ''))) = lower('1-SendCV'), x.StatusDate, null) as ShortlistedDate
, iif(lower(trim(isnull(x.ApplicantActionStatus_StatusIdDescription, ''))) = lower('2-CVSent'), x.StatusDate, null) as SentDate
, iif(lower(trim(isnull(x.ApplicantActionStatus_StatusIdDescription, ''))) = lower('4-Interview'), x.StatusDate, null) as FirstInterviewDate
, iif(lower(trim(isnull(x.ApplicantActionStatus_StatusIdDescription, ''))) = lower('5-ReInterview'), x.StatusDate, null) as SecondInterviewDate
, iif(lower(trim(isnull(x.ApplicantActionStatus_StatusIdDescription, ''))) = lower('7-OfferAccept'), x.StatusDate, null) as OfferedDate
, iif(lower(trim(isnull(x.ApplicantActionStatus_StatusIdDescription, ''))) = lower('7-OfferAccept'), x.StatusDate, null) as PlacedDate
, x.CreatedOn
,[Placements_PlacementIdInvoiceContactID]
,[Placements_PlacementIdPlacementTypeId]
,[Placements_PlacementIdStartDate]
,[Placements_PlacementIdEndDate]
,[Placements_PlacementIdInvoiceAddress]
,[Placements_PlacementIdWorkAddress]
,[Placements_PlacementIdCurrencyId]
,[Placements_PlacementIdSalary]
,[Placements_PlacementIdCommissionPerc]
,[Placements_PlacementIdPlacementFee]
,[Placements_PlacementIdDescription]
,[Placements_PlacementIdPositionAttributeId]
,[Placements_PlacementIdStartCheckOK]
,[Placements_PlacementIdHSManagerId]
,[Placements_PlacementIdDiaryEventId]
,[Placements_PlacementIdClientHirerLegalEntityId]
,[Users_ConsultantEmailAddress]

into [dbo].[VC_Applications1]

from [VC_JobApplications] x
left join (select [company-externalId] as ComId, [company-name] as ComName from VC_Com) y on cast(x.ClientId as varchar(20)) = y.ComId
--left join (select ApplicantActionStatusId as StageId, Description as Stage from ApplicantActionStatus) z on x.StatusId = z.StageId
where Archived = 'N'
and lower(trim(isnull(x.ApplicantActionStatus_StatusIdDescription, ''))) in (
	lower('1-SendCV')
	, lower('2-CVSent')
	, lower('4-Interview')
	, lower('5-ReInterview')
	, lower('7-OfferAccept')
)
go
--select * from VC_Applications1 -- 767096

--select distinct -- 698579 -- 701139
--ClientId
--, JobId
--, ClientContactId
--, ApplicantId
--, ApplicantActionStatus_StatusIdDescription
----, ApplicantActionId

--from VC_Applications1
--order by
--ClientId
--, JobId
--, ClientContactId
--, ApplicantId
--, ApplicantActionStatus_StatusIdDescription

-------------------------------------------764318
drop table if exists [dbo].[VC_Applications2]
go

SELECT
x.[ClientId]
, x.[ComName]
, x.[JobId]
, x.[ClientContactId]
, x.[ApplicantId]
, x.[ApplicantActionStatus_StatusIdDescription] as LatestStage
, x.[ApplicantActionId]
, x.ShortlistedDate
, x.SentDate
, x.FirstInterviewDate
, x.SecondInterviewDate
, x.OfferedDate
, x.PlacedDate
, x.CreatedOn
,[Placements_PlacementIdInvoiceContactID]
,[Placements_PlacementIdPlacementTypeId]
,[Placements_PlacementIdStartDate]
,[Placements_PlacementIdEndDate]
,[Placements_PlacementIdInvoiceAddress]
,[Placements_PlacementIdWorkAddress]
,[Placements_PlacementIdCurrencyId]
,[Placements_PlacementIdSalary]
,[Placements_PlacementIdCommissionPerc]
,[Placements_PlacementIdPlacementFee]
,[Placements_PlacementIdDescription]
,[Placements_PlacementIdPositionAttributeId]
,[Placements_PlacementIdStartCheckOK]
,[Placements_PlacementIdHSManagerId]
,[Placements_PlacementIdDiaryEventId]
,[Placements_PlacementIdClientHirerLegalEntityId]
,[Users_ConsultantEmailAddress]

into [dbo].[VC_Applications2]

FROM
[dbo].[VC_Applications1] x
INNER JOIN 
(
    SELECT
	[ClientId]
	, [JobId]
	, [ClientContactId]
	, [ApplicantId]
    , Max([ApplicantActionStatus_StatusIdPos]) as LatestStage
    FROM
	[dbo].[VC_Applications1]
    GROUP BY [ClientId], [JobId], [ClientContactId], [ApplicantId]
	--order by [ApplicantId], [ClientContactId], [ClientId]
) AS y
ON x.[ClientId] = y.[ClientId] and isnull(x.[JobId], -1) = isnull(y.[JobId], -1) and x.[ClientContactId] = y.[ClientContactId] and x.[ApplicantId] = y.[ApplicantId]
AND x.[ApplicantActionStatus_StatusIdPos] = y.LatestStage
order by
--x.[ApplicantActionId]
x.[ClientId]
, x.[JobId]
, x.[ClientContactId]
, x.[ApplicantId]
----, x.[ApplicantActionStatus_StatusIdDescription]
go
-------------------------
drop table if exists [dbo].[VC_Applications3]
go

select *

into [dbo].[VC_Applications3]

from (
	select
	x.[ClientId]
	, x.[ComName]
	, x.[JobId]
	, x.[ClientContactId]
	, x.[ApplicantId]
	, x.[LatestStage]
	, x.ApplicantActionId
	, x.ShortlistedDate
	, x.SentDate
	, x.FirstInterviewDate
	, x.SecondInterviewDate
	, x.OfferedDate
	, x.PlacedDate
	, x.CreatedOn
	,[Placements_PlacementIdInvoiceContactID]
	,[Placements_PlacementIdPlacementTypeId]
	,[Placements_PlacementIdStartDate]
	,[Placements_PlacementIdEndDate]
	,[Placements_PlacementIdInvoiceAddress]
	,[Placements_PlacementIdWorkAddress]
	,[Placements_PlacementIdCurrencyId]
	,[Placements_PlacementIdSalary]
	,[Placements_PlacementIdCommissionPerc]
	,[Placements_PlacementIdPlacementFee]
	,[Placements_PlacementIdDescription]
	,[Placements_PlacementIdPositionAttributeId]
	,[Placements_PlacementIdStartCheckOK]
	,[Placements_PlacementIdHSManagerId]
	,[Placements_PlacementIdDiaryEventId]
	,[Placements_PlacementIdClientHirerLegalEntityId]
	,[Users_ConsultantEmailAddress]
	, row_number() over(partition by x.[ClientId], x.[JobId], x.[ClientContactId], x.[ApplicantId], x.[LatestStage] order by x.ApplicantActionId desc) rn
	from VC_Applications2 x
) y
where y.rn = 1

go
--select count(*) from [dbo].[VC_Applications3] -- 698579
--select max(ApplicantActionId) from [dbo].[VC_Applications3]
--select top 1000 * from [dbo].[VC_Applications3]
--where JobId is not null
--and JobId in (select [position-externalID] from VC_Job)

-------------------------
--select
----top 100
--*
--from VC_Job
--where [position-title] like 'Floated Candidate - %'

--select distinct [position-type] from VC_Job
-------------------------
--insert into VC_Job -- 697991
--select
--x.ClientContactId
--, 9999 + x.ApplicantActionId
--, concat('Floated Candidate - ', x.ComName, ' - Job - ', x.ApplicantActionId) 
--, 1
--, 'freddie@scopepersonnel.co.uk'
--, 'PERMANENT'
--, 'GBP'
--, 0.00
--, convert(varchar(10), x.CreatedOn, 120)
--, convert(varchar(10), dateadd(MONTH, 12, getdate()), 120)
--, ''
--, ''
--from [dbo].[VC_Applications3] x
--where JobId is null

--update [dbo].[VC_Applications3]
--set JobId = 9999 + ApplicantActionId
--where JobId is null

--  select * from VC_JobApplications where StatusId = 34


--  select * from ApplicantActionStatus

--  ApplicantActionStatusId	SystemCode	Description	Pos
--27	APP_ACT_STT_HOLD	Hold	0
--28	APP_ACT_STT_SENDCV	1-SendCV	1
--29	APP_ACT_STT_CVSENT	2-CVSent	2
--30	APP_ACT_STT_WITHDREW	Withdrew	4
--31	APP_ACT_STT_REJECT	3-Reject	3
--32	APP_ACT_STT_INTERVIEW	4-Interview	4
--33	APP_ACT_STT_REINTERVIEW	5-ReInterview	5
--34	APP_ACT_STT_INTOFFER	6-IntOffer	6
--35	APP_ACT_STT_OFFACCEPT	7-OfferAccept	7
--36	APP_ACT_STT_INTREJECT	8-IntReject	8
--37	APP_ACT_STT_OFFREJECT	9-OfferReject	9