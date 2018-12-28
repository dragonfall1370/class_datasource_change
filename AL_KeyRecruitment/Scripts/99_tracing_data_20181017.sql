select
c.candidateID
, c.phone
, c.phone2
, c.phone3
, c.workPhone
, c.mobile
, trim( ' ,' from
	concat_ws(
		','
		, nullif(iif(trim(isnull(c.mobile, '')) = '0', '', trim(isnull(c.mobile, ''))), '')
		, nullif(trim(isnull(c.phone, '')), '')
		, nullif(trim(isnull(c.phone2, '')), '')
		, nullif(trim(isnull(c.phone3, '')), '')
		, nullif(trim(isnull(c.workPhone, '')), '')
	)
) as [candidate-phone]
from bullhorn1.Candidate C

select count(*) from bullhorn1.Candidate x
where x.isDeleted = 0
and x.isPrimaryOwner = 1

--select * from bullhorn1.BH_ClientCorporationRatios

--USE [BiGGGroup]
--GO

SELECT [CCorpRatioID]
      ,[clientCorporationID]
      ,[CorpName]
      ,[NumJobs]
      ,[NumJobs_Year]
      ,[InterViews]
      ,[InterViews_Year]
      ,[Submissions]
      ,[Submissions_Year]
      ,[Placements]
      ,[Placements_Year]
      ,[YTD_SubtoInterview_Ratio]
      ,[SubtoInterview_Ratio]
      ,[FillRatio]
      ,[FillRatio_Year]
      ,[YTD_InterviewtoPlacement_Ratio]
      ,[InterviewtoPlacement_Ratio]
  FROM [bullhorn1].[BH_ClientCorporationRatios]
  order by clientCorporationID
GO

select * from [bullhorn1].View_Opportunity -- empty

select * from [bullhorn1].BH_JobOpportunity

select * from [bullhorn1].BH_OpportunityHistory -- empty

select * from [bullhorn1].BH_ShortListOpportunity -- empty

select
x.clientCorporationID
, x.ownership
from bullhorn1.BH_ClientCorporation x

select * from [bullhorn1].BH_User

select top 100 * from bullhorn1.BH_User
--where [name] like '%Public%'

select top 100 * from bullhorn1.BH_UserContact

--select * from bullhorn1.BH_UserType
--select * from bullhorn1.BH_UserTypeLocal

select * from bullhorn1.BH_ClientCorporation

select * from bullhorn1.BH_FieldMapList
select * from bullhorn1.BH_FieldMap
select * from bullhorn1.BH_FieldMapInteraction

select
--[ownership]
*
from bullhorn1.BH_ClientCorporation

select * from bullhorn1.BH_Client
where clientCorporationID in
(select clientCorporationID from bullhorn1.BH_ClientCorporation)

select * from bullhorn1.BH_User
where userID in (
	select UserID from bullhorn1.BH_Client
)

select * from bullhorn1.BH_UserContact
where userID in (
	select recruiterUserID from bullhorn1.BH_Client
)

select email, email_old, email2, email3, externalEmail from bullhorn1.BH_UserContact

select count(*) from VCCons
where [contact-companyId] not in
(select [company-externalId] from VCComs)


DECLARE @RC int
DECLARE @SearchStr nvarchar(100)

-- TODO: Set parameter values here.
SET @SearchStr =
--'HSBC Global Markets & Banking'
--'Offer Made' -- [dbo].[ActionTypes].[Description]
--'Second Interview' -- [dbo].[ActionTypes].[Description]
--'Long Lists'
--'clientCorporationRatios'
'description_truong'
EXECUTE @RC = [dbo].[usp_SearchTextInAllTables]
   @SearchStr
GO

select count(*) from BULLHORN1.BH_UserMessage

select max(x.userMessageID) from BULLHORN1.BH_UserMessage x

select count(*)
--x.userMessageID
--, x.email_content
from BULLHORN1.BH_UserMessage x
where len(isnull(x.email_content, '')) = 0

--select 714082 - 637805
--76277

select
--distinct
--[type]
*
from bullhorn1.Candidate

select
distinct
--*
x.status
from bullhorn1.BH_JobResponse x

select
--*
x.status
from bullhorn1.BH_JobResponse x
where x.status = 'New Lead' or x.status = 'Match Job Spec'
-- 102129/107291

select count([company-externalID]) IdCount, count(distinct [company-name]) nameCount from VCComs

select count([candidate-externalId]) IdCount, count(distinct [candidate-email]) emailCount from VCCans

select count(*) from VCCons where [contact-companyId] not in
(
	select [company-externalId] from VCComs
)

select count(*) from VCJobs
where [position-contactId] is null or [position-contactId] not in (
	select [contact-externalId] from VCCons
)

select count(*) from VCApplications
where [application-positionExternalId] not in (
	select [position-externalId] from VCJobs
)

select
count(*)
--*
from VCApplications
where [application-candidateExternalId] not in (
	select [candidate-externalId] from VCCans-- where isDeleted = 0
)

select
--count(*)
*
from bullhorn1.Candidate
where candidateID in (
25265
,2207
,5706
,167
,167
,167
,167
,6578
,6578
,6578
,13223
,5707
)

select count(*) from bullhorn1.Candidate x where x.isDeleted = 1