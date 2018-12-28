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