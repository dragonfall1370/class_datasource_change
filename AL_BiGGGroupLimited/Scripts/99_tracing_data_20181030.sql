/****** Script for SelectTopNRows command from SSMS  ******/
SELECT [fieldMapID]
      ,[privateLabelID]
      ,[entity]
      ,[columnName]
      ,[display]
      ,[editType]
      ,[isRequired]
      ,[isHidden]
      ,[valueList]
      ,[allowMultiple]
      ,[description]
      ,[hint]
      ,[sortOrder]
      ,[limit]
      ,[defaultValue]
      ,[dateLastModified]
      ,[modifyingMasterUserID]
      ,[displayList]
      ,[someField]
      ,[isDescending]
      ,[dateAdded]
  FROM [BiGGGroup].[bullhorn1].[BH_FieldMapList]

  where columnName like '%num%'


  select distinct [company-name] from VCComs

select distinct [candidate-email] from VCCans

select count(*) from VCCons where [contact-companyId] not in
(
	select [company-externalId] from VCComs
)


select distinct [type] from bullhorn1.Candidate

select candidateID, [type] from bullhorn1.Candidate
where lower(trim(isnull([type], ''))) = lower('Perm & Contract')

select bullhorn1.bhfn_DecryptString(convert(nvarchar(max), convert(varbinary, x.commentsCompressed)), 0) as abc from bullhorn1.BH_UserMessage x

select comments from bullhorn1.BH_UserMessage x
where comments is not null

select commentsCompressed from bullhorn1.BH_UserMessage x
where commentsCompressed is not null

select distinct employmentType from bullhorn1.BH_JobPosting

select count(*) from VCJobs
where [position-contactId] is null or [position-contactId] not in (
	select [contact-externalId] from VCCons
)

-- Usage

DECLARE @RC int
DECLARE @SearchStr nvarchar(100)

-- TODO: Set parameter values here.
SET @SearchStr =
--'HSBC Global Markets & Banking'
--'Offer Made' -- [dbo].[ActionTypes].[Description]
--'Second Interview' -- [dbo].[ActionTypes].[Description]
--'Long Lists'
--'clientCorporationRatios'
'currency'
EXECUTE @RC = [dbo].[usp_SearchTextInAllTables] 
   @SearchStr
GO