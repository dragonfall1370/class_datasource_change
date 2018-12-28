/****** Script for SelectTopNRows command from SSMS  ******/
SELECT TOP (1000) [SKILL]
      ,[DISPLAY]
      ,[GROUP_NAME]
  FROM [AJChambersProd].[dbo].[SKILLS_DATA_TABLE]

  SELECT TOP (1000) [CAND_ID]
      ,[SKILLINFO_ID]
      ,[EXP]
      ,[SKILL]
      ,[NUM]
      ,[SKILL_ID]
      ,[JOBS_ID]
  FROM [AJChambersProd].[dbo].[SKILLINFO_DATA_TABLE]

where JOBS_ID is not null

;with
TmpTab1 as (
	select distinct
		trim(isnull(JOBS_ID, '')) as entityExtId
		, replace(dbo.ufn_TrimSpecialCharacters_V2(isnull(SKILL, ','), ''), ',', '-') as Skill
	from [SKILLINFO_DATA_TABLE]
	where trim(isnull(JOBS_ID, '')) <> ''
)

select
entityExtId
, string_agg(Skill, ',') Skills
from TmpTab1
group by entityExtId
order by cast(entityExtId as int)

select STATUS, *
from INTERVIEWS_DATA_TABLE


-- TV Business Area
select
        CONT_ID as 'external_additional_id'
        , 'add_con_info' as additional_type
        , 1007 as form_id
        , 1019 as field_id
        , replace(replace(replace(replace( ltrim(rtrim(UC.customText1)),'Advisory',1),'General',2),'Investment',3),'TT Client',4) as field_value
-- select distinct ltrim(rtrim(UC.customText1))
from CONT_DATA_TABLE Cl
left join bullhorn1.BH_UserContact UC ON Cl.userID = UC.userID
where Cl.isPrimaryOwner = 1 and UC.customText1 <> ''



select CONT_ID, OPTOUT
from CONT_DATA_TABLE
where OPTOUT is not null

select distinct dbo.ufn_TrimSpecialCharacters_V2(STATUS, '') as StatusValue
from JOBS_DATA_TABLE
where dbo.ufn_TrimSpecialCharacters_V2(STATUS, '') <> ''
order by dbo.ufn_TrimSpecialCharacters_V2(STATUS, '')

select
cast(cast(JOBS_ID as int) as varchar(20)) as entityExtId
, dbo.ufn_TrimSpecialCharacters_V2(STATUS, '') as StatusValue
from JOBS_DATA_TABLE
where dbo.ufn_TrimSpecialCharacters_V2(STATUS, '') <> ''
order by JOBS_ID


select distinct dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '') as StatusValue
from JOBS_DATA_TABLE
where dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '') <> ''
order by dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '')


select replace(lower(cast(newid() as varchar(36))), '-' , '')


select
-- form setup --
--'add_con_info' as type
'add_job_info' as type
--'add_can_info' as type
, getdate() as insert_timestamp
, 'en' as default_language
-- field setup --
, replace(lower(cast(newid() as varchar(36))), '-' , '') as field_key
, 0 as required
, 1 as block
, 'source' as name
, 4 as field_type -- dropdownlist
--, 5 as field_type -- checkboxlist
--, '' as constraint_id


;with
TmpTab1 as (
select
---- form setup --
--'add_con_info' as type
'add_job_info' as type
--'add_can_info' as type
, getdate() as insert_timestamp
, 'en' as default_language
---- field setup --
, replace(lower(cast(newid() as varchar(36))), '-' , '') as field_key
, 0 as required
, 1 as block
, 'source' as name
, 4 as field_type -- dropdownlist
--, 5 as field_type -- checkboxlist
--, '' as constraint_id
---- field value setup
)

, TmpTab2 as (
select distinct
dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '') as cfValue
from JOBS_DATA_TABLE
where dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '') <> ''
--order by dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '')
)

select
t1.*, t2.*
from TmpTab1 t1, TmpTab2 t2
order by t2.cfValue

select
cast(cast(JOBS_ID as int) as varchar(20)) as entityExtId
, 'source' as cfName
, 'en' as language
, dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '') as cfValueTitle
from JOBS_DATA_TABLE
where dbo.ufn_TrimSpecialCharacters_V2(SOURCE, '') <> ''
order by JOBS_ID