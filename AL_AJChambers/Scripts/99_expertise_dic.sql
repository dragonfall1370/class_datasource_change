--drop table if exists VCExpertises
--drop table if exists VCSubExpertises

;with
TmpTab1 as (
	SELECT distinct
	[Expertise] as feName
	,[SubExpertise] as sfeName
	FROM ExpertisesSkillMap2
)

, TmpTab2 as (
	select distinct
	feName
	from TmpTab1
)

, TmpTab3 as (
	select
	row_number() over(order by feName) as Id
	, feName
	from TmpTab2
)

-- populate expertise dic

--select *
--into VCExpertises
--from TmpTab3

--select
--y.Id, x.feName, x.sfeName

--into VCSubExpertises

--from TmpTab1 x
--left join TmpTab3 y on x.feName = y.feName
--where len(x.sfeName) > 0