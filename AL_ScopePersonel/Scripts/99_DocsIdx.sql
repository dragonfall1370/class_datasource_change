drop table if exists #TabTmp1

select
ATTACH_ID as ID
, [dbo].[ufn_RefineFileName](filename) as Name
--* -- 57878
--distinct filename --55263
--distinct coalesce(cast(cand_id as nvarchar), jobs_id, cont_id, clnt_id, '') + filename --56266
--distinct concat(attach_id, '_', dbo.ufn_RefineFileName(filename)) --57878

into #TabTmp1

from ATTACHMENTS_DATA_TABLE
--where CLNT_ID is not null
--where cand_id is null and jobs_id is null and cont_id is null and clnt_id is null
where
len(trim(isnull(filename, ''))) > 0 and
(cand_id is not null or jobs_id is not null or cont_id is not null or clnt_id is not null)

--select * from #TabTmp1

drop table if exists #TabTmp2

select
*
, row_number() over(partition by Name order by ID) as rn

into #TabTmp2

from #TabTmp1

drop table if exists #TabTmp1

--select * from #TabTmp2
--where rn > 1

drop table if exists #TabTmp3

select
ID
--, len(cast(rn as varchar)) IDLen
, iif(
	rn = 1
	, Name
	, concat(rn, '_', Name)
) as Name

into #TabTmp3

from #TabTmp2

drop table if exists #TabTmp2

--select * from #TabTmp3

drop table if exists VC_DocsIdx

select
x.ID
, x.Name
, y.fName as OldName
, y.fPath
, y.CLNT_ID as ComID
, y.CONT_ID as ConID
, y.JOBS_ID as JobID
, y.CAND_ID as CandID

into VC_DocsIdx

from #TabTmp3 x
left join (
	select
	ATTACH_ID
	, [FILENAME] as fName
	, ATTACHMENT_PATH as fPath
	, CLNT_ID
	, CONT_ID
	, JOBS_ID
	, CAND_ID
	from ATTACHMENTS_DATA_TABLE
) y on x.ID = y.attach_id

drop table if exists #TabTmp3

select * from VC_DocsIdx
--where ComID is not null and ComID not in (select CLNT_ID from CLNTINFO_DATA_TABLE)
where ComID is not null or ConID is not null or JobID is not null or CandID is not null
order by OldName
--order by Name

--select [Name], replace(replace(fPath, '/var/www/filesys/ba453829262d38427cbe1ad295670f2a', ''), '/', '\') fPath
--from VC_DocsIdx