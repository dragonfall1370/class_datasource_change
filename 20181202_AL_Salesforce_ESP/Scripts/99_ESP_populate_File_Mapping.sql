drop table if exists [dbo].[VCFileMapping]

select
Id
, [dbo].[ufn_PopulateFileName2](a.[Name], a.Id) as Doc

into [dbo].[VCFileMapping]

from Attachment a


--select * from VCComDocs
--where Docs in (
--	select Doc from [dbo].[VCFileMapping]
--)

select * from VCFileMapping

--select * from VCJobDocs