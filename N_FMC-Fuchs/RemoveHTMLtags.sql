CREATE FUNCTION [dbo].[udf_StripHTML] (@HTMLText VARCHAR(MAX))
RETURNS VARCHAR(MAX) AS
BEGIN
    DECLARE @Start INT
    DECLARE @End INT
    DECLARE @Length INT
    SET @Start = CHARINDEX('<',@HTMLText)
    SET @End = CHARINDEX('>',@HTMLText,CHARINDEX('<',@HTMLText))
    SET @Length = (@End - @Start) + 1
    WHILE @Start > 0 AND @End > 0 AND @Length > 0
    BEGIN
        SET @HTMLText = STUFF(@HTMLText,@Start,@Length,'')
        SET @Start = CHARINDEX('<',@HTMLText)
        SET @End = CHARINDEX('>',@HTMLText,CHARINDEX('<',@HTMLText))
        SET @Length = (@End - @Start) + 1
    END
    RETURN LTRIM(RTRIM(@HTMLText))
END
GO
----------------------------------------------------
--Select CandidateId, [dbo].[udf_StripHTML](FrameCondition) as StrippedFrameCondition, [dbo].[udf_StripHTML](Comment) as StrippedComment,
--[dbo].[udf_StripHTML](EmploymentAll) as StrippedEmploymentAll, [dbo].[udf_StripHTML](Skills) as StrippedSkills
--from CandidateInfo
--order by CandidateId

-----------
Select CandidateId, --REPLACE(replace([dbo].[udf_StripHTML](Education),'&nbsp;',''),concat(char(13),char(10)),' ') as StrippedEducation
REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	LTRIM(RTRIM([dbo].[udf_StripHTML](Education))),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&'),
	'&#8230;','...'),'&#8217;',''''),'&#8211;','-'),'&#8220;','"'),'&#8216;',''''),'&#8221;','"'),'&quot;',''),'&#39;',''''),concat(char(10),char(13)),' ')
	 as StrippedEducation
from CandidateEducation
order by CandidateId

Select CandidateId
, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	LTRIM(RTRIM([dbo].[udf_StripHTML](FrameCondition))),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&'),
	'&#8230;','...'),'&#8217;',''''),'&#8211;','-'),'&#8220;','"'),'&#8216;',''''),'&#8221;','"'),'&quot;',''),'&#39;',''''),concat(char(10),char(13)),' ') as StrippedFrameCondition
, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	LTRIM(RTRIM([dbo].[udf_StripHTML](Comment))),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&'),
	'&#8230;','...'),'&#8217;',''''),'&#8211;','-'),'&#8220;','"'),'&#8216;',''''),'&#8221;','"'),'&quot;',''),'&#39;',''''),concat(char(10),char(13)),' ') as StrippedComment
, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	LTRIM(RTRIM([dbo].[udf_StripHTML](EmploymentAll))),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&'),
	'&#8230;','...'),'&#8217;',''''),'&#8211;','-'),'&#8220;','"'),'&#8216;',''''),'&#8221;','"'),'&quot;',''),'&#39;',''''),concat(char(10),char(13)),' ') as StrippedEmploymentAll
, REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	LTRIM(RTRIM([dbo].[udf_StripHTML](Skills))),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&'),
	'&#8230;','...'),'&#8217;',''''),'&#8211;','-'),'&#8220;','"'),'&#8216;',''''),'&#8221;','"'),'&quot;',''),'&#39;',''''),concat(char(10),char(13)),' ') as StrippedSkills
from CandidateInfo
order by CandidateId
----------
Select CandidateId
, LTRIM(RTRIM(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	[dbo].[udf_StripHTML](EmploymentAll),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&'),
	'&#8230;','...'),'&#8217;',''''),'&#8211;','-'),'&#8220;','"'),'&#8216;',''''),'&#8221;','"'),'&quot;',''),'&#39;',''''),concat(char(10),char(13)),' '))) as StrippedEmploymentAll

from CandidateInfo
order by CandidateId