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
Select CandidateId, [dbo].[udf_StripHTML](FrameCondition) as StrippedFrameCondition, [dbo].[udf_StripHTML](Comment) as StrippedComment,
[dbo].[udf_StripHTML](EmploymentAll) as StrippedEmploymentAll, [dbo].[udf_StripHTML](Skills) as StrippedSkills
from CandidateInfo
order by CandidateId