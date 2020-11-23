IF OBJECT_ID (N'dbo.removeNullCharacters', N'FN') IS NOT NULL
    DROP FUNCTION removeNullCharacters;
GO

CREATE FUNCTION [dbo].RemoveNullCharacters
(
    @String NVARCHAR(MAX)
)
RETURNS nvarchar(max)
AS
BEGIN
    RETURN convert(nvarchar(max),convert(varbinary(max),replace(convert(varbinary(max),@String),0x0000,0x)))
END
GO
