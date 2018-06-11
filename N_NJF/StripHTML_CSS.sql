/* --STRIP HTML */

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
    --RETURN LTRIM(RTRIM(@HTMLText))
	RETURN REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(@HTMLText)),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&')
END
GO
----------------------------------------------------
Select [dbo].[udf_StripHTML]('<HTML><BODY>test</BODY></HTML>')


/* --STRIP CSS ORIGINAL */
CREATE FUNCTION [dbo].[udf_StripCSS]
 (@CSSText VARCHAR(MAX))
 RETURNS VARCHAR(MAX)
 AS
 BEGIN
 DECLARE @Start INT
 DECLARE @End INT
 DECLARE @Length INT
 SET @Start = CHARINDEX('{',@CSSText)
 SET @End = CHARINDEX('{',@CSSText,CHARINDEX('}',@CSSText))
 SET @Length = (@End - @Start) + 1
 WHILE @Start > 0
 AND @End > 0
 AND @Length > 0
 BEGIN
 SET @CSSText = STUFF(@CSSText,@Start,@Length,'')
 SET @Start = CHARINDEX('}',@CSSText)
 SET @End = CHARINDEX('{',@CSSText,CHARINDEX('}',@CSSText))
 SET @Length = (@End - @Start) + 1
 END
 RETURN LTRIM(RTRIM(@CSSText))
 END
 GO 
 
 
 /* **STRIP CSS ENHANCED** | I LOVE IT */
--drop function [dbo].[udf_StripCSS2]
 CREATE FUNCTION [dbo].[udf_StripCSS2]
 (@CSSText VARCHAR(MAX))
 RETURNS VARCHAR(MAX)
 AS
 BEGIN
	DECLARE @Start INT
	DECLARE @End INT
	DECLARE @Length INT
	SET @Start = PATINDEX('%<STYLE%',@CSSText)
	SET @End = CHARINDEX('>',@CSSText,PATINDEX('%</STYLE>%',@CSSText)+8)
	SET @Length = (@End - @Start) + 1
 WHILE @Start > 0
	AND @End > 0
	AND @Length > 0
 BEGIN
		SET @CSSText = STUFF(@CSSText,@Start,@Length,'')
		SET @Start = PATINDEX('%<STYLE%',@CSSText)
		SET @End = CHARINDEX('>',@CSSText,PATINDEX('%</STYLE>%',@CSSText)+8)
		SET @Length = (@End - @Start) + 1
 END
	RETURN REPLACE(REPLACE(REPLACE(REPLACE(LTRIM(RTRIM(@CSSText)),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&')
 END
 GO