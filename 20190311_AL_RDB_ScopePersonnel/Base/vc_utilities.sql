drop function if exists [dbo].[ufn_ParseRTF]
go

create function	[dbo].[ufn_ParseRTF] (
	@rtf VARCHAR(max)
)
RETURNS VARCHAR(max)
AS
BEGIN

DECLARE @Stage TABLE (
Chr CHAR(1),
Pos INT
)

SET @rtf = replace(@rtf, char(10), ' ')

INSERT @Stage (
Chr,
Pos
)
SELECT SUBSTRING(@rtf, Number, 1),
Number
FROM master..spt_values
WHERE Type = 'p'
AND SUBSTRING(@rtf, Number, 1) IN ('{', '}')

DECLARE @Pos1 INT,
@Pos2 INT

SELECT @Pos1 = MIN(Pos),
@Pos2 = MAX(Pos)
FROM @Stage

DELETE
FROM @Stage
WHERE Pos IN (@Pos1, @Pos2)

WHILE 1 = 1
BEGIN
SELECT TOP 1 @Pos1 = s1.Pos, @Pos2 = s2.Pos
FROM @Stage AS s1
INNER JOIN @Stage AS s2 ON s2.Pos > s1.Pos
WHERE s1.Chr = '{'
AND s2.Chr = '}'
ORDER BY s2.Pos - s1.Pos

IF @@ROWCOUNT = 0
BREAK

DELETE
FROM @Stage
WHERE Pos IN (@Pos1, @Pos2)

UPDATE @Stage
SET Pos = Pos - @Pos2 + @Pos1 - 1
WHERE Pos > @Pos2

SET @rtf = STUFF(@rtf, @Pos1, @Pos2 - @Pos1 + 1, '')
END

SET @Pos1 = PATINDEX('%\cf[0123456789][0123456789 ]%', @rtf)

WHILE @Pos1 > 0
SELECT @Pos2 = CHARINDEX(' ', @rtf, @Pos1 + 1), @rtf = STUFF(@rtf, @Pos1, @Pos2 - @Pos1 + 1, ''), @Pos1 = PATINDEX('%\cf[0123456789][0123456789 ]%', @rtf)

SET @rtf = REPLACE(@rtf, '\pard', '')

SET @rtf = REPLACE(@rtf, '\par', '')

--SET @rtf = REPLACE(@rtf, '\ulnone', '')

--SET @rtf = REPLACE(@rtf, '\ul', '')

--SET @rtf = REPLACE(@rtf, '\line', '')

--SET @rtf = REPLACE(@rtf, '\fswiss', '')

SET @rtf = case when LEN(@rtf)>0 then LEFT(@rtf, LEN(@rtf) - 1) else @rtf end

SELECT @rtf = REPLACE(@rtf, '\b0 ', ''), @rtf = REPLACE(@rtf, '\b ', '')

SELECT @rtf = STUFF(@rtf, 1, CHARINDEX(' ', @rtf), '')


RETURN @rtf

END

GO

--====================================================
DROP FUNCTION IF EXISTS [dbo].[ufn_RefinePhoneNumber_V2]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_RefinePhoneNumber_V2] (
	@input nvarchar(255)
)
returns nvarchar(20)
as  
begin
	declare @invalidCharPos int = -1
	declare @openParenthesisPos int = -1
	declare @closeParenthesisPos int = -1
	declare @retVal nvarchar(20)

	--while @invalidCharPos <> 0
	--begin
		set @input = [dbo].[ufn_TrimSpecialCharacters_V2](@input, '')
		if(@input like '([a-z][a-z])%')
			set @input = stuff(@input, 1, 4, '')
		-- replace '.' or '�' by ' '
		set @input = replace(@input, '.', ' ')
		set @input = replace(@input, '�', ' ')
		set @input = replace(@input, char(160), ' ')
		set @input = replace(@input, '\', ' - ')
		set @input = replace(@input, '/', ' - ')
		set @invalidCharPos = patindex('%[^-0-9+ )(]%', @input)
		if(@invalidCharPos > 0)
			set @retVal = left(@input, @invalidCharPos - 1)
		else
			set @retVal = @input

		set @openParenthesisPos = charindex('(', @retVal)
		set @closeParenthesisPos = charindex(')', @retVal)
		-- check if '+' present in phone's body
		set @invalidCharPos = charindex('+', @retVal)
		if(@invalidCharPos > 1 and @closeParenthesisPos > 1 and @invalidCharPos > @closeParenthesisPos)
			set @retVal = left(@retVal, @invalidCharPos - 1)
		-- trim
		set @retVal = trim(', ' from @retVal)
		-- check if '(' present in phone's body
		set @openParenthesisPos = charindex('(', @retVal)
		if(@openParenthesisPos > 1 and (@openParenthesisPos >= len(@retVal) - 1))
			begin
				set @closeParenthesisPos = charindex(')', @retVal)
				if(@closeParenthesisPos < 1)
					set @retVal = left(@retVal, @openParenthesisPos - 1)
			end
		else if (@openParenthesisPos >= 1)
			begin
				set @closeParenthesisPos = charindex(')', @retVal)
				if(@closeParenthesisPos < 1)
					set @retVal = replace(@retVal, '(', '')
			end
		else
			begin
				set @closeParenthesisPos = charindex(')', @retVal)
				if(@closeParenthesisPos > 1)
					set @retVal = replace(@retVal, ')', '')
			end
		-- trim
		set @retVal = trim(N'�-, ' from @retVal)
	--end
	return @retVal
end

GO
--====================================================
DROP FUNCTION IF EXISTS [dbo].[ufn_TrimSpecialCharacters_V2]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_TrimSpecialCharacters_V2] (
	@input nvarchar(max)
	, @specifiedCharacters4Trim nvarchar(255)
)
returns nvarchar(max)
as  
begin
	declare @chars4trim nvarchar(10) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + ' ' + @specifiedCharacters4Trim
	declare @retVal nvarchar(max)

	set @retVal = trim(@chars4trim from isnull(@input, ''))

	return @retVal
end

GO


-- region metadata

--ALTER FUNCTION [dbo].[udf_StripHTML]
--(
--@HTMLText varchar(MAX)
--)
--RETURNS varchar(MAX)
--AS
--BEGIN
--DECLARE @Start  int
--DECLARE @End    int
--DECLARE @Length int

---- Replace the HTML entity &amp; with the '&' character (this needs to be done first, as
---- '&' might be double encoded as '&amp;amp;')
--SET @Start = CHARINDEX('&amp;', @HTMLText)
--SET @End = @Start + 4
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, '&')
--SET @Start = CHARINDEX('&amp;', @HTMLText)
--SET @End = @Start + 4
--SET @Length = (@End - @Start) + 1
--END

---- Replace the HTML entity &lt; with the '<' character
--SET @Start = CHARINDEX('&lt;', @HTMLText)
--SET @End = @Start + 3
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, '<')
--SET @Start = CHARINDEX('&lt;', @HTMLText)
--SET @End = @Start + 3
--SET @Length = (@End - @Start) + 1
--END

---- Replace the HTML entity &gt; with the '>' character
--SET @Start = CHARINDEX('&gt;', @HTMLText)
--SET @End = @Start + 3
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, '>')
--SET @Start = CHARINDEX('&gt;', @HTMLText)
--SET @End = @Start + 3
--SET @Length = (@End - @Start) + 1
--END

---- Replace the HTML entity &amp; with the '&' character
--SET @Start = CHARINDEX('&amp;amp;', @HTMLText)
--SET @End = @Start + 4
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, '&')
--SET @Start = CHARINDEX('&amp;amp;', @HTMLText)
--SET @End = @Start + 4
--SET @Length = (@End - @Start) + 1
--END

---- Replace the HTML entity &nbsp; with the ' ' character
--SET @Start = CHARINDEX('&nbsp;', @HTMLText)
--SET @End = @Start + 5
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, ' ')
--SET @Start = CHARINDEX('&nbsp;', @HTMLText)
--SET @End = @Start + 5
--SET @Length = (@End - @Start) + 1
--END

---- Replace any <br> tags with a newline
--SET @Start = CHARINDEX('<br>', @HTMLText)
--SET @End = @Start + 3
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, CHAR(13) + CHAR(10))
--SET @Start = CHARINDEX('<br>', @HTMLText)
--SET @End = @Start + 3
--SET @Length = (@End - @Start) + 1
--END

---- Replace any <br/> tags with a newline
--SET @Start = CHARINDEX('<br/>', @HTMLText)
--SET @End = @Start + 4
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, 'CHAR(13) + CHAR(10)')
--SET @Start = CHARINDEX('<br/>', @HTMLText)
--SET @End = @Start + 4
--SET @Length = (@End - @Start) + 1
--END

---- Replace any <br /> tags with a newline
--SET @Start = CHARINDEX('<br />', @HTMLText)
--SET @End = @Start + 5
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, 'CHAR(13) + CHAR(10)')
--SET @Start = CHARINDEX('<br />', @HTMLText)
--SET @End = @Start + 5
--SET @Length = (@End - @Start) + 1
--END

---- Remove anything between <whatever> tags
--SET @Start = CHARINDEX('<', @HTMLText)
--SET @End = CHARINDEX('>', @HTMLText, CHARINDEX('<', @HTMLText))
--SET @Length = (@End - @Start) + 1

--WHILE (@Start > 0 AND @End > 0 AND @Length > 0) BEGIN
--SET @HTMLText = STUFF(@HTMLText, @Start, @Length, '')
--SET @Start = CHARINDEX('<', @HTMLText)
--SET @End = CHARINDEX('>', @HTMLText, CHARINDEX('<', @HTMLText))
--SET @Length = (@End - @Start) + 1
--END

--RETURN LTRIM(RTRIM(@HTMLText))

--END

--GO
-------------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_ConvertHTMLToText]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[ufn_ConvertHTMLToText](
	@Text NVARCHAR(MAX)
)  
RETURNS NVARCHAR(MAX)  
AS  
BEGIN  
 DECLARE @Start  INT  
 DECLARE @End    INT  
 DECLARE @Length INT
 DECLARE @char	 INT  

 SET @Text = REPLACE(REPLACE(REPLACE(
		@Text
  		,N'<br>',nchar(13)+nchar(10))
		,N'<br/>',nchar(13)+nchar(10))
		,N'<br />',nchar(13)+nchar(10))  
 SET @Start = CHARINDEX(N'<',@Text)  
 SET @End = CHARINDEX(N'>',@Text,CHARINDEX(N'<',@Text))  
 SET @Length = (@End - @Start) + 1

--Remove any text that starts with <  and ends with >
 WHILE @Start > 0 AND @End > 0 AND @Length > 0  
 BEGIN  
   SET @Text = STUFF(@Text,@Start,@Length,N'')  
   SET @Start = CHARINDEX(N'<',@Text, @End-@Length)  
   SET @End = CHARINDEX(N'>',@Text,CHARINDEX(N'<',@Text, @Start))  
   SET @Length = (@End - @Start) + 1  
 END  

--Translate the most common HTML special entities 
 SET @Text = RTRIM(LTRIM(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(
		@Text
		,N'&ldquo;',nchar(8220))
		,N'&rdquo;',nchar(8221))		
		,N'&apos;',nchar(39))
		,N'&middot;',nchar(183))
		,N'&ndash;',nchar(8211))
		,N'&mdash;',nchar(8212))
		,N'&quot;',nchar(34))
		,N'&aacute;',nchar(225))
		,N'&eacute;',nchar(233))
		,N'&iacute;',nchar(237))
		,N'&oacute;',nchar(243))
		,N'&uacute;',nchar(250))
		,N'&agrave;',nchar(224))
		,N'&egrave;',nchar(232))
		,N'&igrave;',nchar(236))
		,N'&ograve;',nchar(242))
		,N'&ugrave;',nchar(249))
		,N'&auml;',nchar(228))
		,N'&euml;',nchar(235))
		,N'&iuml;',nchar(239))
		,N'&ouml;',nchar(246))
		,N'&uuml;',nchar(252))
		,N'&yuml;',nchar(255))
		,N'&acirc;',nchar(226))
		,N'&ecirc;',nchar(234))
		,N'&icirc;',nchar(238))
		,N'&ocirc;',nchar(244))
		,N'&ucirc;',nchar(251))
		,N'&atilde;',nchar(227))
		,N'&otilde;',nchar(245))
		,N'&ntilde;',nchar(241))
		,N'&eth;',nchar(240))
		,N'&oslash;',nchar(248))
		,N'&szlig;',nchar(223))
		,N'&aring;',nchar(229))
		,N'&aelig;',nchar(230))
		,N'&ccedil;',nchar(231))
		,N'&thorn;',nchar(254))
		,N'&pound;',nchar(163))
		,N'&euro;',nchar(8364))
		,N'&copy;',nchar(169))
		,N'&hellip;',nchar(8230))
		,N'&bull;',nchar(8226))
		,N'&rsquo;',nchar(8217))
		,N'&lt;',nchar(60))
		,N'&gt;',nchar(62))
		,N'&amp;',nchar(38))
		,N'&reg;',nchar(174))
		,N'&nbsp;',nchar(160))
  ))

--Take care of anything else between & and ;
 SET @Start = CHARINDEX(N'&',@Text)  
 SET @End = CHARINDEX(N';',@Text,CHARINDEX(N'&',@Text))  
 SET @Length = (@End - @Start) + 1  
 WHILE @Start > 0 AND @End > 0 AND @Length > 0  
 BEGIN  
   IF (charindex(N' ',SUBSTRING(@Text, @Start, @Length)) = 0)  
   --Prevents false positives. If there is a space between & and ;, it should be left alone.
    begin  
      IF ((substring(@Text,@Start + 1,1) = '#') AND (isNumeric(substring(@Text,@Start + 2,@Length - 3)) = 1))
	  --If the character after the ampersand is a # followed by a number, ex. &#234;, it is translated into the proper unicode character.
		begin
		  SET @char = cast(substring(@Text,@Start + 2,@Length - 3) as numeric)
		  SET @Text = STUFF(@Text,@Start,@Length,nchar(@char))
		END
	  ELSE
	  	SET @Text = STUFF(@Text,@Start,@Length,N'')
		--This is how rare special entities not handled in the sections above are handled. It will just eliminate the character.  
    end  
   ELSE  
      SET @Length = 0;  
	  --If there is a space between the & and the ;, then leave it alone. It may be regular text.

   SET @Start = CHARINDEX(N'&',@Text, @End-@Length)  
   SET @End = CHARINDEX(N';',@Text,CHARINDEX(N'&',@Text, @Start))  
   SET @Length = (@End - @Start) + 1  
 END

 RETURN @Text;
END

GO
---------------------------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_RemoveForXMLUnsupportedCharacters]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_RemoveForXMLUnsupportedCharacters] (
	@input nvarchar(max)
)
returns nvarchar(max)
as  
begin
	declare @preInput nvarchar(max)
	declare @retVal nvarchar(max)
	 
	set @preInput = trim(isnull(cast(@input as nvarchar(max)), ''))

	set @retVal = replace(@preInput, char(0x0000), '')
	
	set @retVal = replace(@retVal, char(0x0001), '')

	set @retVal = replace(@retVal, char(0x0002), '')
	
	set @retVal = replace(@retVal, char(0x0003), '')
	
	set @retVal = replace(@retVal, char(0x0004), '')
	
	set @retVal = replace(@retVal, char(0x0005), '')
	
	set @retVal = replace(@retVal, char(0x0006), '')
	
	set @retVal = replace(@retVal, char(0x0007), '')
	
	set @retVal = replace(@retVal, char(0x0008), '')
	
	set @retVal = replace(@retVal, char(0x000B), '')
	
	set @retVal = replace(@retVal, char(0x000C), '')
	
	set @retVal = replace(@retVal, char(0x000E), '')
	
	set @retVal = replace(@retVal, char(0x000F), '')
	
	set @retVal = replace(@retVal, char(0x0010), '')
	
	set @retVal = replace(@retVal, char(0x0011), '')
	
	set @retVal = replace(@retVal, char(0x0012), '')
	
	set @retVal = replace(@retVal, char(0x0013), '')
	
	set @retVal = replace(@retVal, char(0x0014), '')
	
	set @retVal = replace(@retVal, char(0x0015), '')
	
	set @retVal = replace(@retVal, char(0x0016), '')
	
	set @retVal = replace(@retVal, char(0x0017), '')
	
	set @retVal = replace(@retVal, char(0x0018), '')
	
	set @retVal = replace(@retVal, char(0x0019), '')
	
	set @retVal = replace(@retVal, char(0x001A), '')
	
	set @retVal = replace(@retVal, char(0x001B), '')
	
	set @retVal = replace(@retVal, char(0x001C), '')
	
	set @retVal = replace(@retVal, char(0x001D), '')
	
	set @retVal = replace(@retVal, char(0x001E), '')
	
	set @retVal = replace(@retVal, char(0x001F), '')

	return @retVal
end

go


--====================================================
DROP FUNCTION IF EXISTS [dbo].[ufn_EncodeJsonValue]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


--Backspace is replaced with \b
--Form feed is replaced with \f
--Newline is replaced with \n
--Carriage return is replaced with \r
--Tab is replaced with \t
--Double quote is replaced with \"
--Backslash is replaced with \\

create function [dbo].[ufn_EncodeJsonValue] (
	@input nvarchar(max)
)
returns nvarchar(max)
as  
begin
	declare @retVal nvarchar(max)

	set @retVal = trim(isnull(@input, ''))

	if(len(@retVal) = 0) return ''

	--set @retVal =
	--	replace(
	--		replace(
	--			replace(
	--				replace(
	--					replace(
	--						replace(
	--							replace(
	--								replace(
	--									@retVal, char(9), '\t'
	--								)
	--								, char(13), '\r'
	--							)
	--							, char(10), '\n'
	--						)
	--						, '"', '\"'
	--					)
	--					, '\', '\\'
	--				)
	--				, '', ''
	--			)
	--			, @newLineChar1, '\n'
	--		), '''', ''''''
	--	)

	return @retVal
end

GO

---------------------------------------------------

DROP FUNCTION IF EXISTS [dbo].[ufn_HtmlEncode]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE FUNCTION [dbo].[ufn_HtmlEncode](
     @Value nvarchar(max),
     @PreserveNewLine bit)
 RETURNS nvarchar(max)
AS
BEGIN
     DECLARE @Result nvarchar(max)
     SELECT @Result = @Value
     IF @Result IS NOT NULL AND LEN(@Result) > 0
     BEGIN
         SELECT @Result = REPLACE(@Result, N'&', N'&amp;')
         SELECT @Result = REPLACE(@Result, N'<', N'&lt;')
         SELECT @Result = REPLACE(@Result, N'>', N'&gt;')
         SELECT @Result = REPLACE(@Result, N'''', N'&#39;')
         SELECT @Result = REPLACE(@Result, N'"', N'&quot;')
         IF @PreserveNewLine = 1
             SELECT @Result = REPLACE(@Result, CHAR(10), CHAR(10) + N'<br>')
     END
     RETURN @Result
END

GO
----------------------------------------------------------------
DROP PROCEDURE IF EXISTS [dbo].[usp_BullhornExtractFileFromBinaryField]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[usp_BullhornExtractFileFromBinaryField]
(
    @outputPath varchar(max)
)
AS
BEGIN
DECLARE
--@SQLIMG VARCHAR(MAX),
@content VARBINARY(MAX),
@extension VARCHAR(4),
@name varchar(255),
@filePath VARCHAR(MAX),
@ObjectToken INT,
@finalOutputPath varchar(max)

set @finalOutputPath =
iif(
	charindex('\', @outputPath, len(@outputPath) - 2) = len(@outputPath)
	, @outputPath
	, concat(@outputPath, '\')
)

DECLARE ffCur CURSOR FAST_FORWARD FOR 
	SELECT --top 10
		commentsCompressed
		, cast(userMessageID as varchar(255)) fileName
		, '' as Extension
	from bullhorn1.BH_UserMessage x

OPEN ffCur 

FETCH NEXT FROM ffCur INTO @content, @name, @extension

WHILE @@FETCH_STATUS = 0
BEGIN
    SET @filePath = concat(
		@finalOutputPath
		, @name
		, @extension
	)

    PRINT @filePath
    --PRINT @SQLIMG

    EXEC sp_OACreate 'ADODB.Stream', @ObjectToken OUTPUT
    EXEC sp_OASetProperty @ObjectToken, 'Type', 1
    EXEC sp_OAMethod @ObjectToken, 'Open'
    EXEC sp_OAMethod @ObjectToken, 'Write', NULL, @content
    EXEC sp_OAMethod @ObjectToken, 'SaveToFile', NULL, @filePath, 2
    EXEC sp_OAMethod @ObjectToken, 'Close'
    EXEC sp_OADestroy @ObjectToken

    FETCH NEXT FROM ffCur INTO @content, @name, @extension
END

CLOSE ffCur
DEALLOCATE ffCur

--##########################################################################################
--# total: 342,772
--# running time: 01:02:24
END
----------------------------------------------------------------
DROP PROCEDURE IF EXISTS [dbo].[usp_SearchTextInAllTables]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

CREATE PROC [dbo].[usp_SearchTextInAllTables]
(
    @SearchStr nvarchar(100)
)
AS
BEGIN

    CREATE TABLE #Results (ColumnName nvarchar(370), ColumnValue nvarchar(3630))

    SET NOCOUNT ON

    DECLARE @TableName nvarchar(256), @ColumnName nvarchar(128), @SearchStr2 nvarchar(110)
    SET  @TableName = ''
    SET @SearchStr2 = QUOTENAME('%' + @SearchStr + '%','''')

    WHILE @TableName IS NOT NULL

    BEGIN
        SET @ColumnName = ''
        SET @TableName = 
        (
            SELECT MIN(QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME))
            FROM     INFORMATION_SCHEMA.TABLES
            WHERE         TABLE_TYPE = 'BASE TABLE'
                AND    QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME) > @TableName
                AND    OBJECTPROPERTY(
                        OBJECT_ID(
                            QUOTENAME(TABLE_SCHEMA) + '.' + QUOTENAME(TABLE_NAME)
                             ), 'IsMSShipped'
                               ) = 0
        )

        WHILE (@TableName IS NOT NULL) AND (@ColumnName IS NOT NULL)

        BEGIN
            SET @ColumnName =
            (
                SELECT MIN(QUOTENAME(COLUMN_NAME))
                FROM     INFORMATION_SCHEMA.COLUMNS
                WHERE         TABLE_SCHEMA    = PARSENAME(@TableName, 2)
                    AND    TABLE_NAME    = PARSENAME(@TableName, 1)
                    AND    DATA_TYPE IN ('char', 'varchar', 'nchar', 'nvarchar', 'int', 'decimal')
                    AND    QUOTENAME(COLUMN_NAME) > @ColumnName
            )

            IF @ColumnName IS NOT NULL

            BEGIN
                INSERT INTO #Results
                EXEC
                (
                    'SELECT ''' + @TableName + '.' + @ColumnName + ''', LEFT(' + @ColumnName + ', 3630) 
                    FROM ' + @TableName + ' (NOLOCK) ' +
                    ' WHERE ' + @ColumnName + ' LIKE ' + @SearchStr2
                )
            END
        END    
    END

    SELECT ColumnName, ColumnValue FROM #Results
END

GO

-- Usage

--DECLARE @RC int
--DECLARE @SearchStr nvarchar(100)

---- TODO: Set parameter values here.
--SET @SearchStr =
----'HSBC Global Markets & Banking'
----'Offer Made' -- [dbo].[ActionTypes].[Description]
----'Second Interview' -- [dbo].[ActionTypes].[Description]
----'Long Lists'
----'clientCorporationRatios'
--'description_truong'
--EXECUTE @RC = [dbo].[usp_SearchTextInAllTables] 
--   @SearchStr
--GO

-- endregion metadata

----------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_RefineWebAddress]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_RefineWebAddress] (
	@input nvarchar(max)
)
returns nvarchar(max)
as  
begin
	declare @retVal nvarchar(max)
	declare @runIdx int

	set @retVal = trim(isnull(@input, ''))

	if(len(@retVal) = 0) return ''

	set @runIdx =
	
	iif(charindex('://', @retVal) > 0
		,  charindex('://', @retVal) + 3
		, 1
	)

	set @retVal = right(@retVal, len(@retVal) - @runIdx + 1)
	
	-- add / into the end for next logic run
	set @retVal = concat(@retVal, '/')

	set @runIdx = charindex('/', @retVal) - 1

	set @retVal = left(@retVal, @runIdx)

	return @retVal
end

GO

----------------------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_GetContractLengthFromJobTerm]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_GetContractLengthFromJobTerm] (
	@input nvarchar(max)
)
returns nvarchar(max)
as  
begin
	declare @retVal nvarchar(max)
	declare @runIdx int

	set @retVal = trim(isnull(@input, ''))

	if(len(@retVal) = 0) return ''

	set @runIdx = charindex(' ', @retVal)

	if(@runIdx >= 2)
	begin

		set @retVal = left(@retVal, @runIdx - 1)

		if(charindex('+', @retVal) = len(@retVal))
		begin
			set @retVal = left(@retVal, len(@retVal) - 1)
		end
		else
		begin
			if(charindex('-', @retVal) > 0 and charindex('-', @retVal) < len(@retVal))
				set @retVal = right(@retVal, len(@retVal) - charindex('-', @retVal))
		end

	end

	return @retVal
end

GO

--------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_RefineFileName]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


create function [dbo].[ufn_RefineFileName] (
	@input nvarchar(max)
)
returns nvarchar(max)
as  
begin	
	declare @retVal nvarchar(max)

	set @retVal =
	replace(
		replace(
			replace(
				replace(
					replace(
						replace(
							replace(
								replace(
									replace(
										replace(
											replace(
												replace(
														replace(
															trim('\/:*?"<>|&'' ,' from isnull(@input, ''))
															, ','
															, '_'
														)
														, ' '
														, '_'
												)
												, '\'
												, '_'
											)
											, '/'
											, '_'
										)
										, ':'
										, '_'
									)
									, '*'
									, '_'
								)
								, '?'
								, '_'
							)
							, '"'
							, '_'
						)
						, '<'
						, '_'
					)
					, '>'
					, ''
				)
				, '|'
				, '_'
			)
			, '&'
			, '_'
		)
		, ''''
		, '_'
	)

-- unacceptable characters in file name:
-- \/:*?"<>|
-- unrecommendable characters:
-- , \s & '

	return @retVal
end

GO

-----------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_CheckEmailAddress]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_CheckEmailAddress] (
	@input nvarchar(max)
)
returns bit
begin
	declare @retVal bit

	set @retVal =
		case
			when (@input like '%_@_%_.__%' and @input not like '%[,/\"* ]%') then 1
			else 0
		end

return @retVal

end

go

--select [dbo].[ufn_CheckEmailAddress]('ab c@dev.com')

-----------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_TrimSpecialCharacters]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_TrimSpecialCharacters] (
	@input nvarchar(max)
)
returns nvarchar(max)
begin
	declare @retVal nvarchar(max)
	declare @chars4trim nvarchar(255) = nchar(10) + nchar(13) + nchar(9) + nchar(160) + ';: ~`!#$%^&*()_-+=}{][\|,<.>/?''"@'

	set @retVal = trim(@chars4trim from isnull(@input, ''))

return @retVal

end

go

--select [dbo].[ufn_TrimSpecialCharacters](';!ab;(j!fkd*()+_{}')

---------------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_PopulateEmailAddress3]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_PopulateEmailAddress3] (
	@input1 nvarchar(max),
	@input2 nvarchar(max),
	@input3 nvarchar(max)
)
returns nvarchar(max)
begin
	declare @retVal nvarchar(max)

	set @retVal =
		lower(
			[dbo].[ufn_TrimSpecialCharacters](
				concat(
					iif([dbo].[ufn_CheckEmailAddress]([dbo].[ufn_TrimSpecialCharacters](@input1)) = 0
						, ''
						, [dbo].[ufn_TrimSpecialCharacters](@input1))
	
					, iif([dbo].[ufn_CheckEmailAddress]([dbo].[ufn_TrimSpecialCharacters](@input2)) = 0
						, ''
						, ',' + [dbo].[ufn_TrimSpecialCharacters](@input2))

					, iif([dbo].[ufn_CheckEmailAddress]([dbo].[ufn_TrimSpecialCharacters](@input3)) = 0
						, ''
						, ',' + [dbo].[ufn_TrimSpecialCharacters](@input3))
				)
			)
		)

return @retVal

end

go

-----------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_GetHashCode]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_GetHashCode] (
	@input nvarchar(max)
)
returns int
as  
begin
	declare @retVal int
	declare @strToProcess nvarchar(max)
	declare @runLenght int
	declare @i int

	--set @input = HashBytes('MD5', @input)

	set @strToProcess = trim(isnull(@input, ''))

	if(len(@strToProcess) = 0) return 0

	set @runLenght = len(@strToProcess)

	set @retVal = 0

	set @i = 0

	while @i < @runLenght
	begin
		set @i = @i + 1

		set @retVal += unicode(substring(@strToProcess, @i, 1)) * @i
	end

	--return checksum(@input)
	return @retVal
end

GO

-----------------------------------------------------------
drop function if exists [dbo].[ufn_RefinePhoneNumber]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_RefinePhoneNumber] (
	@input nvarchar(max)
)
returns nvarchar(max)
as  
begin
	declare @retVal nvarchar(max)

	set @retVal =
	replace(
		replace(
			--replace(
				replace(
					trim('.,!/ ' from isnull(@input, ''))
					, ' '
					, ''
				)
			--	, '-'
			--	, ''
			--)
			, '//'
			, ','
		)
		, '/'
		, ','
	)

	return @retVal
end

go
-----------------------------------------------------------
drop function if exists [dbo].[ufn_TrimSpecifiedCharacters]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_TrimSpecifiedCharacters] (
	@str4process nvarchar(max),
	@chars4trimming nvarchar(255) -- ex: '., '
)
returns nvarchar(max)
as  
begin
	declare @retVal nvarchar(max)

	set @retVal = trim(@chars4trimming from isnull(@str4process, ''))

	return @retVal
end

go

-----------------------------------------------------------
drop function if exists [dbo].[ufn_PopulateLocationName]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_PopulateLocationName] (
	@billingState nvarchar(max),
	@billingCountry nvarchar(max),
	@chars4trimming nvarchar(255) -- ex: '., '
)
returns nvarchar(max)
as  
begin
	declare @retVal nvarchar(max)
	--declare @chars4trimming nvarchar(255)

	--set @chars4trimming = '., '

	set @retVal =
	
	trim(@chars4trimming from
		concat(
			[dbo].[ufn_TrimSpecifiedCharacters](@billingState, @chars4trimming)

			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@billingCountry, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@billingCountry, @chars4trimming)
				, ''
			)
		)
	)

	return @retVal
end

go
-----------------------------------------------------------
drop function if exists [dbo].[ufn_PopulateLocationAddress]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_PopulateLocationAddress] (
	@billingStreet nvarchar(255),
	@billingCity nvarchar(255),
	@billingState nvarchar(255),
	@billingPostalCode nvarchar(255),
	@billingCountry nvarchar(255),
	@chars4trimming nvarchar(255) -- ex: '., '
)
returns nvarchar(255)
as  
begin
	declare @retVal nvarchar(255)
	--declare @chars4trimming nvarchar(255)

	--set @chars4trimming = '., '

	set @retVal =
	
	trim(@chars4trimming from
		concat(
			[dbo].[ufn_TrimSpecifiedCharacters](@billingStreet, @chars4trimming)

			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@billingCity, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@billingCity, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@billingState, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@billingState, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@billingPostalCode, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@billingPostalCode, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@billingCountry, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@billingCountry, @chars4trimming)
				, ''
			)
		)
	)

	return @retVal
end

go
-----------------------------------------------------------
drop function if exists [dbo].[ufn_PopulateLocationAddressUK]
go

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_PopulateLocationAddressUK] (
	@address1 nvarchar(255),
	@address2 nvarchar(255),
	@address3 nvarchar(255),
	@town nvarchar(255),
	@county nvarchar(255),
	@postCode nvarchar(255),
	@country nvarchar(255),
	@chars4trimming nvarchar(255) -- ex: '., '
)
returns nvarchar(255)
as  
begin
	declare @retVal nvarchar(255)
	--declare @chars4trimming nvarchar(255)

	--set @chars4trimming = '., '

	set @retVal =
	
	trim(@chars4trimming from
		concat(
			[dbo].[ufn_TrimSpecifiedCharacters](@address1, @chars4trimming)

			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@address2, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@address2, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@address3, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@address3, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@town, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@town, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@county, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@county, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@country, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@country, @chars4trimming)
				, ''
			)
			, iif(len([dbo].[ufn_TrimSpecifiedCharacters](@postCode, @chars4trimming)) > 0
				, ', ' + [dbo].[ufn_TrimSpecifiedCharacters](@postCode, @chars4trimming)
				, ''
			)
		)
	)

	return @retVal
end

go
-----------------
-- region Level 2

-----------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_PopulateFileName1]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_PopulateFileName1] (
	@fileName nvarchar(255),
	@stringId nvarchar(255)
)
returns nvarchar(255)
as  
begin
	declare @retVal nvarchar(255)
	declare @refinedFileName nvarchar(255)

	set @refinedFileName =

	iif(len([dbo].[ufn_RefineFileName](@fileName)) > 0
		, [dbo].[ufn_RefineFileName](@fileName)
		, 'untitled'
	)

	set @retVal =
	
	concat(
		iif(isnumeric(trim(isnull(@stringId, ''))) = 0,
			cast([dbo].[ufn_GetHashCode](@stringId) as nvarchar(max))
			, trim(isnull(@stringId, ''))
		)
		, '_'
		, @refinedFileName
	)

	return @retVal
end

GO

-----------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_PopulateFileName2]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_PopulateFileName2] (
	@fileName nvarchar(255),
	@stringId nvarchar(255)
)
returns nvarchar(255)
as  
begin
	declare @retVal nvarchar(255)
	declare @refinedFileName nvarchar(255)

	set @refinedFileName =

	iif(len([dbo].[ufn_RefineFileName](@fileName)) > 0
		, [dbo].[ufn_RefineFileName](@fileName)
		, 'untitled'
	)

	set @retVal =
	
	concat(
		trim(isnull(@stringId, ''))
		, '_'
		, @refinedFileName
	)

	return @retVal
end

GO

--------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
DROP FUNCTION IF EXISTS [dbo].[ufn_PopulateFileName3]
GO

SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO

create function [dbo].[ufn_PopulateFileName3] (
	@fileName nvarchar(255),
	@Id int
)
returns nvarchar(255)
as  
begin
	declare @retVal nvarchar(255)
	declare @refinedFileName nvarchar(255)

	set @refinedFileName =

	iif(len([dbo].[ufn_RefineFileName](@fileName)) > 0
		, [dbo].[ufn_RefineFileName](@fileName)
		, 'untitled'
	)

	set @retVal =
	
	concat(
		@Id
		, '_'
		, @refinedFileName
	)

	return @retVal
end

GO

-- endregion Level 2

--Add CRL assembly to sql server and calling function

--CREATE ASSEMBLY SqlServerClr FROM 'SqlServerClr.dll' --put the full path to DLL here
--go
--CREATE FUNCTION Naturalize(@val as nvarchar(max)) RETURNS nvarchar(1000) 
--EXTERNAL NAME SqlServerClr.UDF.Naturalize
--go
--Then, you can use it like so:

--select *
--from MyTable
--order by dbo.Naturalize(MyTextField)