DROP FUNCTION IF EXISTS [dbo].[udf_StripHTML]

GO

CREATE FUNCTION [dbo].[udf_StripHTML] (@HTMLText NVARCHAR(MAX))

RETURNS NVARCHAR(MAX) AS
BEGIN
/*
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
*/
 DECLARE @Start  INT
 DECLARE @End    INT
 DECLARE @Length INT
 DECLARE @char	 INT

 SET @HTMLText = REPLACE(REPLACE(REPLACE(REPLACE(
		@HTMLText
  		,N'<br>',nchar(13)+nchar(10))
		,N'<br/>',nchar(13)+nchar(10))
		,N'<br />',nchar(13)+nchar(10))
		,N'</STYLE>',nchar(13)+nchar(10))
 SET @Start = CHARINDEX(N'<',@HTMLText)
 SET @End = CHARINDEX(N'>',@HTMLText,CHARINDEX(N'<',@HTMLText))
 SET @Length = (@End - @Start) + 1

--Remove any text that starts with <  and ends with >
 WHILE @Start > 0 AND @End > 0 AND @Length > 0
 BEGIN
   SET @HTMLText = STUFF(@HTMLText,@Start,@Length,N'')
   SET @Start = CHARINDEX(N'<',@HTMLText, @End-@Length)
   SET @End = CHARINDEX(N'>',@HTMLText,CHARINDEX(N'<',@HTMLText, @Start))
   SET @Length = (@End - @Start) + 1
 END

RETURN  RTRIM(LTRIM(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
		@HTMLText
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
		,N'&#xa0;',nchar(160))
		,N'&#xA7;',nchar(160))
		,N'&#39;',nchar(8220)) --dau nhay don		
		,N'p.std   { margin-top: 0; margin-bottom: 0; border: 0 0 0 0; }',nchar(160))
  ))
END
GO

----------------------------------------------------

--select [dbo].[udf_StripHTML]('<HTML><BODY>test</BODY></HTML>')

