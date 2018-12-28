DROP FUNCTION IF EXISTS [dbo].[fn_ConvertHTMLToText]
GO

CREATE FUNCTION fn_ConvertHTMLToText(@Text NVARCHAR(MAX))  
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
