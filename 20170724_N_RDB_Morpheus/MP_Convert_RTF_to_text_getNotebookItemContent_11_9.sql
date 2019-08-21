SET ANSI_NULLS ON
GO
SET QUOTED_IDENTIFIER ON
GO

--DROP FUNCTION [dbo].[RTF2Text]

CREATE FUNCTION [dbo].[RTF2Text]
(
    @rtf nvarchar(max)
)
RETURNS nvarchar(max)
AS
BEGIN
    DECLARE @Pos1 int;
    DECLARE @Pos2 int;
    DECLARE @hex varchar(316);
    DECLARE @Stage table
    (
        [Char] char(1),
        [Pos] int
    );

    INSERT @Stage
        (
           [Char]
         , [Pos]
        )
    SELECT SUBSTRING(@rtf, [Number], 1)
         , [Number]
      FROM [master]..[spt_values]
     WHERE ([Type] = 'p')
       AND (SUBSTRING(@rtf, Number, 1) IN ('{', '}'));

    SELECT @Pos1 = MIN([Pos])
         , @Pos2 = MAX([Pos])
      FROM @Stage;

    DELETE
      FROM @Stage
     WHERE ([Pos] IN (@Pos1, @Pos2));

    WHILE (1 = 1)
        BEGIN
            SELECT TOP 1 @Pos1 = s1.[Pos]
                 , @Pos2 = s2.[Pos]
              FROM @Stage s1
                INNER JOIN @Stage s2 ON s2.[Pos] > s1.[Pos]
             WHERE (s1.[Char] = '{')
               AND (s2.[Char] = '}')
            ORDER BY s2.[Pos] - s1.[Pos];

            IF @@ROWCOUNT = 0
                BREAK

            DELETE
              FROM @Stage
             WHERE ([Pos] IN (@Pos1, @Pos2));

            UPDATE @Stage
               SET [Pos] = [Pos] - @Pos2 + @Pos1 - 1
             WHERE ([Pos] > @Pos2);

            SET @rtf = STUFF(@rtf, @Pos1, @Pos2 - @Pos1 + 1, '');
        END

    SET @rtf = REPLACE(@rtf, '\pard', '');
    SET @rtf = REPLACE(@rtf, '\par', char(10));
	SET @rtf = REPLACE(@rtf,substring(@rtf,PATINDEX('%TX_RTF%}',@rtf),23),'');
    SET @rtf = STUFF(@rtf, 1, CHARINDEX(' ', @rtf), '');

    WHILE (Right(@rtf, 1) IN (' ', CHAR(13), CHAR(10), '}'))
      BEGIN
        SELECT @rtf = SUBSTRING(@rtf, 1, (LEN(@rtf + 'x') - 2));
        IF LEN(@rtf) = 0 BREAK
      END
    
    SET @Pos1 = CHARINDEX('\''', @rtf);

    WHILE @Pos1 > 0
        BEGIN
            IF @Pos1 > 0
                BEGIN
                    SET @hex = '0x' + SUBSTRING(@rtf, @Pos1 + 2, 2);
                    SET @rtf = REPLACE(@rtf, SUBSTRING(@rtf, @Pos1, 4),

CHAR(CONVERT(int, CONVERT (binary(1), @hex,1))));
                    SET @Pos1 = CHARINDEX('\''', @rtf);
                END
        END

    SET @rtf = @rtf + ' ';

    SET @Pos1 = PATINDEX('%\%[0123456789][\ ]%', @rtf);

    WHILE @Pos1 > 0
        BEGIN
            SET @Pos2 = CHARINDEX(' ', @rtf, @Pos1 + 1);

            IF @Pos2 < @Pos1
                SET @Pos2 = CHARINDEX('\', @rtf, @Pos1 + 1);

            IF @Pos2 < @Pos1
                BEGIN
                    SET @rtf = SUBSTRING(@rtf, 1, @Pos1 - 1);
                    SET @Pos1 = 0;
                END
            ELSE
                BEGIN
                    SET @rtf = STUFF(@rtf, @Pos1, @Pos2 - @Pos1 + 1, ' ');
                    SET @Pos1 = PATINDEX('%\%[0123456789][\ ]%', @rtf);
                END
        END

		IF RIGHT(@rtf, 1) = ' '
		SET @rtf = case when LEN(@rtf) > 0 then SUBSTRING(@rtf, 1, LEN(@rtf) -1) 
		else @rtf end

		--OR: 	WHILE (RIGHT(@rtf, 1) IN (' ', CHAR(0)))
		--		SET @rtf = SUBSTRING(@rtf, 1, LEN(@rtf) -1);

    RETURN LTRIM(RTRIM(@rtf));
END
-------------------
--DROP FUNCTION [dbo].[udf_StripHTML]
CREATE FUNCTION [dbo].[udf_StripHTML] (@HTMLText NVARCHAR(MAX))
RETURNS NVARCHAR(MAX) AS
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
---------------------
--StripCSS
--DROP FUNCTION [dbo].[udf_StripCSS]
CREATE FUNCTION [dbo].[udf_StripCSS]
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
	RETURN REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(REPLACE(
	LTRIM(RTRIM(@CSSText)),'&lt;',''),'&gt;',''),'&nbsp;',' '),'&amp;','&'),'&#8230;','...'),'&#8217;',''''),'&#8211;','-'),'&#8220;','"'),'&#8216;',''''),'&#8221;','"'),'&quot;',''),'&#39;','''')
 END
 GO

--ref --https://www.codeproject.com/Tips/821281/Convert-RTF-to-Plain-Text-Revised-Again


--SELECT NotebookItemId, [dbo].[RTF2Text](Memo) as test
--from NotebookItemContent where NotebookItemId = 31

--SELECT ApplicantId, [dbo].[RTF2Text](ProfileDocument) as test
--from ApplicantProfile where applicantId = 616


create table Convert_NotebookItemContent
(NotebookItemId int PRIMARY KEY,
ItemContent nvarchar(max)
)
go
insert into Convert_NotebookItemContent
SELECT nic.NotebookItemId, 
case
when FileExtension = '.html' then
[dbo].[udf_StripHTML]([dbo].[udf_StripCSS](Memo))
else 
iif(left([dbo].[RTF2TXT2](Memo),5)='{\rtf',[dbo].[RTF2Text](Memo),[dbo].[RTF2TXT2](Memo)) --(cast(nic.Memo as nvarchar(max)))
end as ItemContent
from NotebookItemContent nic
left join NotebookItems ni on nic.NotebookItemId = ni.NotebookItemId-- where nic.NotebookItemId = 23313
where
-- Memo is not null and ltrim(rtrim(Memo)) <> '' and 
 NotebookTypeId in (75,84,53) and
 nic.NoteBookItemId between 2 and 20000-- and nic.NotebookItemId = 5381
-- and FileExtension = '.html'--in ('.rtf','','*.rtf') 

insert into Convert_NotebookItemContent 
SELECT ApplicantId, [dbo].[RTF2Text](ProfileDocument) as ItemContent
from ApplicantProfile --where applicantId = 544
where ProfileDocument is not null and ProfileDocument <> ''

select * from Convert_NotebookItemContent
select * from NotebookItems