
/****** Object:  UserDefinedFunction [dbo].[RTF2Text]    Script Date: 24/10/2017 12:12:36 ******/
SET ANSI_NULLS ON
GO

SET QUOTED_IDENTIFIER ON
GO


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
    SELECT SUBSTRING(@rtf, [number], 1)
         , [number]
      FROM [master]..[spt_values]
     WHERE ([type] = 'p')
       AND (SUBSTRING(@rtf, number, 1) IN ('{', '}'));

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

    SET @rtf = REPLACE(@rtf, '{\rtf1\ansi{\fonttbl{\f0 MS Sans Serif;}}', '');
    SET @rtf = REPLACE(@rtf, '\par', '');
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
                    SET @rtf = STUFF(@rtf, @Pos1, @Pos2 - @Pos1 + 1, '');
                    SET @Pos1 = PATINDEX('%\%[0123456789][\ ]%', @rtf);
                END
        END

    IF RIGHT(@rtf, 1) = ' '
        SET @rtf = SUBSTRING(@rtf, 1, LEN(@rtf) -1);

    RETURN @rtf;
END
GO


