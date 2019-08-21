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

    RETURN @rtf;
END

--ref --https://www.codeproject.com/Tips/821281/Convert-RTF-to-Plain-Text-Revised-Again

create table Temp_CandidateResume
(ApplicantId int PRIMARY KEY,
ResumeText nvarchar(max)
)
go

insert into Temp_CandidateResume SELECT ApplicantId, [dbo].[RTF2Text](ProfileDocument) as ResumeText
from ApplicantProfile
where ProfileDocument is not null and ProfileDocument <> ''