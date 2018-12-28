--/

CREATE FUNCTION SubString_Index 
(
   @ExistingString NVARCHAR(200),
   @BreakPoint NVARCHAR(10),
   @number INT
)
RETURNS NVARCHAR(200)
AS
BEGIN
DECLARE @Count INT
DECLARE @Substring NVARCHAR(200)
DECLARE @ssubstring NVARCHAR(200)
SET @ssubstring=@ExistingString
DECLARE @scount INT
SET @scount=0
DECLARE @sscount INT
SET @sscount=0
WHILE(@number>@scount)
    BEGIN
            Select @Count=CHARINDEX(@BreakPoint,@ExistingString)
            Select @ExistingString=SUBSTRING(@ExistingString,@Count+1,LEN(@ExistingString))
            Select @scount=@scount+1 
            select @sscount=@sscount+@Count
    END

SELECT @Substring=SUBSTRING(@ssubstring,0,@sscount)

RETURN @Substring
END

/
