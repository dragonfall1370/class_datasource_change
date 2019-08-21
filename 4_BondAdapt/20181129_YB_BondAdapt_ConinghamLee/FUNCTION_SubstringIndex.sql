--/

CREATE FUNCTION SubstringIndex(
    @SourceString varchar(8000),
    @delim char(1),
    @idx int
)
RETURNS TABLE WITH SCHEMABINDING
RETURN
WITH 
E(n) AS(
    SELECT n FROM (VALUES(0),(0),(0),(0),(0),(0),(0),(0),(0),(0))E(n)
),
E2(n) AS(
    SELECT a.n FROM E a, E b
),
E4(n) AS(
    SELECT a.n FROM E2 a, E2 b
),
cteTally(n) AS(
    SELECT TOP(LEN(@SourceString)) ROW_NUMBER() OVER(ORDER BY (SELECT NULL)) n
    FROM E4
),
ctePosition(n) AS(
    SELECT TOP( @idx) n
    FROM cteTally
    WHERE SUBSTRING(@SourceString, n, 1) = @delim
)
SELECT LEFT( @SourceString, MAX(n) - 1) String
FROM ctePosition;       

/
