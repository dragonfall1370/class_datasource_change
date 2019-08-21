--/

CREATE FUNCTION SubstringIndex_AJB(@SourceStr varchar(8000), @delim char(1), @idx int)
RETURNS TABLE AS RETURN 
  SELECT NewString = SUBSTRING(@sourceStr,1,MAX(x.d)-1)
  FROM (VALUES (@SourceStr)) s(string)
  CROSS APPLY (VALUES (1,NULLIF(CHARINDEX(@delim, s.string,     0),0))) s1(dn, d)
  CROSS APPLY (VALUES (2,NULLIF(CHARINDEX(@delim, s.string,s1.d+1),0))) s2(dn, d)
  CROSS APPLY (VALUES (3,NULLIF(CHARINDEX(@delim, s.string,s2.d+1),0))) s3(dn, d)
  CROSS APPLY (VALUES (4,NULLIF(CHARINDEX(@delim, s.string,s3.d+1),0))) s4(dn, d)
  CROSS APPLY (VALUES (5,NULLIF(CHARINDEX(@delim, s.string,s4.d+1),0))) s5(dn, d)
  CROSS APPLY (VALUES (6,NULLIF(CHARINDEX(@delim, s.string,s5.d+1),0))) s6(dn, d)
  CROSS APPLY 
  (
    SELECT d, dn 
    FROM (
      VALUES (s1.d,s1.dn), (s2.d,s2.dn), (s3.d,s3.dn), 
             (s4.d,s4.dn), (s5.d,s5.dn), (s6.d,s6.dn)) x(d,dn)
    WHERE dn = @idx
  ) x;
/
