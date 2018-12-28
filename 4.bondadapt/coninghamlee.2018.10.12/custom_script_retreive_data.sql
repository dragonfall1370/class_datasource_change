------------Company activities-1,048,680 records-----------------------

select b.[8 Reference Numeric] as company_externalid
    , con.[4 RefNumber Numeric] as contact_externalid
	, job.[1 Job Ref Numeric] as job_externalId
	, b.[1 Name Alphanumeric] as company
	, con.[1 Name Alphanumeric]  as contact
	, [Create Date],  c.code ,  c.Description as actiondesc
	, u.[1 Name Alphanumeric] as consultant
	, job.[3 Job Title Alphanumeric] as job
    , case when [Notes 1] is NULL and [Notes 2] is null and [Notes 3] is null and [Notes 4] is null and [Notes 5] is null and [Notes 6] is null and [Notes 7] is null then '' 
	  else 'Notes: ' end
	+ coalesce(a.[Notes 1], '')
	+ coalesce(' ' + a.[Notes 2], '')
	+ coalesce(' ' + a.[Notes 3], '')
	+ coalesce(' ' + a.[Notes 4], '')
	+ coalesce(' ' + a.[Notes 5], '')
	+ coalesce(' ' + a.[Notes 6], '')
	+ coalesce(' ' + a.[Notes 7], '')as Notes

from act as a
LEFT JOIN F02 as b on b.UniqueID = a.[Field 2]
LEFT JOIN(SELECT * FROM CODES WHERE Codegroup = 94) as c on c.Code = a.[Field 5]
LEFT JOIN F17 as u on u.UniqueID = a.[Field 4]
LEFT JOIN(SELECT UniqueID, [1 Name Alphanumeric],[4 RefNumber Numeric]  FROM F01) as con on con.UniqueID = a.[Field 1]
LEFT JOIN F03 as job on job.UniqueID = a.[Field 3]
where 1=1
--and [Field 2] is not NULL
 and b.[2 Type Codegroup   2] = 2 --migrate site only
 and [Field 5] in('CCC', 'CC', 'FN')
--and [Field 2] in( '80810201E4D28080', '8081020186828080')


if OBJECT_ID(N'temp..#clienthasjobcontact') is not null 
  drop table #clienthasjobcontact
select distinct com.UniqueID
     --, com.[1 Name Alphanumeric] as company
     -- ,c.contact
     -- , d.job
into #clienthasjobcontact
from f02 as com
outer apply
(
select [1 Name Alphanumeric] as contact
 from f01 as con
--where [43 Client Xref] = '80810201D8818080'
where [16 Site Xref] is null and [43 Client Xref] is not null
and con.[43 Client Xref] = com.UniqueID
) as c
outer apply
(

select [3 Job Title Alphanumeric] as job
 from f03 as job
where [2 Site Xref] is null and [152 Client Xref] is not null
and job.[152 Client Xref] = com.UniqueID
) as d
where job is not null or contact is not null

--------------------------------------------------------------------------------