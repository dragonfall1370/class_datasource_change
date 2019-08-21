
----------------------------04_BOND JOB-----------12,655----(868 temp job(jobtype =6,7,16) -> skip)-----11,787perm--
with
-- EMAIL
  sal1 (ID,sal) as (select [1 Job Ref Numeric], [32 Sal From Numeric] as sal from F03 where [32 Sal From Numeric] <> '' )
, sal2 (ID,sal) as (SELECT ID, sal.value as sal FROM sal1 m CROSS APPLY STRING_SPLIT(m.sal,'~') AS sal)
, sal3 (ID,sal,rn) as ( SELECT ID, sal = sal, r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM sal2 )
, s1 (ID,sal) as (select ID, sal from sal3 where rn = 1)
, s2 (ID,sal) as (select ID, sal from sal3 where rn = 2)
--select  * from mail3 where ID in (88780,114533,100014)

-- DOCUMENT
, scd as (select distinct UniqueID  , REVERSE(LEFT(REVERSE([Relative Document Path]), CHARINDEX('\', REVERSE([Relative Document Path])) - 1)) as FN from F03Docs3 )
, cd as ( select UniqueID ,  STUFF((select ', ' + x.FN from scd x where x.uniqueID = scd.UniqueID for xml path('')), 1,2,'') as FN FROM scd GROUP BY UniqueID)

select
       --'BB - ' +  cast(a.[1 Job Ref Numeric] as varchar(20)) as 'position-externalId' , a.UniqueID
         a.[1 Job Ref Numeric] as 'position-externalId' --, a.UniqueID
       , coalesce( cast(con.[4 Ref No Numeric] as varchar(20)), 'default') as 'position-contactId', con.firstname, con.lastname --[20 Contact Xref] --4 RefNumber Numeric --coalesce('BB - ' +  cast(a.[20 Contact Xref] as varchar(20)), 'BB00000') as 'position-contactId'
       , com.[6 Ref No Numeric] as 'position-companyId' , com.companyName --'BB - ' + cast(a.[2 Company Xref] as varchar(20)) as 'position-companyId'
       , case
              when [3 Position Alphanumeric] = '' then 'No JobTitle' + cast(a.[1 Job Ref Numeric] as varchar(20))
              when [3 Position Alphanumeric] is NULL then 'No JobTitle' + cast(a.[1 Job Ref Numeric] as varchar(20))
              when [3 Position Alphanumeric] is not null and rnk = 1 then [3 Position Alphanumeric] 
              else concat(cast(a.[1 Job Ref Numeric] as varchar(20)),' ',[3 Position Alphanumeric]) end as 'position-title'
              --else 'dup_' +  cast(a.[1 Job Ref Numeric] as varchar(20)) + '_' + [3 Position Alphanumeric] end as 'position-title'
--, [185 JobTitle Alphanumeric]
--, coalesce([74 No Reqd Numeric], 1) as 'position-headcount'
       , coalesce(F17.[72 Email Add Alphanumeric], '') as 'position-owners'--[7 Consultant Xref] --75 Email Alphanumeric
--, jt.Description as 'position-type' --[6 Job Type Codegroup 178]--5 PERMANENT, INTERIM_PROJECT_CONSULTING, TEMPORARY, CONTRACT. TEMPORARY_TO_PERMANENT--default PERMANENT
/*   , CASE WHEN [6 Job Type Codegroup 178] in('01', '08', '09') THEN 'CONTRACT'--'RPO'
              WHEN [6 Job Type Codegroup 178] in('10', '14', '15') THEN 'TEMPORARY_TO_PERMANENT'--'OPRA'
              WHEN [6 Job Type Codegroup 178] in('03', '04') THEN 'INTERIM_PROJECT_CONSULTING'--'Perm Contingent'
              WHEN [6 Job Type Codegroup 178] in('02', '05') THEN 'PERMANENT'--'Perm Retained'
              else 'PERMANENT'
              END as 'position-type'*/
       , 'PERMANENT' as 'position-type'
/*  , CASE [196 WorkType Codegroup 165]
              WHEN 1 THEN 'FULL_TIME'
              WHEN 2 THEN 'PART_TIME'
              WHEN 3 THEN 'CASUAL'
              WHEN 4 THEN 'CASUAL' else 'FULL_TIME'
              END as 'position-employmentType'--6 --FULL_TIME, PART_TIME, CASUAL --default FULL_TIME*/
--,  as 'position-currency'--$
--, [13 package Fr Numeric] as 'package from-to'
       --, coalesce([147 Salary 2 Numeric],'') as 'position_actualSalary'
       , s1.sal as 'position-salaryFrom', s2.sal as 'position-salaryTo'--, [32 Sal From Numeric] 
--, [43 PAY STD Numeric] as 'position-payRate' --??or [33 Charge Std Numeric]chrom 
--, [194 Duration Numeric] as 'position-contractLength' --days
--, wu.Description as 'position-contractUnit'--[158 UOM Codegroup 141]
--, np.Description as 'position-noticePeriod'--[198 Notice Codegroup 171]
       --,  '' as 'position-publicDescription'
       , Stuff(  
              Coalesce('Locations: ' + NULLIF(cast(l.description as varchar(max)), '') + char(10), '')
              , 1, 0, '') as 'position-internalDescription'
--  , convert(date,[21 Created Date],103) as 'position-startDate'
--  , coalesce(convert(date, [154 Job Closed Date], 103),'') as 'position-endDate'
       , coalesce(cd.FN, '') 'position-document'--filename
       , Stuff(  
              Coalesce('Ref No: ' + NULLIF(cast(a.[1 Job Ref Numeric]  as varchar(max)), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(cast(s.description as varchar(max)), '') + char(10), '')
              , 1, 0, '') as 'position-note'
/*       , 'Job External ID: BB - ' + cast(a.[1 Job Ref Numeric] as varchar(20)) + char(10)  
              + coalesce('Package Details: ' + a.[31 PackageNte Alphanumeric] + char(10), '') 
              + coalesce('Contact Flat Fee $: ' + a.[39 Flat Fee Numeric] + char(10), '')
              + coalesce('Job Fee %: ' + a.[41 Fee % Numeric] + char(10), '')  as 'position-note'
       , coalesce([92 Notes Alphanumeric],'') as 'position-comment'*/

       , ind.description as 'industry', a.[128 Industry Codegroup 127]
       , cat.description as 'category' , a.[100 Job Cat Codegroup  24]       
-- select top 100 * 
from (select ROW_NUMBER() over(partition by [3 Position Alphanumeric] order by [1 Job Ref Numeric]) as rnk, * from F03) a
LEFT JOIN cd on cd.UniqueID = a.UniqueID
left join s1 on s1.ID = a.[1 Job Ref Numeric]
left join s2 on s2.ID = a.[1 Job Ref Numeric]
left join (SELECT * FROM CODES WHERE Codegroup = 6) s on s.code = a.[5 Status Codegroup   6]
left join (SELECT * FROM CODES WHERE Codegroup = 26) l on l.code = a.[83 Locations Codegroup  26]
left join (select UniqueID as id, [4 Ref No Numeric], [186 Forenames Alphanumeric] as firstName, [185 Surname Alphanumeric] as lastname from F01 where [100 Contact Codegroup  23] = 'Y') con on con.id = a.[20 Contact Xref] --[4 Ref No Numeric] 
left join (select UniqueID as id, [6 Ref No Numeric], [1 Name Alphanumeric] as companyName from F02 )  com on com.id = a.[2 Company Xref] --[6 Ref No Numeric]
left join F17 on F17.[UniqueID] = a.[7 Consultant Xref]

left join (SELECT * FROM CODES WHERE Codegroup = 132) as ind on ind.code = a.[98 Area Codegroup 132] --a.[128 Industry Codegroup 127]
left join (SELECT * FROM CODES WHERE Codegroup = 24) as cat on cat.code = a.[100 Job Cat Codegroup  24]
where [1 Job Ref Numeric] in (4118, 15481, 7362)


/*LEFT JOIN 
       (     SELECT UniqueID, [38 Phone Alphanumeric]
                , [121 EmailCont Alphanumeric]
                , [4 RefNumber Numeric]
              FROM F01 WHERE [16 Site Xref] is not null 
       ) as con on con.UniqueID = a.[20 Contact Xref]
LEFT JOIN F02 as co on co.UniqueID = a.[2 Site Xref]
LEFT JOIN(SELECT * FROM CODES WHERE Codegroup = 178) as jt on jt.Code = a.[6 Job Type Codegroup 178]
LEFT JOIN F17 as jo on jo.UniqueID = a.[7 Consultant Xref]
LEFT JOIN(SELECT * FROM CODES WHERE Codegroup = 141) as wu on wu.Code = a.[158 UOM Codegroup 141]
LEFT JOIN(SELECT * FROM CODES WHERE Codegroup = 171) as np on np.Code = a.[198 Notice Codegroup 171]
where [6 Job Type Codegroup 178] <> 16 --NOT migrate job type = other
--where a.[6 Job Type Codegroup 178]not in('06','07','16')--MIGRATE PERMANENT JOBS ONLY
*/





-- SALARY
with
-- EMAIL
  sal1 (ID,sal) as (select [1 Job Ref Numeric], [32 Sal From Numeric] as sal from F03 where [32 Sal From Numeric] <> '' )
, sal2 (ID,sal) as (SELECT ID, sal.value as sal FROM sal1 m CROSS APPLY STRING_SPLIT(m.sal,'~') AS sal)
, sal3 (ID,sal,rn) as ( SELECT ID, sal = sal, r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM sal2 )
, s1 (ID,sal) as (select ID, sal from sal3 where rn = 1)
, s2 (ID,sal) as (select ID, sal from sal3 where rn = 2)
--select  * from mail3 where ID in (88780,114533,100014)

select
         a.[1 Job Ref Numeric] as 'position-externalId'
       , case
              when [3 Position Alphanumeric] = '' then 'No JobTitle' + cast(a.[1 Job Ref Numeric] as varchar(20))
              when [3 Position Alphanumeric] is NULL then 'No JobTitle' + cast(a.[1 Job Ref Numeric] as varchar(20))
              when [3 Position Alphanumeric] is not null and rnk = 1 then [3 Position Alphanumeric] 
              else concat(cast(a.[1 Job Ref Numeric] as varchar(20)),' ',[3 Position Alphanumeric]) end as 'position-title'
       , s1.sal as 'position-salaryFrom', s2.sal as 'position-salaryTo'--, [32 Sal From Numeric] 
from(select ROW_NUMBER() over(partition by [3 Position Alphanumeric] order by [1 Job Ref Numeric]) as rnk, * from F03 )as a
left join s1 on s1.ID = a.[1 Job Ref Numeric]
left join s2 on s2.ID = a.[1 Job Ref Numeric]





-- INDUSTRY
select
       a.[1 Job Ref Numeric] as 'position-externalId'
       , a.[98 Area Codegroup 132], ind.description as 'job-industry'
-- select top 10 * -- select distinct ind.description
from(select ROW_NUMBER() over(partition by [3 Position Alphanumeric] order by [1 Job Ref Numeric]) as rnk, * from F03 )as a
left join (SELECT * FROM CODES WHERE Codegroup = 132) as ind on ind.code = a.[98 Area Codegroup 132] --a.[128 Industry Codegroup 127]
where ind.description is not null --a.[128 Industry Codegroup 127] <> ''



-- CATEGORY
with
  val1 (ID,val) as (select [1 Job Ref Numeric], replace([100 Job Cat Codegroup  24],char(9),'') as val from F03 where [100 Job Cat Codegroup  24] <> '' ) --and [4 Ref No Numeric] in (87721, 114602) )
, val2 (ID,val) as (SELECT ID, val.value as val FROM val1 m CROSS APPLY STRING_SPLIT(m.val,'~') AS val)
, val3 (ID,field_value) as (
       select ID,
       case
              when val = '1' then 1
              when val = '2' then 2
              when val = '3' then 3
              when val = '176' then 4
              when val = '209' then 5
              when val = '1021' then 6
              when val = '5' then 7
              when val = '227' then 8
              when val = '305' then 9
              when val = '177' then 10
              when val = '7' then 11
              when val = '10' then 12
              when val = '12' then 13
              when val = '15' then 14
              when val = '146' then 15
              when val = '18' then 16
              when val = '192' then 17
              when val = '19' then 18
              when val = '208' then 19
              when val = '306' then 20
              when val = '225' then 21
              when val = '304' then 22
              when val = '203' then 23
              when val = '24' then 24
              when val = '25' then 25
              when val = '28' then 26
              when val = '31' then 27
              when val = '32' then 28
              when val = '37' then 29
              when val = '202' then 30
              when val = '210' then 31
              when val = '39' then 32
              when val = '180' then 33
              when val = '42' then 34
              when val = '174' then 35
              when val = '44' then 36
              when val = '228' then 37
              when val = '50' then 38
              when val = '53' then 39
              when val = '1023' then 40
              when val = '1024' then 41
              when val = '54' then 42
              when val = '1025' then 43
              when val = '56' then 44
              when val = '221' then 45
              when val = '57' then 46
              when val = '302' then 47
              when val = '314' then 48
              when val = '58' then 49
              when val = '59' then 50
              when val = '103' then 51
              when val = '186' then 52
              when val = '191' then 53
              when val = '179' then 54
              when val = '188' then 55
              when val = '184' then 56
              when val = '213' then 57
              when val = '67' then 58
              when val = '211' then 59
              when val = '69' then 60
              when val = '70' then 61
              when val = '72' then 62
              when val = '76' then 63
              when val = '77' then 64
              when val = '315' then 65
              when val = '308' then 66
              when val = '79' then 67
              when val = '181' then 68
              when val = '312' then 69
              when val = '88' then 70
              when val = '404' then 71
              when val = '212' then 72
              when val = '173' then 73
              when val = '224' then 74
              when val = '96' then 75
              when val = '98' then 76
              when val = '401' then 77
              when val = '178' then 78
              when val = '101' then 79
              when val = '102' then 80
              when val = '301' then 81
              when val = '108' then 82
              when val = '81' then 83
              when val = '307' then 84
              when val = '110' then 85
              when val = '316' then 86
              when val = '309' then 87
              when val = '226' then 88
              when val = '215' then 89
              when val = '114' then 90
              when val = '115' then 91
              when val = '406' then 92
              when val = '119' then 93
              when val = '214' then 94
              when val = '190' then 95
              when val = '121' then 96
              when val = '217' then 97
              when val = '130' then 98
              when val = '207' then 99
              when val = '205' then 100
              when val = '222' then 101
              when val = '405' then 102
              when val = '131' then 103
              when val = '317' then 104
              when val = '187' then 105
              when val = '133' then 106
              when val = '1022' then 107
              when val = '175' then 108
              when val = '136' then 109
              when val = '204' then 110
              when val = '137' then 111
              when val = '138' then 112
              when val = '206' then 113
              when val = '141' then 114
              when val = '218' then 115
              when val = '200' then 116
              when val = '201' then 117
              when val = '153' then 118
              when val = '230' then 119
              when val = '223' then 120
              when val = '157' then 121
              when val = '158' then 122
              when val = '407' then 123
              when val = '182' then 124
              when val = '229' then 125
              when val = '165' then 126
              when val = '167' then 127
              when val = '170' then 128
              when val = '402' then 129
              when val = '171' then 130
              when val = '172' then 131
              when val = '403' then 132
              end as val
       from val2              
       )
, val4 (ID,field_value) as (select ID,  STUFF((select ',' + convert(varchar(10),x.field_value) from val3 x where x.ID = val3.ID for xml path('')), 1,1,'') as field_value FROM val3 GROUP BY ID)
--select * from val4

select
         a.[1 Job Ref Numeric] as 'position-externalId'
       , 'add_job_info' as additional_type
       , 1008 as form_id
       , 1022 as field_id        
       , cat.description as 'category' , a.[100 Job Cat Codegroup  24]
       , val4.field_value         
-- select top 10 * -- select distinct cat.description
from(select ROW_NUMBER() over(partition by [3 Position Alphanumeric] order by [1 Job Ref Numeric]) as rnk, * from F03 )as a
left join (SELECT * FROM CODES WHERE Codegroup = 24) as cat on cat.code = a.[100 Job Cat Codegroup  24]
left join val4 on val4.ID = a.[1 Job Ref Numeric]
where cat.description is not null --a.[128 Industry Codegroup 127] <> ''

