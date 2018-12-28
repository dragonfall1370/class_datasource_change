-------------------02.Contact-------------36,585-----------------------------------------
/*with si as
(
	SELECT t2.UniqueID,
			F17.[75 Email Alphanumeric] as consultant
	FROM F01 t2 
	OUTER APPLY 
	(
		SELECT  Item as Consultant
		FROM    cgh.dbo.[DelimitedSplitN4K] (t2.[105 Consultant Xref],'~')
		where t2.[105 Consultant Xref] is not NULL
	)t
	join F17 on F17.UniqueID = t.Consultant
) 

, i as(  SELECT UniqueID,        STUFF((select ', ' + x.Consultant from si x where x.uniqueID = si.UniqueID for xml path('')), 1,2,'') as Consultant   FROM si  GROUP BY UniqueID )


, px as
(---extract candidate status
	SELECT t2.UniqueID, item, ItemNumber
	FROM F01 t2 
	OUTER APPLY 
	(
		SELECT  *
		FROM    cgh.dbo.[DelimitedSplitN4K] (t2.[38 Phone Alphanumeric],'~')
		where t2.[38 Phone Alphanumeric] is not NULL
	)t
) 
*/


with
-- EMAIL
  mail1 (ID,email) as (select [4 Ref No Numeric], replace([33 E-Mail Alphanumeric],char(9),'') as mail from F01 where [100 Contact Codegroup  23] = 'Y' and [33 E-Mail Alphanumeric] <> '' ) --and [4 Ref No Numeric] in (88780,114533,100014))
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,'~') AS email)
, mail3 (ID,email,rn) as ( SELECT ID, email = ltrim(email), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail2 )
, e1 (ID,email) as (select ID, email from mail3 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail3 where rn = 2)
--select  * from ed where ID in (126974,145162, 131686, 163700)

-- PHONE
, phone1 (ID,email) as (select [4 Ref No Numeric], [38 Ph H W O Alphanumeric] as phone from F01 where [100 Contact Codegroup  23] = 'Y' and [38 Ph H W O Alphanumeric] <> '' ) --and [4 Ref No Numeric] in (88780,114533,100014) )
, phone2 (ID,email) as (SELECT ID, email.value as email FROM phone1 m CROSS APPLY STRING_SPLIT(m.email,'~') AS email)
, phone3 (ID,email,rn) as ( SELECT ID, email = email, r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM phone2 )
, p1 (ID,phone) as (select ID, email from phone3 where rn = 1)
, p2 (ID,phone) as (select ID, email from phone3 where rn = 2)
, p3 (ID,phone) as (select ID, email from phone3 where rn = 3)
, p4 (ID,phone) as (select ID, email from phone3 where rn = 4)
--select  * from phone3 where ID in (88780,114533,100014)

-- DOCUMENT
, scd as ( select distinct UniqueID, REVERSE(LEFT(REVERSE([Relative Document Path]), CHARINDEX('\', REVERSE([Relative Document Path])) - 1)) as FN from F01Docs1)
, cd as (select UniqueID,  STUFF((select ', ' + x.FN from scd x where x.uniqueID = scd.UniqueID for xml path('')), 1,2,'') as FN FROM scd GROUP BY UniqueID)



-----------------------------------https://hrboss.atlassian.net/wiki/spaces/SB/pages/19071424/Requirement+specs+Contact+import------------------------------------------------
SELECT  
        --'BB - ' + cast(a.[4 Ref No Numeric] as varchar(20)) as 'contact-externalId', a.[100 Contact Codegroup  23]
        a.[4 Ref No Numeric] as 'contact-externalId' --, a.[100 Contact Codegroup  23]
       , iif(F02.[6 Ref No Numeric] is null, 'default', F02.[6 Ref No Numeric])  as 'contact-companyId' --, CASE WHEN com.[8 Reference Numeric] IS NULL THEN 'BB00000' ELSE 'BB - ' + cast(com.[8 Reference Numeric] as varchar(20)) END as 'contact-companyId'
       , iif([186 Forenames Alphanumeric] = '', 'Firstname', [186 Forenames Alphanumeric] ) as 'contact-firstName' --[26 Salutation Alphanumeric]
       , iif([185 Surname Alphanumeric] = '',concat('Lastname-',a.[4 Ref No Numeric]), a.[185 Surname Alphanumeric] ) as 'contact-Lastname' --[201 Surname Alphanumeric]
       , CASE
             WHEN [27 Title Codegroup  16] = 'MIS' THEN 'MISS'
             WHEN [27 Title Codegroup  16] IS NULL THEN ''
             ELSE  [27 Title Codegroup  16]  END as 'contact-title'
       , [96 Cont Posn Alphanumeric] as 'contact-jobTitle'
       --, p1.phone as 'contact-homephone'
       , p2.phone as 'contact-phone' --'contact-workphone'
       , p3.phone as 'contact-mobilephone'
       , case
              when ed.rn > 1 and ed.email <> '' then concat(ed.email,'_',ed.rn)
              when ed.rn > 1 and ed.email = '' then concat(a.[4 Ref No Numeric],'@noemailaddress.co')
              else ed.email end as 'contact-email' -- , iif(ed.rn > 1 and ed.email <> '',concat(ed.email,'_',ed.rn), iif(ed.email = '' or ed.email is null, concat(a.[4 Ref No Numeric],'@noemailaddress.co'),ed.email) ) as 'candidate-email'
       , cd.FN as 'contact-document'
       , Stuff(
              Coalesce('Ref No: ' + NULLIF(cast(a.[4 Ref No Numeric] as varchar(max)), '') + char(10), '')
              + Coalesce('Permanent 1: ' + NULLIF(F17.[5 Initials Alphanumeric], '') + char(10), '')
              + Coalesce('Permanent 2: ' + NULLIF(F17.[1 Name Alphanumeric], '') + char(10), '')
              , 1, 0, '') as note
       , ind.description as 'industry', [221 Area Codegroup 132]
       , cat.description as 'category', [127 Job Cat Codegroup  24]
       , Stuff( coalesce(' ' + NULLIF(province.Description, ''), '') + Coalesce(', ' + NULLIF(tmp_country.COUNTRY, ''), ''), 1, 1, '') as 'contact-locationAddress'
       , Stuff( coalesce(' ' + NULLIF(province.Description, ''), '') + Coalesce(', ' + NULLIF(tmp_country.COUNTRY, ''), ''), 1, 1, '') as 'contact-locationName'
       , tmp_country.ABBREVIATION as 'contact-country'
-- select count(*) --64452
FROM F01 a --where [100 Contact Codegroup  23] = 'Y' --54210
left join (SELECT * FROM CODES WHERE Codegroup = 132) as ind on ind.code = a.[221 Area Codegroup 132]
left join (SELECT * FROM CODES WHERE Codegroup = 24) as cat on cat.code = a.[127 Job Cat Codegroup  24]
left join (SELECT * FROM CODES WHERE Codegroup = 26) as  province on province.Code = a.[22 Province Codegroup  26]
left join tmp_country on tmp_country.CODE = a.[8 Country Codegroup 133] --countrycode
left join F17 on F17.[UniqueID] = a.[105 Perm Cons Xref]
left join F02 on F02.UniqueID = a.[16 Employer Xref]
left join ed on ed.ID = a.[4 Ref No Numeric]
left join e2 on e2.ID = a.[4 Ref No Numeric]
left join p1 on p1.ID = a.[4 Ref No Numeric]
left join p2 on p2.ID = a.[4 Ref No Numeric]
left join p3 on p3.ID = a.[4 Ref No Numeric]
--LEFT JOIN i on i.UniqueID = a.UniqueID
left join F01Document_contact cd on cd.UniqueID = a.UniqueID --left join cd on cd.UniqueID = a.UniqueID
--left join(select * from px where px.ItemNumber = 2) as px1 on px1.uniqueid = a.uniqueid
where a.[100 Contact Codegroup  23] = 'Y'
--and F02.UniqueID = '80810201BCDF8080'
and a.[4 Ref No Numeric] in (88780,114533,100014)




-- CUSTOM FIELD
SELECT  TOP 100
        a.[4 Ref No Numeric] as 'contact-externalId'
       , 'add_con_info' as additional_type
       , 1006 as form_id
       , 1017 as field_id        
       , cat.description as 'category', [127 Job Cat Codegroup  24]
       , ind.description as 'industry', [221 Area Codegroup 132]
       , sub.description as 'industry', [162 Sub Ind Codegroup 128]
-- select count(*) --64452
FROM F01 a --where [100 Contact Codegroup  23] = 'Y' --54210
left join (SELECT * FROM CODES WHERE Codegroup = 24) as cat on cat.code = a.[127 Job Cat Codegroup  24]
left join (SELECT * FROM CODES WHERE Codegroup = 132) as ind on ind.code = a.[221 Area Codegroup 132]
left join (SELECT * FROM CODES WHERE Codegroup = 128) as sub on sub.code = a.[162 Sub Ind Codegroup 128]
where a.[100 Contact Codegroup  23] = 'Y'


-- INDUSTRY
-- select distinct a.[221 Area Codegroup 132] FROM F01 a
SELECT
        a.[4 Ref No Numeric] as 'contact-externalId'
       , ind.description as 'industry', [221 Area Codegroup 132]
-- select count(*) -- select distinct ind.description
FROM F01 a --where [100 Contact Codegroup  23] = 'Y' --54210
left join (SELECT * FROM CODES WHERE Codegroup = 132) as ind on ind.code = a.[221 Area Codegroup 132]
where a.[100 Contact Codegroup  23] = 'Y' and a.[221 Area Codegroup 132] <> ''




-- SUB INDUSTRY
-- select distinct a.[162 Sub Ind Codegroup 128] FROM F01 a
with
  val1 (ID,val) as (select [4 Ref No Numeric], replace([162 Sub Ind Codegroup 128],char(9),'') as val from F01 where [162 Sub Ind Codegroup 128] <> '' ) --and [4 Ref No Numeric] in (87721, 114602) )
, val2 (ID,val) as (SELECT ID, val.value as val FROM val1 m CROSS APPLY STRING_SPLIT(m.val,'~') AS val)
, val3 (ID,field_value) as (
       select ID,
       case
              when val = '1' then 1
              when val = '41' then 2
              when val = '42' then 3
              when val = '15' then 4
              when val = '43' then 5
              when val = '3' then 6
              when val = '26' then 7
              when val = '31' then 8
              when val = '62' then 9
              when val = '36' then 10
              when val = '47' then 11
              when val = '7' then 12
              when val = '35' then 13
              when val = '37' then 14
              when val = '8' then 15
              when val = '33' then 16
              when val = '64' then 17
              when val = 'FT' then 18
              when val = '9' then 19
              when val = '27' then 20
              when val = '10' then 21
              when val = '39' then 22
              when val = '11' then 23
              when val = '66' then 24
              when val = '12' then 25
              when val = '13' then 26
              when val = '32' then 27
              when val = '61' then 28
              when val = '24' then 29
              when val = '48' then 30
              when val = '49' then 31
              when val = '38' then 32
              when val = '41' then 33
              when val = '40' then 34
              when val = '34' then 35
              when val = '18' then 36
              when val = '19' then 37
              when val = '46' then 38
              when val = '28' then 39
              when val = '14' then 40
              when val = '25' then 41
              when val = '29' then 42
              when val = '20' then 43
              when val = '5' then 44
              when val = '21' then 45
              when val = '44' then 46
              when val = '65' then 47
              when val = '30' then 48
              when val = '22' then 49
              when val = '23' then 50
              when val = '63' then 51
              when val = '45' then 52
              end as val
       from val2              
       )
, val4 (ID,field_value) as (select ID,  STUFF((select ',' + convert(varchar(10),x.field_value) from val3 x where x.ID = val3.ID for xml path('')), 1,1,'') as field_value FROM val3 GROUP BY ID)
--select * from val4
       
SELECT
        a.[4 Ref No Numeric] as 'contact-externalId'
       , 'add_con_info' as additional_type
       , 1007 as form_id
       , 1020 as field_id        
       , sub.description as 'industry', [162 Sub Ind Codegroup 128]
       , val4.field_value
-- select count(*) -- select top 100 * -- select distinct sub.description
FROM F01 a --where [100 Contact Codegroup  23] = 'Y' --54210
left join (SELECT * FROM CODES WHERE Codegroup = 128) as sub on sub.code = a.[162 Sub Ind Codegroup 128]
left join val4 on val4.ID = a.[4 Ref No Numeric]
where a.[100 Contact Codegroup  23] = 'Y' and a.[162 Sub Ind Codegroup 128] <> ''








-- CATEGORY
-- select distinct a.[127 Job Cat Codegroup  24] FROM F01 a
with
  val1 (ID,val) as (select [4 Ref No Numeric], replace([127 Job Cat Codegroup  24],char(9),'') as val from F01 where [127 Job Cat Codegroup  24] <> '' ) --and [4 Ref No Numeric] in (87721, 114602) )
, val2 (ID,val) as (SELECT ID, val.value as val FROM val1 m CROSS APPLY STRING_SPLIT(m.val,'~') AS val)
, val3 (ID,field_value) as (
       select ID,
       case
              when val = '1' then 1
              when val = '2' then 2
              when val = '3' then 3
              when val = '176' then 4
              when val = '209' then 5
              when val = '1021' then 110
              when val = '5' then 6
              when val = '227' then 7
              when val = '305' then 8
              when val = '177' then 9
              when val = '7' then 111
              when val = '10' then 10
              when val = '12' then 112
              when val = '15' then 11
              when val = '146' then 12
              when val = '18' then 13
              when val = '192' then 14
              when val = '19' then 113
              when val = '208' then 15
              when val = '306' then 16
              when val = '225' then 17
              when val = '304' then 18
              when val = '203' then 19
              when val = '24' then 114
              when val = '25' then 115
              when val = '28' then 20
              when val = '31' then 21
              when val = '32' then 22
              when val = '37' then 23
              when val = '202' then 24
              when val = '210' then 25
              when val = '39' then 116
              when val = '180' then 26
              when val = '42' then 27
              when val = '174' then 28
              when val = '44' then 29
              when val = '228' then 30
              when val = '50' then 117
              when val = '53' then 31
              when val = '1023' then 32
              when val = '1024' then 33
              when val = '54' then 34
              when val = '1025' then 35
              when val = '56' then 36
              when val = '221' then 37
              when val = '57' then 118
              when val = '302' then 38
              when val = '314' then 39
              when val = '58' then 40
              when val = '59' then 41
              when val = '103' then 42
              when val = '186' then 43
              when val = '191' then 44
              when val = '179' then 45
              when val = '188' then 46
              when val = '184' then 47
              when val = '213' then 48
              when val = '67' then 49
              when val = '211' then 50
              when val = '69' then 51
              when val = '70' then 52
              when val = '72' then 53
              when val = '76' then 54
              when val = '77' then 119
              when val = '315' then 55
              when val = '308' then 56
              when val = '79' then 57
              when val = '181' then 58
              when val = '312' then 59
              when val = '88' then 60
              when val = '404' then 61
              when val = '212' then 62
              when val = '173' then 63
              when val = '224' then 64
              when val = '96' then 65
              when val = '98' then 120
              when val = '401' then 66
              when val = '178' then 67
              when val = '101' then 68
              when val = '102' then 69
              when val = '301' then 70
              when val = '108' then 121
              when val = '81' then 71
              when val = '307' then 72
              when val = '110' then 122
              when val = '316' then 73
              when val = '309' then 74
              when val = '226' then 123
              when val = '215' then 124
              when val = '114' then 75
              when val = '115' then 76
              when val = '406' then 77
              when val = '119' then 78
              when val = '214' then 125
              when val = '190' then 79
              when val = '121' then 80
              when val = '217' then 81
              when val = '130' then 126
              when val = '207' then 82
              when val = '205' then 83
              when val = '222' then 84
              when val = '405' then 85
              when val = '131' then 86
              when val = '317' then 87
              when val = '187' then 88
              when val = '133' then 89
              when val = '1022' then 90
              when val = '175' then 91
              when val = '136' then 92
              when val = '204' then 93
              when val = '137' then 94
              when val = '138' then 127
              when val = '206' then 95
              when val = '141' then 96
              when val = '218' then 97
              when val = '200' then 98
              when val = '201' then 99
              when val = '153' then 128
              when val = '230' then 129
              when val = '223' then 100
              when val = '157' then 130
              when val = '158' then 101
              when val = '407' then 102
              when val = '182' then 103
              when val = '229' then 104
              when val = '165' then 105
              when val = '167' then 131
              when val = '170' then 132
              when val = '402' then 106
              when val = '171' then 107
              when val = '172' then 108
              when val = '403' then 109
              end as val
       from val2              
       )
, val4 (ID,field_value) as (select ID,  STUFF((select ',' + convert(varchar(10),x.field_value) from val3 x where x.ID = val3.ID for xml path('')), 1,1,'') as field_value FROM val3 GROUP BY ID)
--select * from val4

SELECT
        a.[4 Ref No Numeric] as 'contact-externalId'
       , 'add_con_info' as additional_type
       , 1007 as form_id
       , 1019 as field_id        
       , cat.description as 'category', [127 Job Cat Codegroup  24]
       , val4.field_value
-- select count(*) -- select distinct cat.description
FROM F01 a --where [100 Contact Codegroup  23] = 'Y' --54210
left join (SELECT * FROM CODES WHERE Codegroup = 24) as cat on cat.code = a.[127 Job Cat Codegroup  24]
left join val4 on val4.ID = a.[4 Ref No Numeric]
where a.[100 Contact Codegroup  23] = 'Y' and a.[127 Job Cat Codegroup  24] <> ''


