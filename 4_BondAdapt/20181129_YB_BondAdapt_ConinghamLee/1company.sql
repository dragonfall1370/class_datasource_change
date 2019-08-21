
/*
--- FIRST STEP ---
update F02
set [25 Address Alphanumeric] = 
              ltrim(case
              when LEFT([25 Address Alphanumeric], 3) = '~~~' then right([25 Address Alphanumeric], LEN([25 Address Alphanumeric]) - 3)
              when LEFT([25 Address Alphanumeric], 2) = '~~' then right([25 Address Alphanumeric], LEN([25 Address Alphanumeric]) - 2)
              when LEFT([25 Address Alphanumeric], 1) = '~' then right([25 Address Alphanumeric], LEN([25 Address Alphanumeric]) - 1)
              else [25 Address Alphanumeric] end )
where [25 Address Alphanumeric] <> ''*/


--------------------01.Company-------14,747-----------------------------------------------
--https://hrboss.atlassian.net/wiki/spaces/SB/pages/580157441/15.2.4.+Bond+Adapt+V9
with 
-- CONSULTANT
sc as (
	SELECT t2.UniqueID, F17.[72 Email Add Alphanumeric]as consultant --75 Email Alphanumeric
	FROM F02 t2 
	OUTER APPLY 
              ( SELECT  Item as Consultant 
                FROM dbo.[DelimitedSplitN4K] (t2.[57 Perm Cons Xref],'~') --4 Consultant Xref
                where t2.[57 Perm Cons Xref] is not NULL --4 Consultant Xref
              ) t
	join F17 on F17.UniqueID = t.Consultant
)

, c as( SELECT UniqueID, STUFF((select ', ' + x.Consultant from sc x where x.uniqueID = sc.UniqueID for xml path('')), 1,2,'') as Consultant FROM sc GROUP BY UniqueID )
--select * from c

/*, scc as
(
	SELECT t2.UniqueID
			, c.VinCountryCode
			, c.description
			, ROW_NUMBER() over(partition by UniqueID order by Vincountrycode) rnk
	FROM F02 t2 
	OUTER APPLY 
	(
		SELECT  Item as code
		FROM    dbo.[DelimitedSplitN4K] (t2.[8 Country Codegroup 133],'~') --137 Country Codegroup 146
		where t2.[8 Country Codegroup 133] is not NULL --137 Country Codegroup 146
	)t
	join dbo.[CountryCodeMapping]  as c on c.Code = t.code 
)
, cc as( SELECT VinCountryCode, UniqueID, Description FROM scc WHERE rnk = 1 ) */


-- DOCUMENT
, scd as ( select distinct UniqueID, REVERSE(LEFT(REVERSE([Relative Document Path]), CHARINDEX('\', REVERSE([Relative Document Path])) - 1)) as FN from F02Docs2 )
, cd as ( select UniqueID, STUFF((select ', ' + x.FN from scd x where x.uniqueID = scd.UniqueID for xml path('')), 1,2,'') as FN FROM scd GROUP BY UniqueID )


----------------------------------------https://hrboss.atlassian.net/wiki/spaces/SB/pages/19071426/Requirement+specs+Company+import--------------------------------------------------------------
SELECT 
       --'BB - ' + cast(a.[6 Ref No Numeric] as varchar(20)) as 'company-externalId' , a.UniqueID --8 Reference Numeric
       a.[6 Ref No Numeric] as 'company-externalId' , a.UniqueID --8 Reference Numeric
	, CASE WHEN CompanyName IS NULL THEN 'No Company Name - ' + a.UniqueID
                   WHEN CompanyName IS NOT NULL AND rnk = 1 THEN CompanyName
                   --ELSE CompanyName + ' - Duplicate - ' + a.UniqueID
                   ELSE CompanyName + ' -  '+ a.[6 Ref No Numeric]
                   END as 'company-name'
--       , Stuff( coalesce(' ' + nullif(replace(a.[25 Address Alphanumeric],'~',', '), ''), '') + coalesce(', ' + nullif(b.Description, ''), '') + coalesce(', ' + nullif(a.[33 Postcode Alphanumeric], ''), '') + coalesce(', ' + nullif(tmp_country.COUNTRY, ''), ''), 1, 1, '') as 'company-address'
--       , Stuff( coalesce(' ' + nullif(replace(a.[25 Address Alphanumeric],'~',', '), ''), '') + coalesce(', ' + nullif(b.Description, ''), '') + coalesce(', ' + nullif(a.[33 Postcode Alphanumeric], ''), '') + coalesce(', ' + nullif(tmp_country.COUNTRY, ''), ''), 1, 1, '') as 'company-locationName'
       , ltrim(Stuff( coalesce(' ' + nullif(replace(a.[25 Address Alphanumeric],'~',', ') , ''), '') + coalesce(', ' + nullif(b.Description, ''), '') + coalesce(', ' + nullif(a.[33 Postcode Alphanumeric], ''), '') + coalesce(', ' + nullif(tmp_country.COUNTRY, ''), ''), 1, 1, '') )as 'company-locationaddress'
       , ltrim(Stuff( coalesce(' ' + nullif(replace(a.[25 Address Alphanumeric],'~',', ') , ''), '') + coalesce(', ' + nullif(b.Description, ''), '') + coalesce(', ' + nullif(a.[33 Postcode Alphanumeric], ''), '') + coalesce(', ' + nullif(tmp_country.COUNTRY, ''), ''), 1, 1, '') )as 'company-locationName'
	, coalesce(b.Description,'') 'company-locationState'
	--, coalesce(sb.[1 Suburb Alphanumeric], a.[128 Suburb Xref], '') as 'company-locationDistrict'
	, coalesce(a.[33 Postcode Alphanumeric], '') as 'company-locationZipCode'
	, coalesce(tmp_country.ABBREVIATION, '') as 'company-locationCountry' --[137 Country Codegroup 146]
	--, 'company-nearestTrainStation'
	--, coalesce(hq.[1 Name Alphanumeric], '') as 'company-headQuarter'
	--, coalesce(a.[28 Phone Alphanumeric],'') as 'company-switchBoard'
	, coalesce(a.[28 Phone Alphanumeric], '') as 'company-phone'
	--, a.[29 Fax Alphanumeric] as 'company-fax'
	, coalesce(a.[71 Website Alphanumeric],'') as 'company-website'
	, coalesce(c.Consultant, '') as 'company-owners'
	, coalesce(cd.FN, '') 'company-document'--filename
	, concat('Ref No: ',a.[6 Ref No Numeric]) as 'company-note'
	, ind.Description as 'company-industry', a.[110 Industry Codegroup 127]
	, subind.Description as 'company-subindustry', a.[113 Sub Ind Codegroup 128]
FROM (
       SELECT LTRIM(RTRIM([1 Name Alphanumeric])) as CompanyName
              , ROW_NUMBER() OVER(PARTITION BY LTRIM(RTRIM([1 Name Alphanumeric])) ORDER BY UniqueID) as rnk
              , *
       FROM F02 --WHERE [110 Industry Codegroup 127] = 2 --migrate site only --2 Type Codegroup   2       --where [1 Name Alphanumeric] not like 'xx%'
       ) as a
LEFT JOIN(SELECT * FROM CODES WHERE Codegroup = 26) as b on b.Code = a.[23 Province Codegroup  26]
LEFT JOIN c on c.UniqueID = a.UniqueID --consultant
LEFT JOIN tmp_country on tmp_country.CODE = a.[8 Country Codegroup 133] --countrycode
--LEFT JOIN F39 as sb on sb.UniqueID = a.[128 Suburb Xref]
--LEFT JOIN F02 as hq on hq.UniqueID = a.[23 Client Xref]
--LEFT JOIN F12 as client_office on client_office.UniqueID = a.[11 Created By Xref] --6 Office Xref
LEFT JOIN cd as cd on cd.UniqueID = a.UniqueID
LEFT JOIN (SELECT * FROM CODES WHERE Codegroup = 127) as ind on ind.code = a.[110 Industry Codegroup 127]
LEFT JOIN (SELECT * FROM CODES WHERE Codegroup = 128) as subind on subind.code = replace(a.[113 Sub Ind Codegroup 128],'~','')
--where [CompanyName] like 'Nedbank%'

--LEFT JOIN (SELECT * FROM CODES WHERE Codegroup = 8) as com_source on com_source.Code = a.[13 Source Codegroup   8]
--LEFT JOIN (SELECT [1 Name Alphanumeric], UniqueID FROM F02 WHERE [32 Parent Xref] is NOT NULL) as pc on a.[32 Parent Xref] = pc.UniqueID
--LEFT JOIN (SELECT [1 Name Alphanumeric], UniqueID FROM F02 WHERE [23 Client Xref] is NOT NULL) as cur_client on a.[23 Client Xref] = cur_client.UniqueID
--WHERE 1 = 1 --and a.[6 Ref No Numeric] = 12673
--UNION ALL select 'BB00000','Default Blank Company','','', '','','','','','','','','','','This is Blank Company from Data Import'





-- SUB INDUSTRY
select distinct [113 Sub Ind Codegroup 128]  FROM F02


with
  val1 (ID,val) as (select [6 Ref No Numeric], replace([113 Sub Ind Codegroup 128],char(9),'') as val from F02 where [113 Sub Ind Codegroup 128] <> '' ) --and [4 Ref No Numeric] in (87721, 114602) )
, val2 (ID,val) as (SELECT ID, val.value as val FROM val1 m CROSS APPLY STRING_SPLIT(m.val,'~') AS val)
, val3 (ID,field_value) as (
       select ID,
       case
              when val = '1' then 1
              when val = '41' then 36
              when val = '42' then 38
              when val = '15' then 12
              when val = '43' then 39
              when val = '3' then 2
              when val = '26' then 21
              when val = '31' then 26
              when val = '62' then 47
              when val = '36' then 31
              when val = '47' then 43
              when val = '7' then 4
              when val = '35' then 30
              when val = '37' then 32
              when val = '8' then 5
              when val = '33' then 28
              when val = '64' then 49
              when val = 'FT' then 52
              when val = '9' then 6
              when val = '27' then 22
              when val = '10' then 7
              when val = '39' then 34
              when val = '11' then 8
              when val = '66' then 51
              when val = '12' then 9
              when val = '13' then 10
              when val = '32' then 27
              when val = '61' then 46
              when val = '24' then 19
              when val = '48' then 44
              when val = '49' then 45
              when val = '38' then 33
              when val = '41' then 37
              when val = '40' then 35
              when val = '34' then 29
              when val = '18' then 13
              when val = '19' then 14
              when val = '46' then 42
              when val = '28' then 23
              when val = '14' then 11
              when val = '25' then 20
              when val = '29' then 24
              when val = '20' then 15
              when val = '5' then 3
              when val = '21' then 16
              when val = '44' then 40
              when val = '65' then 50
              when val = '30' then 25
              when val = '22' then 17
              when val = '23' then 18
              when val = '63' then 48
              when val = '45' then 41
              end as val
       from val2              
       )
, val4 (ID,field_value) as (select ID,  STUFF((select ',' + convert(varchar(10),x.field_value) from val3 x where x.ID = val3.ID for xml path('')), 1,1,'') as field_value FROM val3 GROUP BY ID)
--select * from val4


SELECT 
       --'BB - ' + cast(a.[6 Ref No Numeric] as varchar(20)) as 'company-externalId' , a.UniqueID --8 Reference Numeric
       a.[6 Ref No Numeric] as 'company-externalid' , a.UniqueID --8 Reference Numeric
       , 'add_com_info' as additional_type
       , 1006 as form_id
       , 1017 as field_id
	, CASE WHEN CompanyName IS NULL THEN 'No Company Name - ' + a.UniqueID
                   WHEN CompanyName IS NOT NULL AND rnk = 1 THEN CompanyName
                   --ELSE CompanyName + ' - Duplicate - ' + a.UniqueID
                   ELSE CompanyName + ' -  '+ a.[6 Ref No Numeric]
                   END as 'company-name'
	--, ind.Description as 'company-industry', a.[110 Industry Codegroup 127]
	--, subind.Description as 'company-subindustry', a.[113 Sub Ind Codegroup 128]
	, subind.Description as 'company-subindustry'
	, a.[113 Sub Ind Codegroup 128]
/*	, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
	  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
	  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
	  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
	  replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(
	  replace(replace(replace(
	       right(a.[113 Sub Ind Codegroup 128], LEN(a.[113 Sub Ind Codegroup 128]) - 1) ,'~',','),
	       '1','1'),'41','36'),'42','38'),'15','12'),'43','39'),'3','2'),'26','21'),'31','26'),'62','47'),'36','31'),'47','43'),
              '7','4'),'35','30'),'37','32'),'8','5'),'33','28'),'64','49'),'FT','52'),'9','6'),'27','22'),'10','7'),
              '39','34'),'11','8'),'66','51'),'12','9'),'13','10'),'32','27'),'61','46'),'24','19'),'48','44'),'49','45'),
              '38','33'),'41','37'),'40','35'),'34','29'),'18','13'),'19','14'),'46','42'),'28','23'),'14','11'),'25','20'),'29','24'),
              '20','15'),'5','3'),'21','16'),'44','40'),'65','50'),'30','25'),'22','17'),'23','18'),'63','48'),'45','41') as field_value_*/
       , val4.field_value
-- select distinct subind.Description
FROM (
       SELECT LTRIM(RTRIM([1 Name Alphanumeric])) as CompanyName
              , ROW_NUMBER() OVER(PARTITION BY LTRIM(RTRIM([1 Name Alphanumeric])) ORDER BY UniqueID) as rnk
              , *
       FROM F02 --WHERE [110 Industry Codegroup 127] = 2 --migrate site only --2 Type Codegroup   2       --where [1 Name Alphanumeric] not like 'xx%'
       ) as a
--LEFT JOIN (SELECT * FROM CODES WHERE Codegroup = 127) as ind on ind.code = a.[110 Industry Codegroup 127]
LEFT JOIN (SELECT * FROM CODES WHERE Codegroup = 128) as subind on subind.code = replace(a.[113 Sub Ind Codegroup 128],'~','')
left join val4 on val4.ID = a.[6 Ref No Numeric]
where a.[113 Sub Ind Codegroup 128] <> ''

