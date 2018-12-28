

--- FIRST STEP ---
/*
update F01
set [34 Cand Addr Alphanumeric] = 
-- select
              ltrim(case
              when LEFT([34 Cand Addr Alphanumeric], 3) = '~~~' then right([34 Cand Addr Alphanumeric], LEN([34 Cand Addr Alphanumeric]) - 3)
              when LEFT([34 Cand Addr Alphanumeric], 2) = '~~' then right([34 Cand Addr Alphanumeric], LEN([34 Cand Addr Alphanumeric]) - 2)
              when LEFT([34 Cand Addr Alphanumeric], 1) = '~' then right([34 Cand Addr Alphanumeric], LEN([34 Cand Addr Alphanumeric]) - 1)
              else [34 Cand Addr Alphanumeric] end )
--from F01
where [34 Cand Addr Alphanumeric] <> '' 
*/


--------------------03.Candidate---------------108,306---------------------------------------
/*;with y as
(---extract candidate status
	SELECT t2.[4 Candidate Xref],
			c.description
	FROM F13 t2 
	OUTER APPLY 
	(
		SELECT  Item 
		FROM    cgh.dbo.[DelimitedSplitN4K] (t2.[19 LastAction Codegroup 130],'~')
		where t2.[19 LastAction Codegroup 130] is not NULL
	)t
	join (select * from codes where codegroup = 130) as c on c.code = t.Item
	where c.Code in(16,35,36,39,40,41,42,43,44,45,46,47,51,52)
) 
, yy as(
  SELECT [4 Candidate Xref],
        STUFF((select ', ' + x.description from y x where x.[4 Candidate Xref] = y.[4 Candidate Xref] for xml path('')), 1,2,'') as description
  FROM y
  GROUP BY [4 Candidate Xref]
), yyy as(
select [4 Candidate Xref]
 , case when charindex(',', description) <> 0 then SUBSTRING(description, 1, charindex(',', description) - 1) else description end as last_action
 , case when charindex(',', description) <> 0 then substring(description, charindex(',', description) + 1, DATALENGTH(description) - charindex(',', description)) end as penultimate_action
from yy
)
,
scc as
(
	SELECT t2.UniqueID
			, c.VinCountryCode
			, c.description
			, ROW_NUMBER() over(partition by UniqueID order by Vincountrycode) rnk
			
	FROM F01 t2 
	OUTER APPLY 
	(
		SELECT  Item as code
		FROM    cgh.dbo.[DelimitedSplitN4K] (t2.[237 Country Codegroup 146],'~')
		where t2.[237 Country Codegroup 146] is not NULL
	)t
	join cgh.dbo.[CountryCodeMapping]  as c on c.Code = t.code 
) 
, cc as
(
  SELECT VinCountryCode, UniqueID, Description
  FROM scc
  WHERE rnk = 1
), sskill as
(
	SELECT t2.UniqueID,
			c.[1 Attribute Alphanumeric] as Skill
	FROM F01 t2 
	OUTER APPLY 
	(
		SELECT  Item as Skill
		FROM    cgh.dbo.[DelimitedSplitN4K] (t2.[151 Skills Xref],'~')
		where t2.[151 Skills Xref] is not NULL
	)t
	left join F12 as c on c.UniqueID = t.Skill
) 
, skill as(
  SELECT UniqueID,
        STUFF((select ', ' + x.Skill from sskill x where x.uniqueID = sskill.UniqueID for xml path(''),type).value('(./text())[1]','varchar(max)'), 1,2,'') as Skill
  FROM sskill
  GROUP BY UniqueID
), jobhist
as(
select b.[1 Name Alphanumeric] as company
	, a.[4 Start Date Date] as startdate
	, a.[5 End Date Date] as enddate
	, a.[79 Duration Numeric] as duration
	, a.[27 Hist Ref Numeric] as id
	, a.[6 Job Title Alphanumeric] as job
	, a.[1 Candidate Xref] 
	, row_number() over(partition by [1 Candidate Xref] order by a.[4 Start Date Date] desc) as rnk
from f04 as a
JOIN F02 as b on b.UniqueID = a.[2 Site Xref]
where exists(select 1 from F01 as x where x.UniqueID = a.[1 Candidate Xref])
)
,

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
  mail1 (ID,email) as (select [4 Ref No Numeric], replace([33 E-Mail Alphanumeric],char(9),'') as mail from F01 where [101 Candidate Codegroup  23] = 'Y' and [33 E-Mail Alphanumeric] <> '' ) --and [4 Ref No Numeric] in (87721, 114602) )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,'~') AS email)
, mail3 (ID,email,rn) as ( SELECT ID, email = ltrim(email), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail2 )
, e1 (ID,email) as (select ID, email from mail3 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail3 where rn = 2)
, e3 (ID,email) as (select ID, email from mail3 where rn = 3)
--select * from ed where ID in (112523)

-- PHONE
, phone1 (ID,email) as (select [4 Ref No Numeric], LTRIM([38 Ph H W O Alphanumeric]) as phone from F01 where [101 Candidate Codegroup  23] = 'Y' and [38 Ph H W O Alphanumeric] <> '') --and [4 Ref No Numeric] in (87721, 114602) )
, phone2 (ID,email) as (SELECT ID, email.value as email FROM phone1 m CROSS APPLY STRING_SPLIT(m.email,'~') AS email)
, phone3 (ID,email,rn) as ( SELECT ID, email = email, r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM phone2 )
, p1 (ID,phone) as (select ID, email from phone3 where rn = 1)
, p2 (ID,phone) as (select ID, email from phone3 where rn = 2)
, p3 (ID,phone) as (select ID, email from phone3 where rn = 3)
, p4 (ID,phone) as (select ID, email from phone3 where rn = 4)
--select  * from phone3 where ID in (82961)

-- OWNERS
, si as ( SELECT t2.UniqueID, F17.[72 Email Add Alphanumeric] as consultant 
             FROM F01 t2 
	      OUTER APPLY ( SELECT Item as Consultant FROM dbo.[DelimitedSplitN4K] (t2.[105 Perm Cons Xref],'~') where t2.[105 Perm Cons Xref] is not NULL ) t
	      join F17 on F17.UniqueID = t.Consultant
) 
, i as( SELECT UniqueID, STUFF((select ', ' + x.Consultant from si x where x.uniqueID = si.UniqueID for xml path('')), 1,2,'') as Consultant FROM si GROUP BY UniqueID )
-- select distinct consultant from i

/*-- DOCUMENT
, scd as ( select distinct UniqueID, REVERSE(LEFT(REVERSE([Relative Document Path]), CHARINDEX('\', REVERSE([Relative Document Path])) - 1)) as FN from F01Docs1 )
, cd as ( select UniqueID,  STUFF((select ', ' + x.FN from scd x where x.uniqueID = scd.UniqueID for xml path('')), 1,2,'') as FN FROM scd GROUP BY UniqueID) */


-----------------------------------https://hrboss.atlassian.net/wiki/spaces/SB/pages/18284908/Requirement+specs+Candidate+import------------------------------------------------
SELECT --top 200
	 --'BB - ' + cast(a.[4 Ref No Numeric] as varchar(20)) as 'candidate-externalId'
	 a.[4 Ref No Numeric] as 'candidate-externalId'
--       CASE [249 WorkType Codegroup 165] 
--       WHEN NULL THEN 'FULL_TIME'
--	   WHEN 1 THEN 'FULL_TIME'
--	   WHEN 2 THEN 'PART_TIME'
--	   WHEN 3 THEN 'LABOUR_HIRE'
--	   WHEN 4 THEN 'CASUAL'
--	   else 'FULL_TIME'
--      END as 'candidate-employmentType' ----FULL_TIME, PART_TIME, CASUAL --default FULL_TIME, 
       , CASE
             WHEN [27 Title Codegroup  16] = 'MIS' THEN 'MISS'
             WHEN [27 Title Codegroup  16] IS NULL THEN ''
             ELSE  [27 Title Codegroup  16]  END as 'candidate-title'
--     , case when [27 Title Codegroup  16] in('MIS') THEN 'MISS'
--	       when [27 Title Codegroup  16] in('MS') THEN 'MS'
--		   when [27 Title Codegroup  16] in('MR') THEN 'MR'
--		   when [27 Title Codegroup  16] in('MRS') THEN 'MRS'
--		   when [27 Title Codegroup  16] in('DR', 'SIR', 'THE', 'PRO') THEN 'DR'
--		   else ''
--	  end  as 'candidate-title'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName' --, SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position) as 'candidate-lastName'
--	 , [26 Pref Name Alphanumeric] as 'candidate-preferName'
	 , coalesce(i.Consultant, '') as 'candidate-owners'
--	 , coalesce([251 JobTitle Alphanumeric], jh1.job, [96 JobTitle Alphanumeric],'') as 'candidate-jobTitle1'
--	 --, coalesce(CONVERT(date, replace([39 DOB Date], '00/01/1901', NULL), 103), '') as 'candidate-dob'
       , Stuff( coalesce(' ' + NULLIF(replace(replace(a.[34 Cand Addr Alphanumeric],'~',', '),',,',','), ''), '') + coalesce(' ' + NULLIF(province.Description, ''), '') + Coalesce(', ' + NULLIF(tmp_country.COUNTRY, ''), ''), 1, 1, '') as 'candidate-address'
       , Stuff( coalesce(' ' + NULLIF(province.Description, ''), '') + Coalesce(', ' + NULLIF(tmp_country.COUNTRY, ''), ''), 1, 1, '') as 'contact-locationName'
       , coalesce(tmp_country.ABBREVIATION, '') as 'candidate-country'
--	 , coalesce(cc.VinCountryCode, '') as 'candidate-citizenship'
       , case
              when ed.rn > 1 and ed.email <> '' then concat(ed.email,'_',ed.rn)
              when ed.rn > 1 and ed.email = '' then concat(a.[4 Ref No Numeric],'@noemailaddress.co')
              else ed.email end as 'candidate-email'
       , e2.email as 'candidate-PersonalEmail'
--	 , CASE WHEN primary_email IS NULL THEN cast(a.[4 RefNumber Numeric] as varchar(20)) + '@no_email.io'
--	        WHEN primary_email IS NOT NULL AND rnk_email = 1 THEN primary_email
--		    ELSE 'dup_' +  cast(a.[4 RefNumber Numeric] as varchar(20)) + '_' + primary_email
--	   END as 'candidate-email'
	 , coalesce(p1.phone, '') as 'candidate-homePhone'
	 , coalesce(p2.phone, '') as 'candidate-workPhone'
	 , coalesce(p3.phone, '') as 'candidate-mobile'
	 , coalesce(p3.phone, '') as 'candidate-phone'
--	 + case when coalesce(F39.[1 Suburb Alphanumeric], [203 Suburb Xref], '') = '' then '' else coalesce(F39.[1 Suburb Alphanumeric], [203 Suburb Xref], '') + ', ' end
--	 + case when coalesce([204 State Codegroup  51], [35 State Codegroup  51], '') = '' then '' else coalesce([204 State Codegroup  51], [35 State Codegroup  51], '') + ' ' end
--	 + case when coalesce([205 PC Alphanumeric], [23 Postcode Alphanumeric], '') = '' then '' else coalesce([205 PC Alphanumeric], [23 Postcode Alphanumeric], '') + ', ' end
--	 + coalesce(cc.VinCountryCode, '')
--	 as 'candidate-address'--concat state zip code and country??
--	 , coalesce(F39.[1 Suburb Alphanumeric], [203 Suburb Xref], '') as 'candidate-city'--[203 Suburb Xref]
--	 , coalesce([204 State Codegroup  51], [35 State Codegroup  51], '') as 'candidate-State'
--	 , coalesce([205 PC Alphanumeric], [23 Postcode Alphanumeric], '') as 'candidate-zipCode'
--	 --, 'candidate-currency'
--	 , coalesce(skill.Skill,'') as 'candidate-Skill'-- [151 Skills Xref]	 
	 , coalesce([18 Curr Sal Numeric],'') as 'candidate-currentSalary'
	 , s.description as 'candidate-source' --[6 Source Codegroup   8]
--	 , coalesce('Job HistoryId & Duration ' + cast(jh1.id as varchar(30)) + coalesce(' - ' + jh1.duration, '')
--	 + '; ' + cast(jh2.id as varchar(30)) + coalesce(' - ' + jh2.duration, '')
--	 + '; ' + cast(jh3.id as varchar(30)) + coalesce(' - ' + jh3.duration, '')
--	 + coalesce(char(10) + jhs.FN, ''),'') as 'candidate-workHistory'
--	 , coalesce(jh1.company,'') as 'candidate-company1'
--	 , coalesce(convert(date, jh1.startdate, 103), '') as 'candidate-startDate1'
--	 , coalesce(convert(date, jh1.enddate, 103), '') as 'candidate-endDate1'
--	 , coalesce(jh2.company,'') as 'candidate-company2'
--	 , coalesce(convert(date, jh2.startdate, 103), '') as 'candidate-startDate2'
--	 , coalesce(convert(date, jh2.enddate, 103), '') as 'candidate-endDate2'
--	 , coalesce(jh2.job ,'')as 'candidate-jobTitle2'
--	 , coalesce(jh3.company,'') as 'candidate-company3'
--	 , coalesce(convert(date, jh3.startdate, 103),'') as 'candidate-startDate3'
--	 , coalesce(convert(date, jh3.enddate, 103),'') as 'candidate-endDate3'
--	 , coalesce(jh3.job,'') as 'candidate-jobTitle3'
	 , coalesce(cd.FN,'') as 'candidate-resume'
       , Stuff(  
                 Coalesce('Ref No: ' + NULLIF(cast(a.[4 Ref No Numeric] as varchar(max)), '') + char(10), '')
              + Coalesce('Emergency: ' + NULLIF(p4.phone, '') + char(10), '')
              + Coalesce('Alt Email: ' + NULLIF(e3.email, '') + char(10), '')
              , 1, 0, '') as note
       , cat.description as 'category'
       , subind.description as 'subIndustry'
--	 , case when charindex('~', [66 Alert Alphanumeric]) = 1 then '' 
--	   else coalesce('Candidate Alert: ' + case when charindex('~', [66 Alert Alphanumeric]) > 0 then left([66 Alert Alphanumeric], charindex('~', [66 Alert Alphanumeric]) - 1) else [66 Alert Alphanumeric] end + char(10), '')
--	   end as 'candidate-note'
-- select count(*)
FROM  F01 a --where a.[101 Candidate Codegroup  23] = 'Y' --22886
left join (SELECT * FROM CODES WHERE Codegroup = 26) as  province on province.Code = a.[22 Province Codegroup  26]
left join tmp_country on tmp_country.CODE = a.[8 Country Codegroup 133] --countrycode
left join ed on ed.ID = a.[4 Ref No Numeric]
left join e2 on e2.ID = a.[4 Ref No Numeric]
left join e3 on e3.ID = a.[4 Ref No Numeric]
left join p1 on p1.ID = a.[4 Ref No Numeric]
left join p2 on p2.ID = a.[4 Ref No Numeric]
left join p3 on p3.ID = a.[4 Ref No Numeric]
left join p4 on p4.ID = a.[4 Ref No Numeric]
left join (SELECT * FROM CODES WHERE Codegroup = 8) s on s.code = a.[6 Source Codegroup   8]
left join (SELECT * FROM CODES WHERE Codegroup = 24) cat on cat.code = a.[127 Job Cat Codegroup  24]
left join (SELECT * FROM CODES WHERE Codegroup = 128) subind on subind.code = a.[162 Sub Ind Codegroup 128]
left join F01Document cd on cd.UniqueID = a.UniqueID  --left join cd on cd.UniqueID = a.UniqueID
LEFT JOIN i on i.UniqueID = a.UniqueID
where a.[101 Candidate Codegroup  23] = 'Y'
--and a.[4 Ref No Numeric] in (87721, 114602)



/*
(
select 
	  LTRIM(RTRIM([1 Name Alphanumeric])) as ContactName,
	  case when CHARINDEX(' ', LTRIM(RTRIM([1 Name Alphanumeric]))) = 0 
	  then CHARINDEX(' ', LTRIM(RTRIM([1 Name Alphanumeric]))) 
	  else CHARINDEX(' ', LTRIM(RTRIM([1 Name Alphanumeric]))) 
	  end as space_position
	  , ROW_NUMBER() OVER(PARTITION BY LTRIM(RTRIM(primary_email)) ORDER BY UniqueID) as rnk_email
	  , *      
from
--(
--	select *
--	 ,replace(case when charindex('~', [33 E-Mail Alphanumeric]) >= 10 then substring([33 E-Mail Alphanumeric], 1, charindex('~', [33 E-Mail Alphanumeric])) else [33 E-Mail Alphanumeric] end, '~', '') as primary_email
--     ,replace(case when charindex('~', [33 E-Mail Alphanumeric]) >= 10 then substring([33 E-Mail Alphanumeric], charindex('~', [33 E-Mail Alphanumeric]), DATALENGTH([33 E-Mail Alphanumeric]) - charindex('~', [33 E-Mail Alphanumeric])) else '' end, '~', '') as work_email
--	from f01
--	where 1=1
--	and [16 Site Xref] is null 
--	and [1 Name Alphanumeric] not like 'xx%'
--	union all
--	select *
--	 ,replace(case when charindex('~', [33 E-Mail Alphanumeric]) >= 10 then substring([33 E-Mail Alphanumeric], 1, charindex('~', [33 E-Mail Alphanumeric])) else [33 E-Mail Alphanumeric] end, '~', '') as primary_email
--     ,replace(case when charindex('~', [33 E-Mail Alphanumeric]) >= 10 then substring([33 E-Mail Alphanumeric], charindex('~', [33 E-Mail Alphanumeric]), DATALENGTH([33 E-Mail Alphanumeric]) - charindex('~', [33 E-Mail Alphanumeric])) else '' end, '~', '') as work_email
--	from f01 a
--	where 1=1
--	and [16 Site Xref] is NOT null 
--	and exists(select 1 from f03 as x where x.[20 Contact Xref] = a.UniqueID)
--	--and exists (select 1 from [All_Applications_180831] as x where x.[Cand id] = F01.[4 RefNumber Numeric])
--	and [1 Name Alphanumeric] not like 'xx%'
--) as a
left join yyy on yyy.[4 Candidate Xref] = a.UniqueID
left join tmp_country on tmp_country.CODE = a.[8 Country Codegroup 133]
--where [28 Created Date] <>'00/01/1901'
--and 
--(
--  convert(date, [28 Created Date], 103) >= '2013-01-01'
--  or
--   (
--		convert(date, [28 Created Date], 103) < '2013-01-01'
--		and (
--		     penultimate_action in
--				(
--				'Web Arrange Interview',
--				'Cons Interview Booked',
--				'Cons Interview Complete',
--				'Client 1st I/V Booked',
--				'Client 1st I/V Complete',
--				'Client 2nd I/V Booked',
--				'Client 2nd I/V Complete',
--				'Client 3rd I/V Booked',
--				'Client 3rd I/V Complete',
--				'Client 4th I/V Booked',
--				'Client 4th I/V Complete',
--				'Verification'
--				)
--			or [41 Last Conta Date] >= '2013-01-01' 
--			or last_action is not null
--			) 
--   )
)
	

) as a
LEFT JOIN(SELECT * FROM CODES WHERE Codegroup = 4) as c on c.Code = a.[3 Status Codegroup   4]
LEFT JOIN cc on cc.UniqueID = a.UniqueID
LEFT JOIN F39 on F39.UniqueID = a.[203 Suburb Xref]
LEFT JOIN skill on skill.UniqueID = a.UniqueID
LEFT JOIN (SELECT * FROM jobhist WHERE rnk = 1) as jh1 on jh1.[1 Candidate Xref] = a.UniqueID
LEFT JOIN (SELECT * FROM jobhist WHERE rnk = 2) as jh2 on jh2.[1 Candidate Xref] = a.UniqueID
LEFT JOIN (SELECT * FROM jobhist WHERE rnk = 3) as jh3 on jh3.[1 Candidate Xref] = a.UniqueID
OUTER APPLY
(
   select [1 Candidate Xref]
      ,  STUFF((select char(10) + cast(x.id as varchar(30)) + ' - ' + x.company + ' - ' + x.job + ' - ' + convert(varchar(20), x.startdate, 103) + ' - ' 
	  + convert(varchar(20), x.enddate,103)
	from jobhist as x where x.[1 Candidate Xref] = jobhist.[1 Candidate Xref] and x.rnk > 3 for xml path('')), 1,2,'') as FN
	FROM jobhist 
	WHERE jobhist.[1 Candidate Xref] = a.UniqueID
	GROUP BY [1 Candidate Xref]
) as jhs
LEFT JOIN cd on cd.UniqueID = a.UniqueID
left join(select * from px where px.ItemNumber = 3) as px3 on px3.uniqueid = a.uniqueid
left join(select * from px where px.ItemNumber = 1) as px1 on px1.uniqueid = a.uniqueid
left join(select * from px where px.ItemNumber = 2) as px2 on px2.uniqueid = a.uniqueid
left join(select * from px where px.ItemNumber = 4) as px4 on px4.uniqueid = a.uniqueid

*/


-- CUSTOM
SELECT top 200
	 a.[4 Ref No Numeric] as 'candidate-externalId'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName'
       , s.description as 'candidate-source'
       , cat.description as 'category'
       , ind.description as 'industry'
       , subind.description as 'subIndustry'       	 
-- select top 10 *
FROM  F01 a --where a.[101 Candidate Codegroup  23] = 'Y' --22886
left join (SELECT * FROM CODES WHERE Codegroup = 8) s on s.code = a.[6 Source Codegroup   8]
left join (SELECT * FROM CODES WHERE Codegroup = 24) cat on cat.code = a.[127 Job Cat Codegroup  24]
left join (SELECT * FROM CODES WHERE Codegroup = 132) ind on ind.code = a.[221 Area Codegroup 132]
left join (SELECT * FROM CODES WHERE Codegroup = 128) subind on subind.code = a.[162 Sub Ind Codegroup 128]
where a.[101 Candidate Codegroup  23] = 'Y'	and a.[221 Area Codegroup 132] <> ''
and  a.[4 Ref No Numeric] in (144925,156146, 161476)
and a.[186 Forenames Alphanumeric] like '%Vuyiswa%'       



-- SOURCE
SELECT --top 200
	 a.[4 Ref No Numeric] as 'candidate-externalId'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName'
       , ltrim(rtrim(s.description)) as 'candidate-source' --[6 Source Codegroup   8] 	 
-- select top 10 *
FROM  F01 a --where a.[101 Candidate Codegroup  23] = 'Y' --22886
left join (SELECT * FROM CODES WHERE Codegroup = 8) s on s.code = a.[6 Source Codegroup   8]
where a.[101 Candidate Codegroup  23] = 'Y'	AND a.[6 Source Codegroup   8] <> '' and s.description is not null



--INDUSTRY
SELECT --top 200
	 a.[4 Ref No Numeric] as 'candidate-externalId'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName'
       , ind.industry as 'industry', [221 Area Codegroup 132]
-- select distinct ind.industry -- select count(*)
FROM  F01 a 
left join (
       SELECT CODE,
              case
                     when description = 'Compliance' then 'Legal & Compliance'
                     when description = 'Creative' then 'Marketing'
                     when description = 'Engineering' then 'Engineering & Mining'
                     when description = 'Finance' then 'Accounting & Finance'
                     when description = 'Human Resources ' then 'Human Resources'
                     when description = 'Insurance' then null
                     when description = 'Information Technology' then 'Information Technology'
                     when description = 'Medical' then null
                     when description = 'Operations Fund Management ' then null
                     when description = 'Operations Investment Banking ' then null
                     when description = 'Operations' then null
                     when description = 'Risk' then 'Quants & Risk'
                     when description = 'Sales & Trading' then 'Sales'
                     when description = 'Supply Chain/Logistics/Freight' then 'Procurement & Supply Chain'
                     else null end as industry
        FROM CODES WHERE Codegroup = 132
        ) ind on ind.code = a.[221 Area Codegroup 132]
where a.[101 Candidate Codegroup  23] = 'Y'	and a.[221 Area Codegroup 132] <> ''
and ind.industry is not null


-- SUB INDUSTRY
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

SELECT top 200
	 a.[4 Ref No Numeric] as 'candidate-externalId'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName'
       , 'add_can_info' as additional_type
       , 1005 as form_id
       , 1024 as field_id  
       , subind.description as 'subIndustry', a.[162 Sub Ind Codegroup 128]
       , val4.field_value
-- select top 10 *
FROM  F01 a
left join (SELECT * FROM CODES WHERE Codegroup = 128) subind on subind.code = a.[162 Sub Ind Codegroup 128]
left join val4 on val4.ID = a.[4 Ref No Numeric]
where a.[101 Candidate Codegroup  23] = 'Y'	and a.[162 Sub Ind Codegroup 128] <> ''








-- CATEGORY
with
  val1 (ID,val) as (select [4 Ref No Numeric], replace([127 Job Cat Codegroup  24],char(9),'') as val from F01 where [127 Job Cat Codegroup  24] <> '' ) --and [4 Ref No Numeric] in (87721, 114602) )
, val2 (ID,val) as (SELECT ID, val.value as val FROM val1 m CROSS APPLY STRING_SPLIT(m.val,'~') AS val)
, val3 (ID,field_value) as (
       select ID,
       case
when val = '1' then 1
when val = '2' then 54
when val = '3' then 86
when val = '176' then 38
when val = '209' then 64
when val = '1021' then 5
when val = '5' then 113
when val = '227' then 79
when val = '305' then 90
when val = '177' then 39
when val = '7' then 123
when val = '10' then 2
when val = '12' then 16
when val = '15' then 26
when val = '146' then 25
when val = '18' then 42
when val = '192' then 53
when val = '19' then 50
when val = '208' then 63
when val = '306' then 91
when val = '225' then 77
when val = '304' then 89
when val = '203' then 58
when val = '24' then 83
when val = '25' then 84
when val = '28' then 85
when val = '31' then 95
when val = '32' then 101
when val = '37' then 102
when val = '202' then 57
when val = '210' then 65
when val = '39' then 103
when val = '180' then 43
when val = '42' then 111
when val = '174' then 36
when val = '44' then 112
when val = '228' then 80
when val = '50' then 114
when val = '53' then 115
when val = '1023' then 7
when val = '1024' then 8
when val = '54' then 116
when val = '1025' then 9
when val = '56' then 117
when val = '221' then 73
when val = '57' then 118
when val = '302' then 88
when val = '314' then 97
when val = '58' then 119
when val = '59' then 120
when val = '103' then 10
when val = '186' then 47
when val = '191' then 52
when val = '179' then 41
when val = '188' then 49
when val = '184' then 46
when val = '213' then 68
when val = '67' then 121
when val = '211' then 66
when val = '69' then 122
when val = '70' then 124
when val = '72' then 125
when val = '76' then 126
when val = '77' then 127
when val = '315' then 98
when val = '308' then 93
when val = '79' then 128
when val = '181' then 44
when val = '312' then 96
when val = '88' then 130
when val = '404' then 107
when val = '212' then 67
when val = '173' then 35
when val = '224' then 76
when val = '96' then 131
when val = '98' then 132
when val = '401' then 104
when val = '178' then 40
when val = '101' then 3
when val = '102' then 4
when val = '301' then 87
when val = '108' then 11
when val = '81' then 129
when val = '307' then 92
when val = '110' then 12
when val = '316' then 99
when val = '309' then 94
when val = '226' then 78
when val = '215' then 70
when val = '114' then 13
when val = '115' then 14
when val = '406' then 109
when val = '119' then 15
when val = '214' then 69
when val = '190' then 51
when val = '121' then 17
when val = '217' then 71
when val = '130' then 18
when val = '207' then 62
when val = '205' then 60
when val = '222' then 74
when val = '405' then 108
when val = '131' then 19
when val = '317' then 100
when val = '187' then 48
when val = '133' then 20
when val = '1022' then 6
when val = '175' then 37
when val = '136' then 21
when val = '204' then 59
when val = '137' then 22
when val = '138' then 23
when val = '206' then 61
when val = '141' then 24
when val = '218' then 72
when val = '200' then 55
when val = '201' then 56
when val = '153' then 27
when val = '230' then 82
when val = '223' then 75
when val = '157' then 28
when val = '158' then 29
when val = '407' then 110
when val = '182' then 45
when val = '229' then 81
when val = '165' then 30
when val = '167' then 31
when val = '170' then 32
when val = '402' then 105
when val = '171' then 33
when val = '172' then 34
when val = '403' then 106      
              end as val
       from val2              
       )
, val4 (ID,field_value) as (select ID,  STUFF((select ',' + convert(varchar(10),x.field_value) from val3 x where x.ID = val3.ID for xml path('')), 1,1,'') as field_value FROM val3 GROUP BY ID)
--select * from val2 where ID  = 114532


SELECT top 200
	 a.[4 Ref No Numeric] as 'candidate-externalId'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName'
       , 'add_can_info' as additional_type
       , 1005 as form_id
       , 1023 as field_id         
       , a.[127 Job Cat Codegroup  24], cat.description as 'category'
       , val4.field_value 	 
-- select top 10 *
FROM  F01 a
left join (SELECT * FROM CODES WHERE Codegroup = 24) cat on cat.code = a.[127 Job Cat Codegroup  24]
left join val4 on val4.ID = a.[4 Ref No Numeric]
where a.[101 Candidate Codegroup  23] = 'Y'	and a.[127 Job Cat Codegroup  24] <> ''
and  a.[4 Ref No Numeric] = 114532


 -- Employer + Position
SELECT --top 200
	 a.[4 Ref No Numeric] as 'candidate-externalId'
       , coalesce(nullif(replace(a.[186 Forenames Alphanumeric],'?',''), ''), 'No Firstname') as 'contact-firstName' --, case when  LEFT(ContactName, space_position) = '' then  SUBSTRING(ContactName, space_position, datalength(ContactName) - space_position)  else LEFT(ContactName, space_position) end as 'candidate-firstName'
       , coalesce(nullif(replace(a.[185 Surname Alphanumeric],'?',''), ''), concat('Lastname-',a.[4 Ref No Numeric])) as 'contact-lastName' 
       , a.[96 Cont Posn Alphanumeric] as 'candidate-jobTitle1'
       , F02.[1 Name Alphanumeric] as 'candidate-company1'
       , F02.[1 Name Alphanumeric] as 'candidate-employer1'
       , *
FROM  F01 a --where a.[101 Candidate Codegroup  23] = 'Y' --22886
left join F02 on F02.[UniqueID] = a.[16 Employer Xref]
where a.[101 Candidate Codegroup  23] = 'Y'
and a.[4 Ref No Numeric] in (156146) --, 114602)
and a.[186 Forenames Alphanumeric] like '%Jaqueline%'       