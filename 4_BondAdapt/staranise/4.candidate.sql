
with
-- tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, STUFF((SELECT DISTINCT ', ' + TEL_NUMBER from  PROP_TELEPHONE WHERE REFERENCE = a.REFERENCE FOR XML PATH ('')), 1, 2, '')  AS URLList FROM PROP_TELEPHONE as a GROUP BY a.REFERENCE)
  mail1 (ID,email) as (select REFERENCE, ltrim(rtrim(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_LINK,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),'•',''),CHAR(9),' ') )) as email from PROP_EMAIL where EMAIL_LINK like '%@%' and EMAIL_LINK <> '' and EMAIL_LINK IS NOT NULL )
, mail2 (ID,email) as (SELECT ID, Split.a.value('.', 'VARCHAR(100)') AS String FROM (SELECT ID, CAST ('<M>' + REPLACE(REPLACE(email,' ','</M><M>'),',','</M><M>') + '</M>' AS XML) AS Data FROM mail1) AS A CROSS APPLY Data.nodes ('/M') AS Split(a))
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
--, mailpe as (select ID,email as email1 from mail4 where rn = 1)
--, mailwe as (select ID,email as email1 from mail4 where rn = 2)
--, mailoe as (SELECT ID,email = STUFF((SELECT DISTINCT ',' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH ('')), 1, 1, '') FROM mail4 AS a where rn >2 GROUP BY a.ID)
, mail5 (ID, email1, email2, email3) as (
		select pe.ID, email as email1, we.email2, oe.email3 from mail4 pe
		left join (select ID, email as email2 from mail4 where rn = 2) we on we.ID = pe.ID
		left join (SELECT ID, STUFF((SELECT DISTINCT ', ' + email from mail4 WHERE rn > 2 and ID = a.ID FOR XML PATH ('')), 1, 1, '')  AS email3 FROM mail4 AS a where rn > 2 GROUP BY a.ID ) oe on oe.ID = pe.ID
		where pe.rn = 1 )
--select * from mail4 --where email1 = 'phoughton@man.co.uk'

--, dob (BISUNIQUEID,REFERENCE,DT_OF_BIRTH,rn) as (SELECT BISUNIQUEID,REFERENCE,DT_OF_BIRTH,ROW_NUMBER() OVER(PARTITION BY cg.REFERENCE ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_CAND_GEN cg WHERE cg.DT_OF_BIRTH IS NOT NULL)
, dob (BISUNIQUEID,REFERENCE,DT_OF_BIRTH) as (SELECT BISUNIQUEID,REFERENCE,DT_OF_BIRTH FROM PROP_CAND_GEN cg WHERE cg.DT_OF_BIRTH IS NOT NULL)
, photo  (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY = 6532918 AND FILE_EXTENSION in ('gif','jpeg','jpg','png') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
--, resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE DOC_CATEGORY = 31159 AND FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip') and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
, resume (OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STUFF((SELECT DISTINCT ', ' + cast(DOC_ID as varchar(max)) + '.' + FILE_EXTENSION from DOCUMENTS WHERE OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 2, '')  AS doc FROM DOCUMENTS as a GROUP BY a.OWNER_ID)
, skill    as (select REFERENCE,DESCRIPTION from PROP_SKILLS SKILL INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SKILL.SKILL)
, location as (select REFERENCE,DESCRIPTION from PROP_LOCATIONS LOCATION INNER JOIN MD_MULTI_NAMES MN ON MN.ID = LOCATION.LOCATION)

--, industry    as (select REFERENCE,DESCRIPTION from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY)
, JobCategory as (select REFERENCE,DESCRIPTION from PROP_JOB_CAT JOB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = JOB_CAT.JOB_CATEGORY)
, SubCategory as (select REFERENCE,DESCRIPTION from PROP_SUB_CAT SUB_CAT INNER JOIN MD_MULTI_NAMES MN ON MN.ID = SUB_CAT.SUB_CAT)

, owner as ( select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE,EMPLOYEE.NAME,mail.EMAIL_LINK,EMPLOYEE.USER_REF from PROP_PERSON_GEN PERSON_GEN 
	INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%_@_%.__%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	where CONFIG_NAME = 'Permanent'
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)
--select * from owner where EMAIL_LINK like '%david.fleming@venetian.com.mo%'


--select top 20
select
	 pg.REFERENCE as 'candidate-externalId'
	, Coalesce(NULLIF(replace(pg.FIRST_NAME,'?',''), ''), 'No Firstname') as 'contact-firstName'
	, Coalesce(NULLIF(replace(pg.LAST_NAME,'?',''), ''), 'No Lastname') as 'contact-lastName'
	, pg.MIDDLE_NAME as 'candidate-middleName' 
	, CONVERT(VARCHAR(10),dob.DT_OF_BIRTH,110) as 'candidate-dob'

	, title.TITLE as 'candidate-title'
	--, pg.JOB_TITLE as '(candidate-title)'
	--, pg.TITLE as 'candidate-title'
	, case title.TITLE 
		when 'Mr' then 'MALE' 
		when 'Ms' then 'FEMALE'
		when 'Mrs' then 'FEMALE'
		when 'Miss' then 'FEMALE'
		when 'Doctor' then ''
		when 'Not Active' then ''
	end as 'candidate-gender'

	, concat(address.STREET1,iif(street2.STREET2 = '' OR street2.STREET2 is NULL,'',concat(', ',street2.STREET2))) as '(company-address)'
	, Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(street2.STREET2, ''), ''), 1, 1, '') as 'company-address'
	, address.TOWN as 'candidate-city'
	, address.state as 'candiadte-state'
	, address.POST_CODE as 'candidate-zipCode'
	, Nationality.Nationality as 'candidate-citizenship'
	
	,case
		when cnt.DESCRIPTION like '%AFGHANISTAN%' then 'AF'
		when cnt.DESCRIPTION like '%�LAND%' then ''
		when cnt.DESCRIPTION like '%AMERICAN%' then ''
		when cnt.DESCRIPTION like '%ANGOLA%' then 'AO'
		when cnt.DESCRIPTION like '%ANTARCTICA%' then ''
		when cnt.DESCRIPTION like '%AUSTRALIA%' then 'AU'
		when cnt.DESCRIPTION like '%AUSTRIA%' then 'AT'
		when cnt.DESCRIPTION like '%BAHRAIN%' then 'BH'
		when cnt.DESCRIPTION like '%BANGLADESH%' then 'BD'
		when cnt.DESCRIPTION like '%BELGIUM%' then 'BE'
		when cnt.DESCRIPTION like '%CAMBODIA%' then 'KH'
		when cnt.DESCRIPTION like '%CANADA%' then 'CA'
		when cnt.DESCRIPTION like '%CHAD%' then 'TD'
		when cnt.DESCRIPTION like '%CHINA%' then 'CN'
		when cnt.DESCRIPTION like '%COSTA%' then 'CR'
		when cnt.DESCRIPTION like '%DENMARK%' then 'DK'
		when cnt.DESCRIPTION like '%FINLAND%' then 'FI'
		when cnt.DESCRIPTION like '%FRANCE%' then 'FR'
		when cnt.DESCRIPTION like '%GERMANY%' then 'DE'
		when cnt.DESCRIPTION like '%GHANA%' then 'GH'
		when cnt.DESCRIPTION like '%GREECE%' then 'GR'
		when cnt.DESCRIPTION like '%HONG%' then 'CN'
		when cnt.DESCRIPTION like '%HUNGARY%' then 'HU'
		when cnt.DESCRIPTION like '%ICELAND%' then 'IS'
		when cnt.DESCRIPTION like '%INDIA%' then 'IN'
		when cnt.DESCRIPTION like '%INDONESIA%' then 'ID'
		when cnt.DESCRIPTION like '%IRELAND%' then 'IE'
		when cnt.DESCRIPTION like '%ITALY%' then 'IT'
		when cnt.DESCRIPTION like '%JAPAN%' then 'JP'
		when cnt.DESCRIPTION like '%KAZAKHSTAN%' then 'KZ'
		when cnt.DESCRIPTION like '%KENYA%' then 'KE'
		when cnt.DESCRIPTION like '%KOREA%North%' then 'KP'
		when cnt.DESCRIPTION like '%KOREA%South%' then 'KR'
		when cnt.DESCRIPTION like '%LUXEMBOURG%' then 'LU'
		when cnt.DESCRIPTION like '%MACAU%' then 'MO'
		when cnt.DESCRIPTION like '%MALAYSIA%' then 'MY'
		when cnt.DESCRIPTION like '%MALDIVES%' then ''
		when cnt.DESCRIPTION like '%MOLDOVA%' then ''
		when cnt.DESCRIPTION like '%NETHERLANDS%' then 'NL'
		when cnt.DESCRIPTION like '%NEW%' then 'NZ'
		when cnt.DESCRIPTION like '%NULL%' then ''
		when cnt.DESCRIPTION like '%OMAN%' then 'OM'
		when cnt.DESCRIPTION like '%PHILIPPINES%' then 'PH'
		when cnt.DESCRIPTION like '%QATAR%' then 'QA'
		when cnt.DESCRIPTION like '%ROMANIA%' then 'RO'
		when cnt.DESCRIPTION like '%RUSSIA%' then 'RU'
		when cnt.DESCRIPTION like '%SINGAPORE%' then 'SG'
		when cnt.DESCRIPTION like '%SLOVAKIA%' then 'SK'
		when cnt.DESCRIPTION like '%SOUTH%' then 'ZA'
		when cnt.DESCRIPTION like '%SRI%' then 'LK'
		when cnt.DESCRIPTION like '%SWAZILAND%' then 'SZ'
		when cnt.DESCRIPTION like '%SWEDEN%' then 'SE'
		when cnt.DESCRIPTION like '%SWITZERLAND%' then 'CH'
		when cnt.DESCRIPTION like '%TAIWAN%' then 'TW'
		when cnt.DESCRIPTION like '%THAILAND%' then 'TH'
		when cnt.DESCRIPTION like '%TURKEY%' then 'TR'
		when cnt.DESCRIPTION like '%UKRAINE%' then 'UA'
		when cnt.DESCRIPTION like '%UNITED ARAB EMIRATES%' then 'AE'
		when cnt.DESCRIPTION like '%UNITED KINGDOM%' then 'GB'
		when cnt.DESCRIPTION like '%UNITED STATES%' then 'US'
		when cnt.DESCRIPTION like '%VIET%' then 'VN'
		end as 'candidate-country'
	
	
	, mail5.email1 as 'candidate-email'
	, mail5.email2 as 'candidate-workEmail'
	
	, case 
                when (cast(telmobile.TEL_NUMBER  as varchar(max)) != '' and telmobile.TEL_NUMBER is not null) then telmobile.TEL_NUMBER
                when (cast(telhome.TEL_NUMBER as varchar(max)) != '' and telhome.TEL_NUMBER is not null) then telhome.TEL_NUMBER
                else telwork.TEL_NUMBER
        end as 'candidate-phone' --primary phone
	
	, telhome.TEL_NUMBER as 'candidate-homePhone'
	, telwork.TEL_NUMBER as 'candidate-mobile'	 --candidate-phone
	, telwork.TEL_NUMBER as 'candidate-workPhone'
	
	, case
		when cur.DESCRIPTION like '%Australia%' then 'AUD'
		when cur.DESCRIPTION like '%Chinese%' then 'CNY'
		when cur.DESCRIPTION like '%Euro%' then 'EUR'
		when cur.DESCRIPTION like '%Hong%' then 'HKD'
		when cur.DESCRIPTION like '%Japan%' then 'JPY'
		when cur.DESCRIPTION like '%Kuwait%' then 'KWD'
		when cur.DESCRIPTION like '%New%' then 'NZD'
		when cur.DESCRIPTION like '%Romania%' then 'RON'
		when cur.DESCRIPTION like '%Singapore%' then 'SGD'
		when cur.DESCRIPTION like '%United Kingdom%' then 'GBP'
		when cur.DESCRIPTION like '%United States%' then 'USD'
		end as 'candidate-currency'

	--, 'PERMANENT' as 'candidate-jobTypes'
	, cast(cp.SALARY_CURR as int) as 'candidate-currentSalary'
	, cast(cp.SALARY_DES as int) as 'candidate-desiredSalary'
	
	, ee2.ESTAB as 'candidate-schoolName' --'candidate-education'
	, ee3.QUAL as 'candidate-degreeName'
	--, ee.TO_DATE as 'candidate-graduationDate'
        --, ee3.DEGREE as 'candidate-degreeName'
	--, ee.gpa 'candidate-gpa'
	, pg.LINKED_IN as 'contact-linkedin'
	, skills.DESCRIPTION 'candidate-skills'

	, pg.EMPLOYER as 'candidate-employer1' 
        , pg.JOB_TITLE as 'candidate-jobtitle1'
        , pg.EMPLOYER as 'candidate-company1'
	--, left(st1.StartDate,10) as 'candidate-startdate1'
	--, left(et01.EndDate,10) as 'candidate-enddate1'

	, owner.EMAIL_LINK as 'candidate-owners'
	--, t4.finame as 'Candidate File'
	
	, replace(resume.DOC_ID,'.txt','.rtf') as 'candidate-resume'
	, photo.DOC_ID as 'candidate-photo'
	, replace(concat(
		 iif(pg.Salutation = '' OR pg.Salutation is NULL,'',concat ('Salutation: ',pg.Salutation,char(10)))
		, iif(pg.CHINESENAME = '' OR pg.CHINESENAME is NULL,'',concat ('Chinese Name: ',pg.CHINESENAME,char(10)))
		, iif(pg.ChiFullName = '' OR pg.ChiFullName is NULL,'',concat ('Full Name: ',pg.ChiFullName,char(10)))
		, iif(emailh2.EMAIL_LINK = '' OR emailh2.EMAIL_LINK is NULL,'',concat ('Home Email: ',emailh2.EMAIL_LINK,char(10)))
		, iif(mail5.email3 = '' or mail5.email3 is null,'',concat('Other Email: ',mail5.email3,char(10)))
		, case when (pl.DESCRIPTION = '' or pl.DESCRIPTION is null) then '' else concat('Location: ',pl.DESCRIPTION,char(10)) end
		, iif(status.Status = '' OR status.Status is NULL,'',concat ('Status: ',status.Status,char(10)))
		, iif(rating.Ranking = '' OR rating.Ranking is NULL,'',concat ('Ranking: ',rating.Ranking,char(10)))
		, iif(cur.DESCRIPTION = '' or cur.DESCRIPTION is null,'',concat('Currency: ',case
			when cur.DESCRIPTION like '%Australia%' then 'AUD'
			when cur.DESCRIPTION like '%Chinese%' then 'CNY'
			when cur.DESCRIPTION like '%Euro%' then 'EUR'
			when cur.DESCRIPTION like '%Hong%' then 'HKD'
			when cur.DESCRIPTION like '%Japan%' then 'JPY'
			when cur.DESCRIPTION like '%Kuwait%' then 'KWD'
			when cur.DESCRIPTION like '%New%' then 'NZD'
			when cur.DESCRIPTION like '%Romania%' then 'RON'
			when cur.DESCRIPTION like '%Singapore%' then 'SGD'
			when cur.DESCRIPTION like '%United Kingdom%' then 'GBP'
			when cur.DESCRIPTION like '%United States%' then 'USD'
			end,char(10)))
		, iif(/*sal.SALARY_DES = '' OR*/ sal.SALARY_DES is NULL,'',concat ('Salary Desired: ',sal.SALARY_DES,char(10)))
		, iif(sal.CurrentSalaryMonth = '' OR sal.CurrentSalaryMonth is NULL,'',concat ('Current Salary Month: ',sal.CurrentSalaryMonth,char(10)))
		, iif(sal.ExpectedSalaryMonth = '' OR sal.ExpectedSalaryMonth is NULL,'',concat ('Expected Salary Month: ',sal.ExpectedSalaryMonth,char(10)))	
		, case when (cast(pg.PERSON_ID as varchar(max)) = '' or pg.PERSON_ID is null) then '' else concat('Personal ID: ',pg.PERSON_ID,char(10)) end
		, iif(pg.ID_Passport = '' OR pg.ID_Passport is NULL,'',concat ('ID_Passport: ',pg.ID_Passport,char(10)))
		, iif(visa.VISA = '' OR visa.VISA is NULL,'',concat ('VISA: ',visa.VISA,char(10)))
		, iif(websiteprofile.PORTFOLIO = '' OR websiteprofile.PORTFOLIO is NULL,'',concat ('Website Profile: ',websiteprofile.PORTFOLIO,char(10)))
		, iif(otheronlineprofile.ONLINE_PRO = '' OR otheronlineprofile.ONLINE_PRO is NULL,'',concat ('Other Online Profile: ',otheronlineprofile.ONLINE_PRO,char(10)))
		, iif(consname.ConsName = '' OR consname.ConsName is NULL,'',concat ('Consultant: ',consname.ConsName,char(10)))
		, case when (owner.NAME = '' or owner.NAME is null) then '' else concat('Candidate Owner: ',owner.NAME,char(10)) end
		, iif(source.Source = '' OR source.Source is NULL,'',concat ('Source: ',source.Source,char(10)))
		, iif(referral.Referral = '' OR referral.Referral is NULL,'',concat ('Referral: ',referral.Referral,char(10)))
		--, case when (industry.DESCRIPTION = '' or industry.DESCRIPTION is null) then '' else concat('Industry: ',industry.DESCRIPTION,char(10)) end
		, case when (JOBCATEGORY.DESCRIPTION = '' or JOBCATEGORY.DESCRIPTION is null) then '' else concat('Industry: ',JOBCATEGORY.DESCRIPTION,char(10)) end
		, case when (JOBCATEGORY.DESCRIPTION = '' or JOBCATEGORY.DESCRIPTION is null) then '' else concat('Job Category: ',JOBCATEGORY.DESCRIPTION,char(10)) end
		, case when (SubCategory.DESCRIPTION = '' or SubCategory.DESCRIPTION is null) then '' else concat('Sub Category: ',SubCategory.DESCRIPTION,char(10)) end
		--, note.NOTE,char(10)
		),'&amp;','') as 'candidate-note'
	--, comment.NOTE as 'candidate-comments'
-- select count(*)
-- select top 1000 RATING,PQE,PQE_YEAR,PQE_YEAR2
from PROP_PERSON_GEN pg --21158 rows
left join (SELECT REFERENCE, MN.DESCRIPTION as TITLE FROM PROP_PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_PERSON_GEN.TITLE) title on pg.REFERENCE = title.REFERENCE
--left join (SELECT * from dob where rn = 1) dob on pg.REFERENCE = dob.REFERENCE
left join dob on pg.REFERENCE = dob.REFERENCE

left join mail5 ON pg.REFERENCE = mail5.ID -- candidate-email
left join (SELECT REFERENCE, EMAIL_LINK from PROP_EMAIL where OCC_ID = 100000217) emailh2 on pg.REFERENCE = emailh2.REFERENCE

left join PROP_CAND_PREF cp on pg.REFERENCE = cp.REFERENCE
left join owner ON pg.REFERENCE = owner.PROP_PERSON_GEN_REFERENCE
--left join comment on pg.REFERENCE = comment.OWNER_ID
--left join note on pg.REFERENCE = note.OWNER_ID
left join photo on pg.REFERENCE = photo.OWNER_ID
left join resume on pg.REFERENCE = resume.OWNER_ID

left join (select REFERENCE, CONFIG_NAME,STREET1,TOWN,STATE,COUNTRY,POST_CODE from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') address ON pg.REFERENCE = address.REFERENCE
left join (select REFERENCE, DESCRIPTION from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID INNER JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY where CONFIG_NAME = 'Primary') cnt ON pg.REFERENCE = cnt.REFERENCE
left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on pg.REFERENCE = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on pg.REFERENCE = telmobile.REFERENCE --candidate-mobile
left join (select REFERENCE, TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work') telwork on pg.REFERENCE = telwork.REFERENCE --candidate-phone & candidate-workPhone
left join (SELECT REFERENCE, DESCRIPTION FROM PROP_PERSON_GEN PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PERSON_GEN.CURRENCY) cur on pg.REFERENCE = cur.REFERENCE

left join (SELECT REFERENCE, PORTFOLIO FROM PROP_PORTFOLIO) websiteprofile on pg.REFERENCE = websiteprofile.REFERENCE
left join (SELECT REFERENCE, ONLINE_PRO from PROP_ONLINE) otheronlineprofile on pg.REFERENCE = otheronlineprofile.REFERENCE
left join (select PERSON_GEN.REFERENCE, EMPLOYEE.NAME as ConsName from PROP_PERSON_GEN PERSON_GEN INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT where CONFIG_NAME = 'Permanent') consName on pg.REFERENCE = consname.REFERENCE
left join (select REFERENCE, STREET2 from PROP_ADDRESS ADDRESS INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= ADDRESS.OCC_ID where CONFIG_NAME = 'Primary') street2 on pg.REFERENCE = street2.REFERENCE --and ADDRESS.REFERENCE = <<PROP_PERSON_GEN.REFERENCE>>
left join (SELECT REFERENCE, iif(RELOCATE = 'Y','Yes','No') AS Relocate FROM PROP_CAND_PREF) relocate on pg.REFERENCE = relocate.REFERENCE
left join (SELECT REFERENCE, MN.DESCRIPTION as Ranking from PROP_PERSON_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_PERSON_GEN.RATING) rating on pg.REFERENCE = rating.REFERENCE
left join (SELECT REFERENCE, SALARY_DES, MN.DESCRIPTION as CurrentSalaryMonth,MN2.DESCRIPTION as ExpectedSalaryMonth FROM PROP_CAND_PREF CAND_PREF LEFT JOIN MD_MULTI_NAMES MN ON MN.ID = CAND_PREF.SALARY_TYPE LEFT JOIN MD_MULTI_NAMES MN2 ON MN2.ID = CAND_PREF.EXPSAL_TYPE) sal on pg.REFERENCE = sal.REFERENCE --where SALARY_DES is not null and MN.DESCRIPTION is not null and MN2.DESCRIPTION is not null
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DESCRIPTION + char(10) FROM skill b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 0, '') FROM skill a GROUP BY REFERENCE) skills ON pg.REFERENCE = skills.REFERENCE -- candidate-email & candidate-workEmail
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM location b WHERE DESCRIPTION != '' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 1, '') FROM location a GROUP BY REFERENCE) pl on pg.REFERENCE = pl.REFERENCE
--left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM industry b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 1, '') FROM industry a GROUP BY REFERENCE) industry ON pg.REFERENCE = industry.REFERENCE -- industry
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM JobCategory b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 1, '') FROM JobCategory a GROUP BY REFERENCE) JobCategory ON pg.REFERENCE = JobCategory.REFERENCE -- JobCategory
left join (SELECT REFERENCE, DESCRIPTION = STUFF((SELECT DISTINCT ', ' + DESCRIPTION FROM SubCategory b WHERE b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 1, '') FROM SubCategory a GROUP BY REFERENCE) SubCategory ON pg.REFERENCE = SubCategory.REFERENCE -- SubCategory
left join (SELECT REFERENCE, ESTAB = STUFF((SELECT DISTINCT '; ' + ESTAB FROM PROP_EDU_ESTAB b WHERE b.ESTAB != '' and b.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1,'') FROM PROP_EDU_ESTAB a GROUP BY REFERENCE) ee2 on pg.REFERENCE = ee2.REFERENCE
left join (SELECT REFERENCE, QUAL = STUFF((SELECT DISTINCT '; ' + QUAL FROM PROP_EDU_ESTAB b WHERE b.qual != '' and b.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') FROM PROP_EDU_ESTAB a GROUP BY REFERENCE) ee3 on pg.REFERENCE = ee3.REFERENCE
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.VISA_TYPE where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') As VISA FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) visa on pg.REFERENCE = visa.REFERENCE
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.STATUS where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') As Status FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) status on pg.REFERENCE = status.REFERENCE
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.SOURCE where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') As Source FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) source on pg.REFERENCE = source.REFERENCE --21159
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.NOT_PERIOD where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') As Noticeperiod FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) Noticeperiod on pg.REFERENCE = Noticeperiod.REFERENCE --22765
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(MN.DESCRIPTION COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN MD_MULTI_NAMES MN ON MN.ID = PROP_CAND_GEN.NATION where REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') As Nationality FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) Nationality on pg.REFERENCE = Nationality.REFERENCE --21167 --Need to convert value to 2 digits country code 
left join (SELECT REFERENCE, STUFF((SELECT distinct ', ' + REPLACE(PG.CHIFULLNAME COLLATE Latin1_General_BIN, char(26), '') FROM PROP_CAND_GEN INNER JOIN PROP_PERSON_GEN PG ON PG.REFERENCE = PROP_CAND_GEN.REFERRAL where PG.REFERENCE = a.REFERENCE FOR XML PATH(''), TYPE).value('.', 'nvarchar(MAX)'), 1, 1, '') As referral FROM PROP_CAND_GEN as a GROUP BY a.REFERENCE) referral on pg.REFERENCE = referral.REFERENCE --21193
--where pg.REFERENCE in (776140,843385,738164,827440)
where pg.FIRST_NAME like '%Susanna%' and pg.LAST_NAME like '%Poon%'