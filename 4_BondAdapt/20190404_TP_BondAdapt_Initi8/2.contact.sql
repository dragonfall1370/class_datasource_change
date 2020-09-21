
-- ALTER DATABASE [Initi8_130219] SET COMPATIBILITY_LEVEL = 130

with
  contact0 (CLIENT,name,CONTACT,rn) as (SELECT cc.CLIENT, cg.name, cc.CONTACT,ROW_NUMBER() OVER(PARTITION BY CONTACT ORDER BY cc.BISUNIQUEID DESC) AS rn FROM PROP_X_CLIENT_CON cc left join PROP_CLIENT_GEN cg on cg.reference = cc.client )
, contact as (select CLIENT, name, CONTACT from contact0 where rn = 1)
--select count(*) from contact --15702
--select * from contact where CONTACT = 38077

-- EMAIL
, mail1 (ID,email) as (select REFERENCE, replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(replace(EMAIL_ADD
        ,'/',' '),'<',' '),'>',' '),'(',' '),')',' '),':',' '),'.@','@'),'@.','@'),'+',' '),'&',' '),'[',' '),']',' '),'?',' '),'''',' '),';',' '),'•',' '),CHAR(9),' ') as mail from PROP_EMAIL where EMAIL_ADD like '%_@_%.__%' )
, mail2 (ID,email) as (SELECT ID, email.value as email FROM mail1 m CROSS APPLY STRING_SPLIT(m.email,',') AS email)
, mail3 (ID,email) as (SELECT ID, case when RIGHT(email, 1) = '.' then LEFT(email, LEN(email) - 1) when LEFT(email, 1) = '.' then RIGHT(email, LEN(email) - 1) else email end as email from mail2 WHERE email like '%_@_%.__%')
, mail4 (ID,email,rn) as ( SELECT ID, email = ltrim(rtrim(CONVERT(NVARCHAR(MAX), email))), r1 = ROW_NUMBER() OVER (PARTITION BY ID ORDER BY ID desc) FROM mail3 )
, e1 (ID,email) as (select ID, email from mail4 where rn = 1)
, ed  (ID,email,rn) as (SELECT ID,email,ROW_NUMBER() OVER(PARTITION BY email ORDER BY ID DESC) AS rn FROM e1 )
, e2 (ID,email) as (select ID, email from mail4 where rn = 2)
, e3 (ID,email) as (select ID, email from mail4 where rn = 3)
, e4 (ID,email) as (select ID, email from mail4 where rn = 4)
--select * from ed where ID in (45315)


--, tel(REFERENCE, TEL_NUMBER) as (SELECT REFERENCE, string_agg( ltrim(rtrim( Stuff( Coalesce(' ' + NULLIF(replace(TEL_NUMBER,',',' '), ''), '') + Coalesce(' - ext ' + NULLIF(extension, ''), '') , 1, 1, '') )), ',') from PROP_TELEPHONE WHERE (TEL_NUMBER <> '' and TEL_NUMBER is not null) GROUP BY REFERENCE)
--select * from tel where reference in (629510) TEL_NUMBER like '%- ext%' and 
--select * from PROP_TELEPHONE where reference in (629510)

/*
--select top 100 * from DOCUMENTS where updated_date is null or updated_date = ''
, with doc_note (OWNER_ID, DOC_ID, NOTE) as (
        SELECT D.OWNER_ID, D.DOC_ID, D.DOC_NAME --,DC.NOTE 
        from DOCUMENTS D 
        --left join DOCUMENT_CONTENT DC on DC.DOC_ID = d.DOC_ID 
        WHERE D.DOC_CATEGORY = 6532841 
        AND D.FILE_EXTENSION in ('bmp','doc','docx','gif','jpeg','jpg','pdf','PDFX','png','rtf','TXT','xls','xlsx','zip'))
--select top 100 * from doc_note where OWNER_ID = 394903
, doc(OWNER_ID, NOTE) as (SELECT OWNER_ID, STUFF((SELECT char(10) + 'NOTE: ' + NOTE from doc_note WHERE NOTE != '' and NOTE is not null and OWNER_ID = a.OWNER_ID FOR XML PATH ('')), 1, 0, '')  AS doc FROM doc_note as a GROUP BY a.OWNER_ID)
select top 50 * from doc where OWNER_ID = 394903
*/

, dob (BISUNIQUEID,REFERENCE,DT_OF_BIRTH,rn) as (SELECT BISUNIQUEID,REFERENCE,DT_OF_BIRTH,ROW_NUMBER() OVER(PARTITION BY cg.REFERENCE ORDER BY cg.BISUNIQUEID DESC) AS rn FROM PROP_CAND_GEN cg)

, owner as ( 
       select PERSON_GEN.REFERENCE as PROP_PERSON_GEN_REFERENCE, PERSON_GEN.FIRST_NAME, PERSON_GEN.LAST_NAME, tmp_email.EMAIL as EMAIL_ADD
              , EMPLOYEE.REFERENCE as PROP_EMPLOYEE_GEN_REFERENCE
              , EMPLOYEE.NAME, EMPLOYEE.USER_REF --mail.email_add ,
       from PROP_PERSON_GEN PERSON_GEN
	INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE 
	INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID 
	INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
	--left join (SELECT REFERENCE, email_add = STUFF((SELECT DISTINCT ', ' + email_add FROM PROP_EMAIL b WHERE b.email_add like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	--left join (SELECT a.REFERENCE, string_agg(a.email_add,',') as email_add FROM (SELECT distinct REFERENCE, email_add from PROP_EMAIL WHERE email_add like '%@%') a GROUP BY REFERENCE) mail ON EMPLOYEE.REFERENCE = mail.REFERENCE
	left join tmp_email ON EMPLOYEE.REFERENCE = tmp_email.REFERENCE
	where CONFIG_NAME = 'Permanent'
	--and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
	)
--select * from owner

, doc(OWNER_ID, DOC_ID) as (SELECT OWNER_ID, STRING_AGG( concat(cast(DOC_ID as varchar(max)),'.',FILE_EXTENSION),',') WITHIN GROUP (ORDER BY DOC_ID) DOC FROM DOCUMENTS GROUP BY OWNER_ID)

--select count(*)
select --top 5
--select --distinct
	  ccc.CONTACT as 'contact-externalId', pg.person_id
	, ccc.CLIENT as 'contact-companyId', ccc.name as 'company-name'
	, pg.title as 'contact-title'
	, replace(pg.FIRST_NAME,'?','') as 'contact-firstName'
	, case when (replace(pg.LAST_NAME,'?','') = '' or pg.LAST_NAME is null) then 'No Lastname' else replace(pg.LAST_NAME,'?','') end as 'contact-lastName'
	--, pg.middle_name as 'contact-middleName'
	, pg.salutation as 'Preferred Name' --**

	--, ltrim(replace(replace(replace(replace(replace(replace(pe.EMAIL_LINK,'�',''),'?',''),'  ',''),char(9),''),'/',','),'mailto:','')) as 'contact-email'
	--, case when (pe.rn > 1 and pe.String is not null) then concat('DUPLICATED_',pe.rn,'_',pe.String) else pe.String end as 'contact-email'
	--, case when (mail5.email != '' and mail5.email is not null and maildup.rn) then concat('DUPLICATED_',maildup.rn,'_',maildup.email) else mail5.email end as 'contact-email)'
       --, iif(maildup.ID in (select ID from mail5),concat('DUPLICATED_',maildup.rn,'_',maildup.email),mail5.email) as 'contact-email'
       , iif(ed.rn > 1, concat(ed.email,'_',ed.rn), ed.email) as 'contact-email'
	, tel.TEL_NUMBER as 'contact-phone'
	, pg.JOB_TITLE as 'contact-jobTitle'
	, pg.LINKEDIN as 'contact-linkedin'
	, owner.EMAIL_ADD as 'contact-owners'
       , Stuff(+ Coalesce('Person ID: ' + NULLIF(convert(nvarchar(max),pg.person_id), '') + char(10), '')
              + Coalesce('Status: ' + NULLIF(pg.status, '') + char(10), '')
--            + Coalesce('Notes: ' + NULLIF(cast(pg. as nvarchar(max)), '') + char(10), '')
--            + Coalesce('Recruits For: ' + NULLIF(cast(pg. as nvarchar(max)), '') + char(10), '')
              + Coalesce('Facebook: ' + NULLIF(cast(pg.facebook as nvarchar(max)), '') + char(10), '')
              + Coalesce('Twitter: ' + NULLIF(cast(pg.twitter as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Status: ' + NULLIF(cast(pg.status as nvarchar(max)), '') + char(10), '')
              + Coalesce('Grading: ' + NULLIF(cast(pg.Grading as nvarchar(max)), '') + char(10), '')
              + Coalesce('Last Contact: ' + NULLIF(cast( convert(date,pg.Audit_Last_Contact) as nvarchar(max)), '') + char(10), '')
              + Coalesce('By: ' + NULLIF(cast(pg.Audit_By as nvarchar(max)), '') + char(10), '')
              + Coalesce('At: ' + NULLIF(cast(pg.Audit_At as nvarchar(max)), '') + char(10), '')
              + Coalesce('Last Updated: ' + NULLIF(cast(pg.Audit_Last_Updated as nvarchar(max)), '') + char(10), '')
              --+ Coalesce('Contact Owner: ' + NULLIF(cast(owner.NAME as nvarchar(max)), '') + char(10), '')
              , 1, 0, '') as 'contact-note'

	, replace(doc.DOC_ID,'.txt','.rtf') as 'contact-document'
	--, left(replace(d.Notes,'&#x0D;',''),32000) as 'contact-comments'

, e2.email as 'personal_email'
, telmobile.TEL_NUMBER as 'Mobile Phone'
--, telhome.TEL_NUMBER as 'Home Phone'
--, telwork.TEL_NUMBER as 'Work Phone'
, ltrim(Stuff( Coalesce(' ' + NULLIF(telwork.TEL_NUMBER, ''), '') + Coalesce(', ' + NULLIF(telhome.TEL_NUMBER, ''), '') , 1, 1, '') ) as 'Home Phone'

, ltrim(Stuff( Coalesce(' ' + NULLIF(address.STREET1, ''), '') + Coalesce(', ' + NULLIF(address.STREET2, ''), '') , 1, 1, '') ) as 'address'
, address.locality
, address.TOWN as 'city'
, address.county as 'state'
, address.POST_CODE as 'post_code'
, address.COUNTRY
, address.country_name as 'country_code'
, pg.e_shot as 'Mail Subscribed'

--select count(*) --15780--select count(distinct cc.CONTACT) --15701 rows -- select * from contact
--from PROP_X_CLIENT_CON cc
--left join contact ccc on ccc.CONTACT = cc.CONTACT --
from contact ccc
left join (
       select
              pg.REFERENCE, pg.person_id, pg.FIRST_NAME, pg.LAST_NAME, pg.MIDDLE_NAME, pg.salutation, pg.JOB_TITLE, pg.linkedin, status.DESCRIPTION as status, pg.facebook, pg.skype_id, pg.twitter
              , cg.E_SHOT, cg.grading
              , eg.name as 'Audit_By', CONVERT(VARCHAR(5),ph.last_cont_tm,108) as 'Audit_At', ph.last_cont_dt as 'Audit_Last_Contact'
              , eg2.lst_act_dt as 'Audit_Last_Updated'
              , case
              when title.DESCRIPTION in ('Doctor') then 'Dr.'
              when title.DESCRIPTION in ('Sir','Mr') then 'Mr.'
              when title.DESCRIPTION in ('Ms') then 'Ms.'
              when title.DESCRIPTION in ('Miss') then 'Miss.'
              when title.DESCRIPTION in ('Mrs') then 'Mrs.'
              end as title
       -- select distinct title.DESCRIPTION -- select *
       from PROP_PERSON_GEN pg
       left join (select pg.REFERENCE, pg.title, mn.DESCRIPTION from PROP_PERSON_GEN pg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = pg.title where MN.ID is not null and LANGUAGE = 10010) title on title.REFERENCE = pg.REFERENCE
       left join (select cg.REFERENCE, cg.status, mn.DESCRIPTION from PROP_CONT_GEN cg INNER JOIN MD_MULTI_NAMES MN ON MN.ID = cg.status where MN.ID is not null and LANGUAGE = 10010) status on status.REFERENCE = pg.REFERENCE
       left join PROP_CONT_GEN cg on cg.reference = pg.reference
       left join PROP_PERSON_HIST ph on ph.reference = pg.reference
       left join PROP_EMPLOYEE_GEN eg on eg.user_ref = ph.last_cont_by
       left join PROP_EMPLOYEE_GEN eg2 on eg2.reference = pg.reference
       --where pg.REFERENCE in (45315)
       --where pg.REFERENCE in (66096, 71174, 44530, 116689764500)
--       where pg.person_id in (66096, 71174, 44530, 1136780)
       ) pg on ccc.CONTACT = pg.REFERENCE
left join ( 
       SELECT REFERENCE, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE, COUNTRY
              , case
		when DESCRIPTION like 'AFGHANI%' then 'AF'
		when DESCRIPTION like 'ALBANIA%' then 'AL'
		when DESCRIPTION like 'ARGENTI%' then 'AR'
		when DESCRIPTION like 'AUSTRAL%' then 'AU'
		when DESCRIPTION like 'AUSTRIA%' then 'AT'
		when DESCRIPTION like 'BELARUS%' then 'BY'
		when DESCRIPTION like 'BELGIUM%' then 'BE'
		when DESCRIPTION like 'BRAZIL%' then 'BR'
		when DESCRIPTION like 'BULGARI%' then 'BG'
		when DESCRIPTION like 'CANADA%' then 'CA'
		when DESCRIPTION like 'CHILE%' then 'CL'
		when DESCRIPTION like 'CHINA%' then 'CN'
		when DESCRIPTION like 'COLOMBI%' then 'CO'
		when DESCRIPTION like 'CYPRUS%' then 'CY'
		when DESCRIPTION like 'CZECH%' then 'CZ'
		when DESCRIPTION like 'DENMARK%' then 'DK'
		when DESCRIPTION like 'ECUADOR%' then 'EC'
		when DESCRIPTION like 'EGYPT%' then 'EG'
		when DESCRIPTION like 'ESTONIA%' then 'EE'
		when DESCRIPTION like 'FINLAND%' then 'FI'
		when DESCRIPTION like 'FRANCE%' then 'FR'
		when DESCRIPTION like 'GERMANY%' then 'DE'
		when DESCRIPTION like 'GIBRALT%' then 'ES'
		when DESCRIPTION like 'GREECE%' then 'GR'
		when DESCRIPTION like 'HUNGARY%' then 'HU'
		when DESCRIPTION like 'ICELAND%' then 'IS'
		when DESCRIPTION like 'INDIA%' then 'IN'
		when DESCRIPTION like 'INDONES%' then 'ID'
		when DESCRIPTION like 'IRELAND%' then 'IE'
		when DESCRIPTION like 'ISRAEL%' then 'IL'
		when DESCRIPTION like 'ITALY%' then 'IT'
		when DESCRIPTION like 'JORDAN%' then 'JO'
		when DESCRIPTION like 'KAZAKHS%' then 'KZ'
		when DESCRIPTION like 'KENYA%' then 'KE'
		when DESCRIPTION like 'LATVIA%' then 'LV'
		when DESCRIPTION like 'LEBANON%' then 'LB'
		when DESCRIPTION like 'LIECHTE%' then 'LI'
		when DESCRIPTION like 'LITHUAN%' then 'LT'
		when DESCRIPTION like 'LUXEMBO%' then 'LU'
		when DESCRIPTION like 'MACEDON%' then 'MK'
		when DESCRIPTION like 'MALAYSI%' then 'MY'
		when DESCRIPTION like 'MALTA%' then 'MT'
		when DESCRIPTION like 'MOLDOVA%' then 'MD'
		when DESCRIPTION like 'NETHERL%' then 'NL'
		when DESCRIPTION like 'NEW ZEALAND%' then 'NZ'
		when DESCRIPTION like 'NORWAY%' then 'NO'
		when DESCRIPTION like 'PANAMA%' then 'PA'
		when DESCRIPTION like 'PHILIPP%' then 'PH'
		when DESCRIPTION like 'POLAND%' then 'PL'
		when DESCRIPTION like 'PORTUGA%' then 'PT'
		when DESCRIPTION like 'QATAR%' then 'QA'
		when DESCRIPTION like 'ROMANIA%' then 'RO'
		when DESCRIPTION like 'RUSSIAN%' then 'RU'
		when DESCRIPTION like 'SAUDI%' then 'SA'
		when DESCRIPTION like 'SERBIA%' then 'RS'
		when DESCRIPTION like 'SINGAPO%' then 'SG'
		when DESCRIPTION like 'SLOVAKI%' then 'SK'
		when DESCRIPTION like 'SLOVENI%' then 'SI'
		when DESCRIPTION like 'SOUTH AFRICA%' then 'ZA'
		when DESCRIPTION like 'SPAIN%' then 'ES'
		when DESCRIPTION like 'SRI%' then 'LK'
		when DESCRIPTION like 'SWAZILA%' then 'SZ'
		when DESCRIPTION like 'SWEDEN%' then 'SE'
		when DESCRIPTION like 'SWITZER%' then 'CH'
		when DESCRIPTION like 'TAIWAN%' then 'TW'
		when DESCRIPTION like 'TAJIKIS%' then 'TJ'
		when DESCRIPTION like 'TURKEY%' then 'TR'
		when DESCRIPTION like 'UKRAINE%' then 'UA'
		when DESCRIPTION like '%UNITED%ARAB%' then 'AE'
		when DESCRIPTION like '%UAE%' then 'AE'
		when DESCRIPTION like '%U.A.E%' then 'AE'
		when DESCRIPTION like '%UNITED%KINGDOM%' then 'GB'
		when DESCRIPTION like '%UNITED%STATES%' then 'US'
		when DESCRIPTION like '%US%' then 'US'
              end as 'country_name'
       from (
              select REFERENCE, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE, COUNTRY, MN.DESCRIPTION , rn = ROW_NUMBER() OVER (PARTITION BY REFERENCE, STREET1, STREET2, LOCALITY, TOWN, county, POST_CODE /*COUNTRY, MN.DESCRIPTION*/ ORDER BY CONFIG_NAME asc)
              -- select distinct OCC.config_name 
              from PROP_ADDRESS ADDRESS 
              left JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID = ADDRESS.OCC_ID
              left JOIN MD_MULTI_NAMES MN ON MN.ID = ADDRESS.COUNTRY
              --where OCC.config_name = 'Primary'
              --where MN.LANGUAGE = 10010 --and REFERENCE in (45315)
              --order by OCC.config_name asc
              ) a
       where a.rn = 1 --and REFERENCE in (45315)
       ) address on address.REFERENCE = ccc.CONTACT
left join (SELECT REFERENCE, string_agg( ltrim(rtrim( Stuff( coalesce(' ' + NULLIF(replace(TEL_NUMBER,',',' '), ''), '') + coalesce(' - ext ' + NULLIF(extension, ''), '') , 1, 1, '') )), ',') as TEL_NUMBER from PROP_TELEPHONE WHERE (TEL_NUMBER <> '' and TEL_NUMBER is not null) GROUP BY REFERENCE) tel on tel.REFERENCE = ccc.CONTACT
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Home') telhome on ccc.CONTACT = telhome.REFERENCE --candidate-homePhone
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Mobile') telmobile on ccc.CONTACT = telmobile.REFERENCE --candidate-mobile
left join (select REFERENCE,TEL_NUMBER from PROP_TELEPHONE TEL INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= TEL.OCC_ID where CONFIG_NAME = 'Work') telwork on ccc.CONTACT = telwork.REFERENCE --candidate-phone & candidate-workPhone
left join ed ON ccc.CONTACT = ed.id -- DUPLICATED-EMAIL
left join e2 ON ccc.CONTACT = e2.id -- Other-EMAIL
--left join e3 ON ccc.CONTACT = e3.id -- Other-EMAIL
--left join e4 ON ccc.CONTACT = e4.id -- Other-EMAIL
left join owner ON ccc.CONTACT = owner.PROP_PERSON_GEN_REFERENCE
left join doc on ccc.CONTACT = doc.OWNER_ID
where pg.reference in (40498,116659009709,63816,116656559606,116688555147)
--where pg.REFERENCE in (116674167980) --(116656592765,144326,64844,76453,45315,116658192116,107089,81194,148714,192845,45276,41656,70761,44521,116674156720,74244,60062,51006,116656277811,74451,70958,116656572087,716701,80356,46641,114507,43472,69091,74449,82366,76386,388433,45090,113921,92851,218777,44486,116657199270,77507)
--where pg.person_id in (107796,105767,75209,89420,79136)
--where owner.EMAIL_ADD is not null
--where ccc.CONTACT is not null
--left join photo on pg.REFERENCE = photo.OWNER_ID
--and mail5.email is not null
--and pg.chinesename is not null
--and ccc.CONTACT in (396970,397884,397965,398036,406635,409577,410217,414232,415647,420063,436059,528026,550535,558455,567093,588173,588743,591415,592958,596988,599869,626195,629811,631637,659586,661339,676728,718815,728977,753998,760676,801076,814991)
--and pedup.rn = 2
--and ccc.CONTACT in (394855,394857,394858,394859,394860,394941,422556,440045,520613,538555,556969,596437,603166,672412,682563,715196,724260,767526,775203,808647,816694,838398)
--and pg.FIRST_NAME like '%David%%' and pg.LAST_NAME like '%Wadsworth%'
--and pg.FIRST_NAME like '%Susanna%' and pg.LAST_NAME like '%Poon%'
--and mail.EMAIL_LINK like '%chris.tang@staranise.com.hk%'
--and pe.string like '%)%'
--and own.EMPLOYEE_NAME is not null
--and ccc.CLIENT = 395108
--and mail.EMAIL_LINK like '%david.fleming@venetian.com.mo%'
--order by ccc.CONTACT desc

/*
--select EMPLOYEE.REFERENCE,EMPLOYEE.NAME,EMPLOYEE.USER_REF
select PERSON_GEN.REFERENCE,PERSON_GEN.FIRST_NAME,PERSON_GEN.LAST_NAME,EMPLOYEE.NAME,mail.EMAIL_LINK
--,mail.EMAIL_LINK 
from PROP_PERSON_GEN PERSON_GEN 
INNER JOIN PROP_OWN_CONS CONS ON PERSON_GEN.REFERENCE = CONS.REFERENCE INNER JOIN MD_NAMED_OCCS OCC ON OCC.OCC_ID= CONS.OCC_ID INNER JOIN PROP_EMPLOYEE_GEN EMPLOYEE ON EMPLOYEE.USER_REF = CONS.CONSULTANT 
left join (SELECT REFERENCE, EMAIL_LINK = STUFF((SELECT DISTINCT ', ' + EMAIL_LINK FROM PROP_EMAIL b WHERE b.EMAIL_LINK like '%@%' and b.REFERENCE = a.REFERENCE FOR XML PATH('')), 1, 2, '') FROM PROP_EMAIL a GROUP BY REFERENCE) mail ON PERSON_GEN.REFERENCE = mail.REFERENCE
where CONFIG_NAME = 'Permanent'
and PERSON_GEN.FIRST_NAME like '%Christian%' and PERSON_GEN.LAST_NAME like '%Kwan%'
and mail.EMAIL_LINK is not null
*/
